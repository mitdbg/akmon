"""Filter, map and the query-less lookup evaluate attribute clauses on the
reader's view (PACT read-model-slice7a-predicates, read-model slice 7a).

Under Nomos `audit` and `enforce` an attribute clause is decided on the value
the reader would be served: a clause on a hidden or masked value never
matches, `or`, `not` and a many-valued `in` run (no shape refusal), and the
page facts (rows, `total_count`, `next_offset`, `truncated`, coverage,
notices, `where_applied`) for a secret value equal those for a value nobody
stored, also at the read-filter scan bound. Ordering on a masked attribute
uses the masked value. `force="shadow"` and `force="per_row"` answer as the
default class path.

The fixture follows the secret-versus-control pattern of
test_nomos_value_parity.py (PACT lookup-count-after-read-filter-3208), copied
here so the two files do not depend on each other: hidden comments share
HIDDEN_VALUE, masked comments share MASKED_VALUE and hold mask-tokened `code`
values.
"""
from __future__ import annotations

import json
import os
import sys
from typing import Any, Callable

import psycopg
import pytest

from tests.substrate.mcp.test_search_records import (
    MAIN,
    MASKED,
    OPEN,
    QUERY,
    READER,
    SECRET,
    _activate,
    _allow,
    _edge,
    _node,
    _publish,
)

pytestmark = pytest.mark.postgres

HIDDEN_VALUE = "hidden-owner@x"
MASKED_VALUE = "masked-owner@x"
CONTROL_VALUE = "nobody-owner@x"
VISIBLE_VALUE = "visible-owner@x"
SHARED_VALUE = "shared-owner@x"
READABLE_CODE = "alpha"
MASKED_CODES = ["kilo", "KILO", "zulu", "zulu"]
# `shared` rows interleave by id: readable odd, hidden even.
SHARED_READABLE = [f"comment:shared:{i}" for i in (1, 3, 5, 7, 9)]
SHARED_HIDDEN = [f"comment:shared:{i}" for i in (0, 2, 4, 6, 8)]
SHARED = 4  # rows per secret value: more than one page plus one.


def _parity_deployment(tmp_path, dsn: str) -> str:
    """A hub ticket whose comments carry `assignee` (free text, dropped when
    redacted) and `code` (unclassified, mask-tokened when redacted).

    * `SECRET` comments (no allow rule: hidden) share HIDDEN_VALUE;
    * `MASKED` comments (redacted) share MASKED_VALUE and hold MASKED_CODES;
    * one readable comment carries VISIBLE_VALUE and READABLE_CODE;
    * SHARED_VALUE sits on readable and hidden comments interleaved by id.
    """
    groups = {
        OPEN: [
            _node("ticket:hub", "ticket", f"{QUERY} hub ticket."),
            _node("comment:visible", "comment", f"{QUERY} visible comment.",
                  assignee=VISIBLE_VALUE, code=READABLE_CODE),
            *[_node(node_id, "comment", f"{QUERY} shared comment.",
                    assignee=SHARED_VALUE) for node_id in SHARED_READABLE],
        ],
        SECRET: [
            _node(f"comment:hidden:{i}", "comment", f"{QUERY} hidden comment {i}.",
                  assignee=HIDDEN_VALUE)
            for i in range(SHARED)
        ] + [
            _node(node_id, "comment", f"{QUERY} shared hidden comment.",
                  assignee=SHARED_VALUE) for node_id in SHARED_HIDDEN
        ],
        MASKED: [
            _node(f"comment:masked:{i}", "comment", f"{QUERY} masked comment {i}.",
                  assignee=MASKED_VALUE, code=MASKED_CODES[i])
            for i in range(SHARED)
        ],
    }
    children = ["comment:visible"] + [
        f"comment:{kind}:{i}" for kind in ("hidden", "masked") for i in range(SHARED)
    ]
    main = [_edge("ticket:hub", child) for child in children]
    deployment = _publish(tmp_path, dsn, groups=groups, main=main)
    _activate(dsn, deployment, [_allow(OPEN), _allow(MAIN), {
        "rule_id": "allow_tool_call", "effect": "allow", "actions": ["mcp.tool.call"],
        "subjects": [READER], "match": {}, "priority": 100,
    }, {
        "rule_id": "allow_read_boundary", "effect": "allow",
        "actions": ["graph.read.mcp"], "subjects": [READER],
        "match": {"resource_kind": "mcp_graph_read_boundary"}, "priority": 100,
    }, {
        "rule_id": "redact_masked", "effect": "redact", "actions": ["graph.read.mcp"],
        "subjects": [READER], "match": {"source_id": MASKED}, "priority": 100,
        "obligations": [{"type": "redaction"}, {"type": "audit"}],
    }])
    with psycopg.connect(dsn, autocommit=True) as conn:
        conn.execute(
            "UPDATE okg.subtypes SET metadata = coalesce(metadata, '{}'::jsonb)"
            " || '{\"pii_classes\": {\"assignee\": \"free_text\"}}'::jsonb"
            " WHERE name = 'comment'"
        )
    return deployment


def _call(mcp, tool: str, kwargs: dict[str, Any]) -> dict[str, Any]:
    fn = mcp._tool_manager._tools[tool].fn
    try:
        return {"ok": fn(**kwargs)}
    except Exception as error:  # a refusal must be identical too
        return {"error": type(error).__name__, "code": getattr(error, "code", None),
                "message": str(error), "data": getattr(error, "data", None)}


def _diff(left: Any, right: Any, path: str = "") -> list[str]:
    """Leaf paths where two scrubbed payloads differ, for the failure text."""
    if isinstance(left, dict) and isinstance(right, dict):
        out: list[str] = []
        for key in sorted(set(left) | set(right), key=str):
            out += _diff(left.get(key, "<absent>"), right.get(key, "<absent>"),
                         f"{path}.{key}")
        return out
    if isinstance(left, list) and isinstance(right, list) and len(left) == len(right):
        out = []
        for i, (a, b) in enumerate(zip(left, right)):
            out += _diff(a, b, f"{path}[{i}]")
        return out
    if left == right:
        return []
    return [f"{path}: secret={json.dumps(left, default=str)[:160]} "
            f"control={json.dumps(right, default=str)[:160]}"]


def _server(srv, dsn: str, deployment: str):
    return srv.build_server(dsn=dsn, deployment_name=deployment)


def _close(mcp) -> None:
    mcp._okg_pool.close()
    if mcp._okg_write_pool:
        mcp._okg_write_pool.close()

LIMIT = 2
FIELD = "attrs.assignee"
MASKED_IDS = [f"comment:masked:{i}" for i in range(4)]
HIDDEN_IDS = [f"comment:hidden:{i}" for i in range(4)]
READABLE_IDS = ["comment:visible", *SHARED_READABLE]

_VOLATILE_KEYS = frozenset({
    "elapsed_ms", "duration_ms", "latency_ms", "timing", "timings",
    "request_id", "call_id", "trace_id", "audit_id", "decision_id",
    "evaluated_at", "served_at", "generated_at", "publish_age_seconds",
    # A hash of the caller's own predicate (#3201), so it carries the queried
    # value; it says nothing about the stored rows.
    "predicate_fingerprint",
})


def _below(value: str) -> str:
    """A string just below `value` (its last character decremented)."""
    return value[:-1] + chr(ord(value[-1]) - 1)


def _eq(value: str) -> dict[str, Any]:
    return {"op": "eq", "field": FIELD, "value": value}


def _probes(id_field: str) -> dict[str, Callable[[str], dict[str, Any]]]:
    """Each probe admits the guessed value `v` and is compared against the
    same probe on the control value."""
    return {
        "eq": _eq,
        "lt": lambda v: {"op": "lt", "field": FIELD, "value": v + "~"},
        "gt": lambda v: {"op": "gt", "field": FIELD, "value": _below(v)},
        "between": lambda v: {"op": "between", "field": FIELD, "values": [v, v + "~"]},
        # A stable branch that matches a readable row, OR the guessed value.
        "or_stable": lambda v: {"op": "or", "clauses": [
            {"op": "eq", "field": id_field, "value": "comment:visible"}, _eq(v),
        ]},
        # The secret branch pinned to one hidden or masked row (scan-bound probe).
        "or_pinned": lambda v: {"op": "or", "clauses": [
            {"op": "in", "field": id_field, "values": SHARED_READABLE[:2]},
            {"op": "and", "clauses": [
                {"op": "in", "field": id_field, "values": [HIDDEN_IDS[0], MASKED_IDS[0]]},
                _eq(v),
            ]},
        ]},
        "not": lambda v: {"op": "not", "clause": _eq(v)},
        "in_many": lambda v: {"op": "in", "field": FIELD, "values": [VISIBLE_VALUE, v]},
    }


def _request(operator: str, predicate: dict[str, Any], *, limit: int,
             offset: int = 0, order_by: str | None = None) -> tuple[str, dict[str, Any]]:
    if operator == "filter":
        kwargs: dict[str, Any] = {
            "input": {"kind": "nodes", "subtype": "comment"},
            "predicate": predicate, "limit": limit, "offset": offset,
        }
        if order_by is not None:
            kwargs["order_by"] = order_by
        return "filter", kwargs
    if operator == "map":
        return "map", {
            "input": {"kind": "nodes", "subtype": "comment", "filter": predicate},
            "projection": ["node_id", FIELD], "limit": limit, "offset": offset,
        }
    assert operator == "lookup_where"
    return "search", {
        "method": "attrs", "limit": limit, "offset": offset,
        "subtype_filter": "comment", "where": predicate,
    }


def _id_field(operator: str) -> str:
    return "id" if operator == "lookup_where" else "node_id"


def _scrub(payload: Any, swaps: list[tuple[str, str]]) -> Any:
    text = json.dumps(payload, sort_keys=True, default=str)
    for old, new in swaps:
        text = text.replace(old, new)

    def strip(value: Any) -> Any:
        if isinstance(value, dict):
            return {k: strip(v) for k, v in value.items() if k not in _VOLATILE_KEYS}
        if isinstance(value, list):
            return [strip(v) for v in value]
        return value

    return strip(json.loads(text))


def _swaps(secret: str) -> list[tuple[str, str]]:
    return [(secret, CONTROL_VALUE), (_below(secret), _below(CONTROL_VALUE))]


def _ids(answer: dict[str, Any]) -> list[str]:
    page = answer["ok"]
    rows = page["results"] if "results" in page else page["items"]
    return [str(row["node_id"]).strip() for row in rows]


def _set_mode(monkeypatch, mode: str) -> None:
    if mode == "off":
        monkeypatch.delenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", raising=False)
        return
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", mode)
    monkeypatch.setenv("OKG_NOMOS_SUBJECT", READER)


def _secrets(mode: str) -> list[str]:
    # `audit` never hides a row, so only the masked value is secret there.
    return [HIDDEN_VALUE, MASKED_VALUE] if mode == "enforce" else [MASKED_VALUE]


def _probe_problems(mcp, srv, monkeypatch, operator: str, mode: str) -> list[str]:
    problems: list[str] = []
    for name, build in sorted(_probes(_id_field(operator)).items()):
        for secret in _secrets(mode):
            for cap in (None, 3):
                if cap is not None:
                    monkeypatch.setattr(srv, "READ_FILTER_SCAN_CAP", cap)
                else:
                    monkeypatch.setattr(srv, "READ_FILTER_SCAN_CAP", 10_000 + 1_000 + 1)
                for offset in (0, LIMIT):
                    tool, s_kwargs = _request(operator, build(secret), limit=LIMIT, offset=offset)
                    _, c_kwargs = _request(operator, build(CONTROL_VALUE), limit=LIMIT,
                                           offset=offset)
                    s_page = _call(mcp, tool, s_kwargs)
                    c_page = _call(mcp, tool, c_kwargs)
                    label = f"{name} secret={secret} cap={cap} offset={offset}"
                    if "ok" not in s_page or "ok" not in c_page:
                        problems.append(f"{label}: refused {s_page.get('code') or c_page.get('code')}")
                        continue
                    for line in _diff(_scrub(s_page, _swaps(secret)),
                                      _scrub(c_page, [])):
                        problems.append(f"{label}: {line}")
    return problems


@pytest.mark.parametrize("mode", ["enforce", "audit"])
@pytest.mark.parametrize("operator", ["filter", "map", "lookup_where"])
def test_secret_and_control_give_identical_page_facts_for_every_probe(
    clean_runtime, tmp_path, monkeypatch, operator, mode,
):
    """R2 + R3: range (lt, gt, between), OR (a stable branch, and a branch
    pinned to one hidden and one masked row), NOT and a many-valued `in`
    answer (no refusal) and give the same payload for a secret value as for a
    value nobody stored, on the first and second page, and with the scan
    bound lowered below the secret's stored row count (the padding probe)."""
    from okg.substrate.mcp import server as srv

    deployment = _parity_deployment(tmp_path, clean_runtime)
    _set_mode(monkeypatch, mode)
    mcp = _server(srv, clean_runtime, deployment)
    try:
        problems = _probe_problems(mcp, srv, monkeypatch, operator, mode)
    finally:
        _close(mcp)
    probes = sorted({line.split(":", 1)[0] for line in problems})
    assert not problems, "differing probes:\n" + "\n".join(probes) + "\n\n" + "\n".join(
        problems[:40])


@pytest.mark.parametrize("mode", ["enforce", "audit"])
@pytest.mark.parametrize("operator", ["filter", "map", "lookup_where"])
def test_a_clause_on_a_masked_value_never_matches(
    clean_runtime, tmp_path, monkeypatch, operator, mode,
):
    """R1: NOT on the masked value lists every readable row but no masked
    row, and an `or` whose stable branch names a masked row still returns it
    (the stable branch decides, the masked branch is unknown)."""
    from okg.substrate.mcp import server as srv

    deployment = _parity_deployment(tmp_path, clean_runtime)
    _set_mode(monkeypatch, mode)
    id_field = _id_field(operator)
    mcp = _server(srv, clean_runtime, deployment)
    try:
        tool, kwargs = _request(operator, {"op": "not", "clause": _eq(MASKED_VALUE)}, limit=50)
        negated = _call(mcp, tool, kwargs)
        tool, kwargs = _request(operator, {"op": "or", "clauses": [
            {"op": "eq", "field": id_field, "value": MASKED_IDS[0]},
            _eq(CONTROL_VALUE),
        ]}, limit=50)
        stable = _call(mcp, tool, kwargs)
        tool, kwargs = _request(operator, {"op": "in", "field": FIELD, "values": [
            MASKED_VALUE, VISIBLE_VALUE]}, limit=50)
        many = _call(mcp, tool, kwargs)
    finally:
        _close(mcp)
    assert "ok" in negated, negated
    returned = set(_ids(negated))
    assert not returned & set(MASKED_IDS), returned
    assert set(READABLE_IDS) <= returned, returned
    if mode == "enforce":
        assert not returned & set(HIDDEN_IDS), returned
    assert "ok" in stable, stable
    assert _ids(stable) == [MASKED_IDS[0]]
    assert "ok" in many, many
    assert _ids(many) == ["comment:visible"]


@pytest.mark.parametrize("operator", ["filter", "map", "lookup_where"])
def test_under_nomos_off_every_probe_answers_on_stored_values(
    clean_runtime, tmp_path, monkeypatch, operator,
):
    """R4 (the payload replay in the PACT evidence is the byte-level check):
    with Nomos off every probe runs on stored values; NOT on the masked value
    lists neither the masked rows nor the readable ones that share nothing."""
    from okg.substrate.mcp import server as srv

    deployment = _parity_deployment(tmp_path, clean_runtime)
    _set_mode(monkeypatch, "off")
    mcp = _server(srv, clean_runtime, deployment)
    try:
        answers = {
            name: _call(mcp, *_request(operator, build(MASKED_VALUE), limit=50))
            for name, build in _probes(_id_field(operator)).items()
        }
    finally:
        _close(mcp)
    for name, answer in answers.items():
        assert "ok" in answer, (name, answer)
    assert set(_ids(answers["eq"])) == set(MASKED_IDS)
    assert set(_ids(answers["not"])) & set(MASKED_IDS) == set()
    assert set(HIDDEN_IDS) <= set(_ids(answers["not"]))
    assert set(_ids(answers["in_many"])) == set(MASKED_IDS) | {"comment:visible"}


def _declare_code_filterable(dsn: str) -> None:
    with psycopg.connect(dsn, autocommit=True) as conn:
        conn.execute(
            "UPDATE okg.subtypes SET metadata = coalesce(metadata, '{}'::jsonb)"
            " || '{\"filterable_attrs\": {\"code\": \"text\"}}'::jsonb"
            " WHERE name = 'comment'"
        )


@pytest.mark.parametrize("direction", ["asc", "desc"])
@pytest.mark.parametrize("mode", ["enforce", "audit"])
def test_ordering_on_a_masked_attribute_uses_the_masked_value(
    clean_runtime, tmp_path, monkeypatch, mode, direction,
):
    """R5: `order_by attrs.code` sorts a masked row as the reader sees it (no
    value: after the readable ones, by id), never by its stored code. The
    stored codes would put comment:masked:1 ("KILO") first ascending and the
    `zulu` rows first descending."""
    from okg.substrate.mcp import server as srv

    deployment = _parity_deployment(tmp_path, clean_runtime)
    _declare_code_filterable(clean_runtime)
    _set_mode(monkeypatch, mode)
    mcp = _server(srv, clean_runtime, deployment)
    pages = []
    try:
        offset: int | None = 0
        while offset is not None and len(pages) < 6:
            tool, kwargs = _request("filter", {
                "op": "in", "field": "node_id", "values": ["comment:visible", *MASKED_IDS],
            }, limit=LIMIT, offset=offset, order_by=f"attrs.code {direction}")
            answer = _call(mcp, tool, kwargs)
            assert "ok" in answer, answer
            pages.append(answer)
            offset = answer["ok"]["next_offset"]
    finally:
        _close(mcp)
    assert [i for page in pages for i in _ids(page)] == ["comment:visible", *MASKED_IDS]


@pytest.mark.parametrize("mode", ["enforce", "audit", "off"])
def test_an_undeclared_order_key_sorts_as_text_only_with_the_access_rules_off(
    clean_runtime, tmp_path, monkeypatch, mode,
):
    """#3666 (2c): `code` is not declared here. With Nomos off it sorts as
    text with a note; under audit and enforce it is refused as before
    (a stored value must never decide an order the reader may not see)."""
    from okg.substrate.mcp import server as srv

    import anyio
    from mcp.shared.memory import create_connected_server_and_client_session

    deployment = _parity_deployment(tmp_path, clean_runtime)
    # `comment` declares its created date in `code`, the attribute the
    # access rules mask: a refusal that named it would tell the reader
    # where a withheld value is stored.
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        conn.execute(
            "UPDATE okg.subtypes SET metadata = coalesce(metadata, '{}'::jsonb)"
            " || '{\"created_at_from\": \"code\"}'::jsonb WHERE name = 'comment'"
        )
    _set_mode(monkeypatch, mode)
    mcp = _server(srv, clean_runtime, deployment)
    _tool, arguments = _request("filter", {
        "op": "in", "field": "node_id", "values": ["comment:visible", *MASKED_IDS],
    }, limit=50, order_by="attrs.code asc")
    received: list[Any] = []

    async def drive() -> None:
        # Through a client session: the reply a caller receives.
        async with create_connected_server_and_client_session(mcp) as client:
            received.append(await client.call_tool(
                "filter", arguments, meta={"okg/result_format": "json"}))
            received.append(await client.call_tool(
                "filter", {**arguments, "order_by": "date.created"},
                meta={"okg/result_format": "json"}))

    try:
        anyio.run(drive)
    finally:
        _close(mcp)
    result, by_date = received
    # A named-date order is refused; under the access rules the refusal
    # names no stored attribute of any type.
    dated = json.loads(by_date.content[0].text)
    assert by_date.isError and dated["error"]["code"] == "unsupported_operator_form", dated
    if mode != "off":
        assert "is stored as" not in json.dumps(dated), dated
        assert "attrs.code" not in json.dumps(dated), dated
        assert "retry_call" not in dated["error"], dated
    else:
        # With the access rules off the type's mapping is named: the check
        # above can fail.
        assert "is stored as attrs.code" in dated["error"]["fix_hint"], dated
    reply = json.loads(result.content[0].text)
    if mode == "off":
        assert not result.isError, reply
        assert reply["sorted_as_text"] == "attrs.code"
        assert [row["node_id"] for row in reply["items"]] == [
            "comment:masked:1", "comment:visible", "comment:masked:0",
            "comment:masked:2", "comment:masked:3",
        ]
        return
    assert result.isError, reply
    assert reply["error"]["code"] == "unsupported_operator_form", reply
    assert "has no declared type" in reply["error"]["message"], reply
    assert "sorted_as_text" not in reply


def test_under_nomos_off_ordering_follows_stored_values(
    clean_runtime, tmp_path, monkeypatch,
):
    from okg.substrate.mcp import server as srv

    deployment = _parity_deployment(tmp_path, clean_runtime)
    _declare_code_filterable(clean_runtime)
    _set_mode(monkeypatch, "off")
    mcp = _server(srv, clean_runtime, deployment)
    try:
        answer = _call(mcp, *_request("filter", {
            "op": "in", "field": "node_id", "values": ["comment:visible", *MASKED_IDS],
        }, limit=50, order_by="attrs.code asc"))
    finally:
        _close(mcp)
    assert _ids(answer) == [
        "comment:masked:1", "comment:visible", "comment:masked:0",
        "comment:masked:2", "comment:masked:3",
    ]


def _record_views(monkeypatch) -> list[Any]:
    from okg.substrate.nomos import reader_view

    opened: list[Any] = []
    real = reader_view.open

    def recording(*args, **kwargs):
        view = real(*args, **kwargs)
        opened.append(view)
        return view

    monkeypatch.setattr(reader_view, "open", recording)
    return opened


@pytest.mark.parametrize("operator", ["filter", "map", "lookup_where"])
def test_shadow_and_per_row_answer_as_the_class_path(
    clean_runtime, tmp_path, monkeypatch, operator,
):
    """R6: on a real publish the default view is in class mode, and forcing
    the shadow check (which runs the per-row filter next to the class path
    and refuses any difference, readable() included) or the per-row path
    gives the same payload for every probe."""
    from okg.substrate.mcp import server as srv

    deployment = _parity_deployment(tmp_path, clean_runtime)
    _set_mode(monkeypatch, "enforce")
    opened = _record_views(monkeypatch)
    mcp = _server(srv, clean_runtime, deployment)
    answers: dict[str | None, dict[str, Any]] = {}
    try:
        for force in (None, "shadow", "per_row"):
            monkeypatch.setattr(srv, "READER_VIEW_FORCE", force)
            answers[force] = {
                f"{name}:{secret}": _scrub(
                    _call(mcp, *_request(operator, build(secret), limit=LIMIT)), [])
                for name, build in _probes(_id_field(operator)).items()
                for secret in (HIDDEN_VALUE, MASKED_VALUE, VISIBLE_VALUE)
            }
            if force is None:
                assert opened and {v.mode for v in opened} == {"class"}, [
                    (v.mode, v.reason) for v in opened]
            opened.clear()
    finally:
        _close(mcp)
    for force in ("shadow", "per_row"):
        problems = _diff(answers[force], answers[None])
        assert not problems, f"force={force}:\n" + "\n".join(problems[:40])
    for key, answer in answers[None].items():
        assert "ok" in answer, (key, answer)


def test_known_limitation_a_revealed_key_decides_on_rows_without_a_class(
    clean_runtime, tmp_path, monkeypatch,
):
    """Disclosed limitation (slice 7b, issue #3203): a row with no current
    read class (in practice a hidden row with no source lineage, which the
    per-row filter hides) is kept by the view's predicate, and readable() is
    TRUE for a key its subtype reveals, so a clause on that key decides in
    SQL whether the row is scanned and counts toward the 11,001-row scan
    bound. A key the subtype masks is unknown (NULL) there, so it cannot
    decide. This pins the current behavior; backfill and lineage close it."""
    from okg.substrate.mcp import server as srv

    deployment = _parity_deployment(tmp_path, clean_runtime)
    _set_mode(monkeypatch, "enforce")
    opened = _record_views(monkeypatch)
    mcp = _server(srv, clean_runtime, deployment)
    try:
        _call(mcp, *_request("filter", _eq(CONTROL_VALUE), limit=LIMIT))
    finally:
        _close(mcp)
    assert opened, "filter under enforce opened no reader view"
    view = opened[0]
    assert view.mode == "class", (view.mode, view.reason)
    revealed = "classification"
    assert not view.masked_attrs("comment").masked(revealed)
    assert view.masked_attrs("comment").masked("assignee")
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        row = conn.execute(
            "SELECT ({p}) AS kept, ({r}) AS revealed, ({m}) AS masked FROM (VALUES"
            " ('{{\"classification\": \"internal\"}}'::jsonb, 'comment'::text,"
            " NULL::uuid, NULL::smallint))"
            " AS r(attrs, subtype, nomos_read_class, nomos_read_class_version)".format(
                p=view.predicate("r").as_string(conn),
                r=view.readable("r", revealed).as_string(conn),
                m=view.readable("r", "assignee").as_string(conn),
            )
        ).fetchone()
    assert row == (True, True, None)


def _paged_ids(answer: dict[str, Any]) -> list[str]:
    return _ids(answer)


@pytest.mark.parametrize("operator", ["filter", "map", "lookup_where"])
def test_reaching_the_read_filter_bound_counts_only_rows_the_view_keeps(
    clean_runtime, tmp_path, monkeypatch, operator,
):
    """The #3208 scan bound, under the reader's view: hidden rows (whose class
    the view hides) and masked values never count toward it, so a value only
    hidden rows share no longer reaches it; readable rows still do. At the
    bound the page claims more MAY exist (`N+`), offers no cursor, and says
    paging stopped."""
    from okg.substrate.mcp import server as srv

    deployment = _parity_deployment(tmp_path, clean_runtime)
    _set_mode(monkeypatch, "enforce")
    monkeypatch.setattr(srv, "READ_FILTER_SCAN_CAP", 2)
    mcp = _server(srv, clean_runtime, deployment)
    try:
        shared = _call(mcp, *_request(operator, _eq(SHARED_VALUE), limit=LIMIT))
        hidden = _call(mcp, *_request(operator, _eq(HIDDEN_VALUE), limit=LIMIT))
        control = _call(mcp, *_request(operator, _eq(CONTROL_VALUE), limit=LIMIT))
    finally:
        _close(mcp)
    page = shared["ok"]
    assert _paged_ids(shared) == SHARED_READABLE[:2]
    assert page["truncated"] is True and page["next_offset"] is None
    assert page["pagination_capped"] is True and page["read_filter_scan_bound"] == 2
    assert "paging stops here" in page["pagination_notice"]
    if operator != "lookup_where":  # a `where` lookup reports no count
        assert (page["total_count"], page["total_count_basis"]) == (2, "lower_bound")
    assert not _diff(_scrub(hidden, _swaps(HIDDEN_VALUE)), _scrub(control, []))
    assert "read_filter_scan_bound" not in hidden["ok"]


def test_edge_attribute_clauses_run_under_the_reader_view(
    clean_runtime, tmp_path, monkeypatch,
):
    """The edge page joins the stored edge version for its read class too:
    `or` and `not` over an edge attribute answer under enforce, and a value
    nobody stored and an edge attribute every readable edge lacks agree."""
    from okg.substrate.mcp import server as srv

    deployment = _parity_deployment(tmp_path, clean_runtime)
    _set_mode(monkeypatch, "enforce")
    opened = _record_views(monkeypatch)
    mcp = _server(srv, clean_runtime, deployment)
    try:
        negated = _call(mcp, "filter", {
            "input": {"kind": "edges"},
            "predicate": {"op": "not", "clause": {
                "op": "eq", "field": "attrs.weight", "value": "heavy"}},
            "limit": 50,
        })
        either = _call(mcp, "filter", {
            "input": {"kind": "edges"},
            "predicate": {"op": "or", "clauses": [
                {"op": "eq", "field": "edge_type", "value": "contains"},
                {"op": "eq", "field": "attrs.weight", "value": "heavy"},
            ]},
            "limit": 50,
        })
    finally:
        _close(mcp)
    assert {v.row_kind for v in opened} == {"edge"}
    assert {v.mode for v in opened} == {"class"}, [(v.mode, v.reason) for v in opened]
    assert "ok" in negated, negated
    assert "ok" in either, either
    # No edge stores `weight`, so `not` matches every edge the reader may read.
    assert len(negated["ok"]["items"]) == len(either["ok"]["items"]) > 0


# --------------------------------------------------------------------------- #
# A redacted row with an accepted overlay on a key its subtype reveals         #
# --------------------------------------------------------------------------- #
# Nodes: the per-row filter applies accepted overlays BEFORE it decides and
# masks (read_filter._enriched_node_rows), so a redacted node is served its
# OVERLAID value on a revealed key, the value the SQL tests. Edges: the
# per-row filter does not apply overlays (_enriched_edge_rows), and the later
# overlay pass leaves a ReadRedactedAttrs result alone, so a redacted edge is
# served its STORED value; the SQL must not decide it on the overlay value
# (security review round 1, B1). Either way a clause matches the served value
# only, and an order follows it.
OVERLAY_STORED = ["stage-a", "stage-b", "stage-c"]
OVERLAY_VALUES = ["stage-z", "stage-y", "stage-x"]  # reverses the stored order
OVERLAY_IDS = [f"comment:overlay:{i}" for i in range(3)]
PLAIN_IDS = [f"comment:plain:{i}" for i in range(3)]
EDGE_STORED = "weight-stored"
EDGE_OVERLAY = "weight-overlay"


def _overlay_deployment(tmp_path, dsn: str) -> str:
    """Redacted comments (MASKED source) whose revealed `stage` (non_pii) and
    dropped `assignee` are stored, then given an accepted overlay on `stage`;
    one redacted `contains` edge whose revealed `weight` gets an overlay."""
    from okg.substrate.attributes import (
        materialize_effective_attributes_for_generation,
        record_attribute_observation,
    )
    from okg.substrate.library.sources.base import EdgeFact
    from okg.substrate.mcp import open_session

    groups = {
        OPEN: [
            _node("ticket:hub", "ticket", f"{QUERY} hub ticket."),
            _node("comment:visible", "comment", f"{QUERY} visible comment.",
                  stage="stage-m"),
            *[_node(node_id, "comment", f"{QUERY} plain comment {i}.")
              for i, node_id in enumerate(PLAIN_IDS)],
            # Unredacted edges whose `weight` gets an overlay.
            *[EdgeFact(
                src="ticket:hub", dst=node_id, edge_type="contains",
                attrs={"weight": f"plain-stored-{i}"},
                source_record_id={"id": f"ticket:hub|contains|{node_id}"},
                source_revision={"v": 1},
            ) for i, node_id in enumerate(PLAIN_IDS)],
        ],
        MASKED: [
            *[_node(node_id, "comment", f"{QUERY} overlay comment {i}.",
                    assignee=MASKED_VALUE, stage=OVERLAY_STORED[i])
              for i, node_id in enumerate(OVERLAY_IDS)],
            EdgeFact(
                src="ticket:hub", dst=OVERLAY_IDS[0], edge_type="contains",
                attrs={"weight": EDGE_STORED},
                source_record_id={"id": f"ticket:hub|contains|{OVERLAY_IDS[0]}"},
                source_revision={"v": 1},
            ),
        ],
    }
    main = [_edge("ticket:hub", "comment:visible")]
    deployment = _publish(tmp_path, dsn, groups=groups, main=main)
    _activate(dsn, deployment, [_allow(OPEN), _allow(MAIN), {
        "rule_id": "allow_tool_call", "effect": "allow", "actions": ["mcp.tool.call"],
        "subjects": [READER], "match": {}, "priority": 100,
    }, {
        "rule_id": "allow_read_boundary", "effect": "allow",
        "actions": ["graph.read.mcp"], "subjects": [READER],
        "match": {"resource_kind": "mcp_graph_read_boundary"}, "priority": 100,
    }, {
        "rule_id": "redact_masked", "effect": "redact", "actions": ["graph.read.mcp"],
        "subjects": [READER], "match": {"source_id": MASKED}, "priority": 100,
        "obligations": [{"type": "redaction"}, {"type": "audit"}],
    }])
    with psycopg.connect(dsn, autocommit=True) as conn:
        conn.execute(
            "UPDATE okg.subtypes SET metadata = coalesce(metadata, '{}'::jsonb)"
            " || '{\"pii_classes\": {\"assignee\": \"free_text\","
            " \"stage\": \"non_pii\"}, \"filterable_attrs\": {\"stage\": \"text\"}}'::jsonb"
            " WHERE name = 'comment'"
        )
        generation = str(open_session(conn, deployment_name=deployment).generation_id)
        # okg.graph_edges reads through a session pin; the stored edge sits in
        # its Chronos segment.
        segment = conn.execute(
            "SELECT physical_table FROM _chronos_branch_tables "
            "WHERE backend = 'interval' AND table_name = 'okg.graph_edges'"
        ).fetchone()[0]
        edges = conn.execute(
            f"SELECT edge_id, src, dst, edge_type FROM {segment} WHERE deleted = FALSE"
        ).fetchall()
        found = [e[0] for e in edges if e[1] == "ticket:hub" and e[2] == OVERLAY_IDS[0]]
        assert len(found) == 1, edges
        edge_id = found[0]
        plain = {e[2]: e[0] for e in edges if e[1] == "ticket:hub" and e[2] in PLAIN_IDS}
        assert sorted(plain) == PLAIN_IDS, edges
        for i, node_id in enumerate(PLAIN_IDS):
            record_attribute_observation(
                conn, target_kind="edge", target_id=str(plain[node_id]),
                generation_id=generation, attr_key="weight",
                value=f"plain-overlay-{i}", producer="reviewer",
            )
        for node_id, value in zip(OVERLAY_IDS, OVERLAY_VALUES):
            record_attribute_observation(
                conn, target_kind="node", target_id=node_id, generation_id=generation,
                attr_key="stage", value=value, producer="reviewer",
            )
        record_attribute_observation(
            conn, target_kind="edge", target_id=str(edge_id), generation_id=generation,
            attr_key="weight", value=EDGE_OVERLAY, producer="reviewer",
        )
        materialize_effective_attributes_for_generation(conn, generation_id=generation)
    return deployment


def _overlay_answers(mcp, srv, monkeypatch, force: str | None) -> dict[str, Any]:
    monkeypatch.setattr(srv, "READER_VIEW_FORCE", force)
    only = {"op": "in", "field": "node_id", "values": OVERLAY_IDS}

    def nodes(predicate: dict[str, Any]) -> dict[str, Any]:
        return _call(mcp, *_request("filter", {"op": "and", "clauses": [only, predicate]},
                                    limit=50))

    def edges(predicate: dict[str, Any]) -> dict[str, Any]:
        return _call(mcp, "filter", {
            "input": {"kind": "edges"}, "predicate": predicate, "limit": 50,
        })

    stage = "attrs.stage"
    return {
        "served": _call(mcp, *_request("filter", only, limit=50)),
        "eq_overlay": nodes({"op": "eq", "field": stage, "value": OVERLAY_VALUES[0]}),
        "eq_stored": nodes({"op": "eq", "field": stage, "value": OVERLAY_STORED[0]}),
        "not_overlay": nodes({"op": "not", "clause": {
            "op": "eq", "field": stage, "value": OVERLAY_VALUES[0]}}),
        "not_stored": nodes({"op": "not", "clause": {
            "op": "eq", "field": stage, "value": OVERLAY_STORED[0]}}),
        "gt_stored": nodes({"op": "gt", "field": stage, "value": "stage-a"}),
        "lookup_overlay": _call(mcp, *_request(
            "lookup_where", {"op": "eq", "field": stage, "value": OVERLAY_VALUES[1]},
            limit=50)),
        "lookup_stored": _call(mcp, *_request(
            "lookup_where", {"op": "eq", "field": stage, "value": OVERLAY_STORED[1]},
            limit=50)),
        "order_asc": _call(mcp, *_request("filter", only, limit=50,
                                          order_by="attrs.stage asc")),
        "order_desc": _call(mcp, *_request("filter", only, limit=50,
                                           order_by="attrs.stage desc")),
        "edge_eq_overlay": edges({"op": "eq", "field": "attrs.weight", "value": EDGE_OVERLAY}),
        "edge_eq_stored": edges({"op": "eq", "field": "attrs.weight", "value": EDGE_STORED}),
        "edge_not_overlay": edges({"op": "not", "clause": {
            "op": "eq", "field": "attrs.weight", "value": EDGE_OVERLAY}}),
        "edges_all": edges({"op": "eq", "field": "edge_type", "value": "contains"}),
    }


def _plain(value: Any) -> str:
    """A served string without the untrusted-data envelope lines."""
    return "\n".join(
        line for line in str(value).splitlines() if not line.startswith("\u27e6")
    ).strip()


def _edge_rows(answer: dict[str, Any]) -> list[dict[str, Any]]:
    assert "ok" in answer, answer
    return list(answer["ok"]["items"])


@pytest.mark.parametrize("mode", ["enforce", "audit"])
def test_a_redacted_row_with_an_overlay_is_decided_on_its_served_value(
    clean_runtime, tmp_path, monkeypatch, mode,
):
    """Security review round 1, B1: on a redacted row with an accepted
    overlay on a revealed key, eq, not and a range decide on the value the
    reader is served, never the other one: the overlay value on a node
    (filter and lookup; an order follows it), the stored value on an edge. The shadow check (per-row filter beside the class path, refusing
    any difference in rows, class decisions, readable() and the masks) and
    the per-row path give the same payloads."""
    from okg.substrate.mcp import server as srv

    deployment = _overlay_deployment(tmp_path, clean_runtime)
    mcp = _server(srv, clean_runtime, deployment)
    answers: dict[str | None, dict[str, Any]] = {}
    try:
        # The overlays are live: with Nomos off both serve the overlay value.
        _set_mode(monkeypatch, "off")
        off = _overlay_answers(mcp, srv, monkeypatch, None)
        assert _ids(off["eq_overlay"]) == [OVERLAY_IDS[0]], off["eq_overlay"]
        assert [_plain(row["dst"]) for row in _edge_rows(off["edge_eq_overlay"])] == [
            OVERLAY_IDS[0]], off["edge_eq_overlay"]
        _set_mode(monkeypatch, mode)
        opened = _record_views(monkeypatch)
        for force in (None, "shadow", "per_row"):
            answers[force] = _overlay_answers(mcp, srv, monkeypatch, force)
            if force is None:
                assert opened and {v.mode for v in opened} == {"class"}, [
                    (v.mode, v.reason) for v in opened]
                assert {v.row_kind for v in opened} == {"node", "edge"}
            opened.clear()
    finally:
        _close(mcp)
    got = answers[None]
    for key, answer in got.items():
        assert "ok" in answer, (key, answer)
    served = {row["node_id"]: row["attrs"] for row in got["served"]["ok"]["items"]}
    assert set(served) == set(OVERLAY_IDS), served
    for node_id, value in zip(OVERLAY_IDS, OVERLAY_VALUES):
        # Redacted (assignee dropped, marker set), served the overlay value.
        assert served[node_id].get("nomos_redacted") is True, served[node_id]
        assert "assignee" not in served[node_id], served[node_id]
        assert _plain(served[node_id]["stage"]) == value, served[node_id]
    assert _ids(got["eq_overlay"]) == [OVERLAY_IDS[0]]
    assert _ids(got["eq_stored"]) == []
    assert _ids(got["not_overlay"]) == OVERLAY_IDS[1:]
    assert _ids(got["not_stored"]) == OVERLAY_IDS
    # Every overlay value sorts after "stage-a"; the stored "stage-a" does not.
    assert _ids(got["gt_stored"]) == OVERLAY_IDS
    assert _ids(got["lookup_overlay"]) == [
        OVERLAY_IDS[1]]
    assert got["lookup_stored"]["ok"]["results"] == []
    # The stored order is 0, 1, 2; the served (overlay) order is 2, 1, 0.
    assert _ids(got["order_asc"]) == list(reversed(OVERLAY_IDS))
    assert _ids(got["order_desc"]) == OVERLAY_IDS
    edge = [row for row in _edge_rows(got["edges_all"])
            if _plain(row["dst"]) == OVERLAY_IDS[0]]
    assert len(edge) == 1 and edge[0]["attrs"].get("nomos_redacted") is True, edge
    # The redacted edge is served its stored weight, so only that matches.
    assert _plain(edge[0]["attrs"]["weight"]) == EDGE_STORED, edge
    assert _edge_rows(got["edge_eq_overlay"]) == []
    assert [_plain(row["dst"]) for row in _edge_rows(got["edge_eq_stored"])] == [
        OVERLAY_IDS[0]]
    negated = [_plain(row["dst"]) for row in _edge_rows(got["edge_not_overlay"])]
    assert OVERLAY_IDS[0] in negated and "comment:visible" in negated, negated
    for force in ("shadow", "per_row"):
        problems = _diff(_scrub(answers[force], []), _scrub(got, []))
        assert not problems, f"force={force}:\n" + "\n".join(problems[:40])


def test_shadow_refuses_a_redacts_value_the_per_row_filter_contradicts(
    clean_runtime, tmp_path, monkeypatch,
):
    """The shadow check compares ReaderView.redacts() with the per-row
    outcome: a view that claims an unredacted row comes back redacted is
    refused, not served."""
    from psycopg import sql

    from okg.substrate.mcp import server as srv
    from okg.substrate.nomos import reader_view

    deployment = _overlay_deployment(tmp_path, clean_runtime)
    _set_mode(monkeypatch, "enforce")
    mcp = _server(srv, clean_runtime, deployment)
    try:
        monkeypatch.setattr(srv, "READER_VIEW_FORCE", "shadow")
        visible = {"op": "eq", "field": "attrs.stage", "value": "stage-m"}
        honest = _call(mcp, *_request("filter", visible, limit=5))
        monkeypatch.setattr(reader_view.ReaderView, "redacts",
                            lambda self, alias: sql.SQL("TRUE"))
        lying = _call(mcp, *_request("filter", visible, limit=5))
    finally:
        _close(mcp)
    assert _ids(honest) == ["comment:visible"], honest
    assert lying.get("error") == "ReaderViewShadowMismatch", lying
    assert "redacts() disagrees" in lying["message"], lying


def test_an_unredacted_edge_with_an_overlay_is_decided_in_sql(
    clean_runtime, tmp_path, monkeypatch,
):
    """Review round 2, N3: an edge the view knows is served unredacted
    (ReaderView.redacts() FALSE) keeps its overlaid value decided in SQL, so
    a value no edge has scans no edge and the page stays below a lowered scan
    bound. A redacts() that is always unknown would leave every overlaid edge
    undecided, scanned whatever its value, and reach the bound: the second
    half proves the first half can fail."""
    from psycopg import sql

    from okg.substrate.mcp import server as srv
    from okg.substrate.nomos import reader_view

    deployment = _overlay_deployment(tmp_path, clean_runtime)
    _set_mode(monkeypatch, "enforce")
    monkeypatch.setattr(srv, "READ_FILTER_SCAN_CAP", 2)
    mcp = _server(srv, clean_runtime, deployment)
    probe = {"input": {"kind": "edges"}, "limit": 5, "predicate": {
        "op": "eq", "field": "attrs.weight", "value": "plain-overlay-nobody"}}
    served = {"input": {"kind": "edges"}, "limit": 5, "predicate": {
        "op": "eq", "field": "attrs.weight", "value": "plain-overlay-1"}}
    try:
        decided = _call(mcp, "filter", probe)
        match = _call(mcp, "filter", served)
        monkeypatch.setattr(reader_view.ReaderView, "redacts",
                            lambda self, alias: sql.SQL("CAST(NULL AS boolean)"))
        unknown = _call(mcp, "filter", probe)
    finally:
        _close(mcp)
    assert "ok" in decided and "ok" in match and "ok" in unknown, (decided, unknown)
    assert decided["ok"]["items"] == []
    assert "read_filter_scan_bound" not in decided["ok"], decided
    assert decided["ok"]["truncated"] is False, decided
    assert [_plain(row["dst"]) for row in match["ok"]["items"]] == [PLAIN_IDS[1]]
    assert unknown["ok"]["items"] == []
    assert unknown["ok"].get("read_filter_scan_bound") == 2, unknown


# --------------------------------------------------------------------------- #
# Review round 2: rows the view must not place                                 #
# --------------------------------------------------------------------------- #
SSN_SECRET = "ssn-secret-0001"
SSN_CONTROL = "ssn-nobody-0002"
SALARY_SECRET = "salary-secret-77"
SALARY_CONTROL = "salary-nobody-88"
MOVED_IDS = [f"comment:moved:{i}" for i in range(3)]
GONE_IDS = [f"comment:gone:{i}" for i in range(3)]


def _plain_rules(extra: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [_allow(OPEN), _allow(MAIN), {
        "rule_id": "allow_tool_call", "effect": "allow", "actions": ["mcp.tool.call"],
        "subjects": [READER], "match": {}, "priority": 100,
    }, {
        "rule_id": "allow_read_boundary", "effect": "allow",
        "actions": ["graph.read.mcp"], "subjects": [READER],
        "match": {"resource_kind": "mcp_graph_read_boundary"}, "priority": 100,
    }, *extra]


def _segment(conn, table: str) -> str:
    return conn.execute(
        "SELECT physical_table FROM _chronos_branch_tables "
        "WHERE backend = 'interval' AND table_name = %s", (table,),
    ).fetchone()[0]


def _bound_parity(mcp, srv, monkeypatch, field: str, secret: str, control: str,
                  ids: list[str]) -> list[str]:
    """filter, map and lookup on `secret` against `control`, with the scan
    bound lowered below the number of rows storing `secret` and with it
    unlowered; also `not`. Returns the differing leaves."""
    problems: list[str] = []
    for cap in (2, 11_001):
        monkeypatch.setattr(srv, "READ_FILTER_SCAN_CAP", cap)
        for operator in ("filter", "map", "lookup_where"):
            for name, build in (
                ("eq", lambda v: {"op": "eq", "field": field, "value": v}),
                ("not", lambda v: {"op": "not", "clause": {
                    "op": "eq", "field": field, "value": v}}),
            ):
                tool, s_kwargs = _request(operator, build(secret), limit=LIMIT)
                _, c_kwargs = _request(operator, build(control), limit=LIMIT)
                s_page, c_page = _call(mcp, tool, s_kwargs), _call(mcp, tool, c_kwargs)
                label = f"{operator} {name} cap={cap}"
                if "ok" not in s_page or "ok" not in c_page:
                    problems.append(f"{label}: refused {s_page} {c_page}")
                    continue
                if set(_ids(s_page)) & set(ids):
                    problems.append(f"{label}: served a hidden row {_ids(s_page)}")
                problems += [f"{label}: {line}" for line in _diff(
                    _scrub(s_page, [(secret, control)]), _scrub(c_page, []))]
    return problems


def test_a_classification_overlay_that_hides_a_class_keeps_its_keys_undecided(
    clean_runtime, tmp_path, monkeypatch,
):
    """Review round 2, S1: in overlay mode a class visible and unredacted
    under `internal` but hidden under `secret` must not count as
    never-redacted. Three comments of that class are overlaid to `secret`
    (the per-row filter hides them); a clause on their masked `ssn` must not
    decide in SQL whether they are scanned, so their value and a value nobody
    stored give the same page facts at a lowered scan bound."""
    from okg.substrate.attributes import (
        materialize_effective_attributes_for_generation,
        record_attribute_observation,
    )
    from okg.substrate.mcp import open_session
    from okg.substrate.mcp import server as srv

    groups = {OPEN: [
        _node("ticket:hub", "ticket", f"{QUERY} hub ticket."),
        _node("comment:visible", "comment", f"{QUERY} visible comment.",
              classification="internal", ssn="ssn-visible-0003"),
        *[_node(node_id, "comment", f"{QUERY} moved comment {i}.",
                classification="internal", ssn=SSN_SECRET)
          for i, node_id in enumerate(MOVED_IDS)],
    ]}
    deployment = _publish(tmp_path, clean_runtime, groups=groups,
                          main=[_edge("ticket:hub", "comment:visible")])
    _activate(clean_runtime, deployment, _plain_rules([{
        "rule_id": "deny_secret", "effect": "deny", "actions": ["graph.read.mcp"],
        "subjects": [READER], "match": {"classification": "secret"}, "priority": 300,
    }]))
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        conn.execute(
            "UPDATE okg.subtypes SET metadata = coalesce(metadata, '{}'::jsonb)"
            " || '{\"pii_classes\": {\"ssn\": \"direct_identifier\","
            " \"classification\": \"non_pii\"}}'::jsonb WHERE name = 'comment'"
        )
        generation = str(open_session(conn, deployment_name=deployment).generation_id)
        for node_id in MOVED_IDS:
            record_attribute_observation(
                conn, target_kind="node", target_id=node_id, generation_id=generation,
                attr_key="classification", value="secret", producer="reviewer",
            )
        materialize_effective_attributes_for_generation(conn, generation_id=generation)
    _set_mode(monkeypatch, "enforce")
    opened = _record_views(monkeypatch)
    mcp = _server(srv, clean_runtime, deployment)
    try:
        problems = _bound_parity(mcp, srv, monkeypatch, "attrs.ssn", SSN_SECRET,
                                 SSN_CONTROL, MOVED_IDS)
    finally:
        _close(mcp)
    node_views = [v for v in opened if v.row_kind == "node"]
    assert node_views and {(v.mode, v._state.overlay) for v in node_views} == {
        ("class", True)}, [(v.mode, v.reason, v._state.overlay) for v in node_views]
    assert not problems, "\n".join(problems[:40])


def test_a_join_miss_leaves_every_key_undecided(
    clean_runtime, tmp_path, monkeypatch,
):
    """Review round 2, S2: an effective row whose stored version the reader
    view's join cannot find has a NULL subtype there; read through the
    legacy classifier it would reveal `salary` and `grade`, which the comment
    subtype masks on these redacted rows. Their masked values must not decide
    in SQL whether they are scanned (their value and a value nobody stored
    give the same page facts at a lowered scan bound), and an order on
    `grade` sorts them as no value (after the readable one, by id), not by
    the stored grades, which run the other way."""
    from okg.substrate.mcp import server as srv
    from okg.substrate.nomos.reader_view import AttrMask

    legacy = AttrMask("", False, None)
    assert legacy.action("salary") == legacy.action("grade") == "reveal"
    groups = {
        OPEN: [
            _node("ticket:hub", "ticket", f"{QUERY} hub ticket."),
            _node("comment:visible", "comment", f"{QUERY} visible comment.",
                  salary="salary-visible-11", grade="grade-m"),
        ],
        MASKED: [
            _node(node_id, "comment", f"{QUERY} gone comment {i}.",
                  salary=SALARY_SECRET, grade=f"grade-{'zyx'[i]}")
            for i, node_id in enumerate(GONE_IDS)
        ],
    }
    deployment = _publish(tmp_path, clean_runtime, groups=groups,
                          main=[_edge("ticket:hub", "comment:visible")])
    _activate(clean_runtime, deployment, _plain_rules([{
        "rule_id": "redact_masked", "effect": "redact", "actions": ["graph.read.mcp"],
        "subjects": [READER], "match": {"source_id": MASKED}, "priority": 100,
        "obligations": [{"type": "redaction"}, {"type": "audit"}],
    }]))
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        conn.execute(
            "UPDATE okg.subtypes SET metadata = coalesce(metadata, '{}'::jsonb)"
            " || '{\"pii_classes\": {\"salary\": \"direct_identifier\","
            " \"grade\": \"direct_identifier\"},"
            " \"filterable_attrs\": {\"grade\": \"text\"}}'::jsonb WHERE name = 'comment'"
        )
        # The stored versions vanish from the segment the view joins; the
        # effective snapshot still lists the rows.
        conn.execute(
            f"UPDATE {_segment(conn, 'okg.graph_nodes')} SET deleted = TRUE"
            " WHERE node_id = ANY(%s)", (GONE_IDS,),
        )
    _set_mode(monkeypatch, "enforce")
    mcp = _server(srv, clean_runtime, deployment)
    try:
        problems = _bound_parity(mcp, srv, monkeypatch, "attrs.salary", SALARY_SECRET,
                                 SALARY_CONTROL, GONE_IDS)
        monkeypatch.setattr(srv, "READ_FILTER_SCAN_CAP", 11_001)
        ordered = _call(mcp, *_request("filter", {
            "op": "in", "field": "node_id", "values": ["comment:visible", *GONE_IDS]},
            limit=50, order_by="attrs.grade asc"))
    finally:
        _close(mcp)
    assert not problems, "\n".join(problems[:40])
    assert "ok" in ordered, ordered
    rows = ordered["ok"]["items"]
    assert all(row["attrs"].get("nomos_redacted") is True for row in rows[1:]), rows
    assert _ids(ordered) == ["comment:visible", *GONE_IDS], ordered


@pytest.mark.parametrize("force", [None, "shadow", "per_row"])
def test_lookup_attr_filters_never_match_a_withheld_value(
    clean_runtime, tmp_path, monkeypatch, force,
):
    """Review round 2, S3: under enforce the lookup's attr_filters recheck is
    three-valued: a filter on the mask token, or `None` on a key a redacted
    row dropped, never matches a masked row, whatever path the view takes;
    and the three paths answer alike."""
    from okg.substrate.mcp import server as srv
    from okg.substrate.nomos.read_masking import MASK_TOKEN

    deployment = _parity_deployment(tmp_path, clean_runtime)
    _set_mode(monkeypatch, "enforce")
    monkeypatch.setattr(srv, "READER_VIEW_FORCE", force)
    mcp = _server(srv, clean_runtime, deployment)

    def lookup(filters: dict[str, Any]) -> dict[str, Any]:
        return _call(mcp, "search", {
            "method": "attrs", "limit": 50, "subtype_filter": "comment",
            "attr_filters": filters,
        })

    try:
        token = lookup({"code": MASK_TOKEN})
        absent = lookup({"assignee": None})
        readable = lookup({"code": READABLE_CODE})
    finally:
        _close(mcp)
    for answer in (token, absent, readable):
        assert "ok" in answer, answer
    assert _ids(token) == [], token
    assert _ids(absent) == [], absent
    assert _ids(readable) == ["comment:visible"], readable


def test_a_classification_overlay_added_at_a_read_generation_is_seen(
    clean_runtime, tmp_path, monkeypatch,
):
    """Review round 2, S4: whether a classification overlay exists is read on
    every open, so an overlay materialized at a generation this process has
    already read switches the next view to overlay mode."""
    from okg.substrate.attributes import (
        materialize_effective_attributes_for_generation,
        record_attribute_observation,
    )
    from okg.substrate.mcp import open_session
    from okg.substrate.mcp import server as srv

    deployment = _parity_deployment(tmp_path, clean_runtime)
    _set_mode(monkeypatch, "enforce")
    opened = _record_views(monkeypatch)
    mcp = _server(srv, clean_runtime, deployment)
    try:
        _call(mcp, *_request("filter", _eq(CONTROL_VALUE), limit=LIMIT))
        before = [v._state.overlay for v in opened if v.row_kind == "node"]
        opened.clear()
        with psycopg.connect(clean_runtime, autocommit=True) as conn:
            generation = str(open_session(conn, deployment_name=deployment).generation_id)
            record_attribute_observation(
                conn, target_kind="node", target_id="comment:visible",
                generation_id=generation, attr_key="classification",
                value="restricted", producer="reviewer",
            )
            materialize_effective_attributes_for_generation(conn, generation_id=generation)
        _call(mcp, *_request("filter", _eq(CONTROL_VALUE), limit=LIMIT))
        after = [v._state.overlay for v in opened if v.row_kind == "node"]
    finally:
        _close(mcp)
    assert before == [False], before
    assert after == [True], after


def test_the_view_takes_the_callers_reading_of_the_nomos_mode(
    clean_runtime, tmp_path, monkeypatch,
):
    """Review round 2, N2: the page step reads the Nomos mode once and hands
    it to the reader view, which does not read it again."""
    from okg.substrate.mcp import server as srv
    from okg.substrate.nomos import reader_view

    deployment = _parity_deployment(tmp_path, clean_runtime)
    _set_mode(monkeypatch, "enforce")
    opened = _record_views(monkeypatch)
    mcp = _server(srv, clean_runtime, deployment)
    reads: list[str] = []
    real = reader_view.rf.deployment_requires_nomos_runtime

    def counting(name: str) -> bool:
        # Only the reader view's own readings count; the per-row filter has
        # its own, given the caller's value.
        if sys._getframe(1).f_globals.get("__name__") == reader_view.__name__:
            reads.append(name)
        return real(name)

    try:
        monkeypatch.setattr(reader_view.rf, "deployment_requires_nomos_runtime", counting)
        _call(mcp, *_request("filter", _eq(CONTROL_VALUE), limit=LIMIT))
        monkeypatch.setattr(reader_view.rf, "deployment_requires_nomos_runtime", real)
    finally:
        _close(mcp)
    assert opened and {v.mode for v in opened} == {"class"}
    assert reads == [], reads


# --------------------------------------------------------------------------- #
# No active bundle (review round 3, option a)                                  #
# --------------------------------------------------------------------------- #
NB_VISIBLE = "nb-visible@x"
NB_SECRET = "nb-secret@x"
NB_CONTROL = "nb-nobody@x"
NB_READABLE = [f"comment:nb:{i}" for i in range(4)]
NB_NO_LINEAGE = [f"comment:nolin:{i}" for i in range(3)]


def _no_bundle_deployment(tmp_path, dsn: str) -> str:
    """No policy bundle is ever activated. Four comments with lineage
    (readable under audit) carry `assignee`, one of them NB_VISIBLE; three
    comments lose their source lineage (in the effective attrs and the
    stored row, so the per-row filter hides them) and share NB_SECRET.
    `assignee` is a free-text key the comment subtype would drop if a row
    were redacted."""
    from okg.substrate.mcp import open_session

    groups = {OPEN: [
        _node("ticket:hub", "ticket", f"{QUERY} hub ticket."),
        *[_node(node_id, "comment", f"{QUERY} nb comment {i}.",
                assignee=NB_VISIBLE if i == 0 else f"nb-other-{i}@x")
          for i, node_id in enumerate(NB_READABLE)],
        *[_node(node_id, "comment", f"{QUERY} no-lineage comment {i}.", assignee=NB_SECRET)
          for i, node_id in enumerate(NB_NO_LINEAGE)],
    ]}
    deployment = _publish(tmp_path, dsn, groups=groups,
                          main=[_edge("ticket:hub", NB_READABLE[0])])
    with psycopg.connect(dsn, autocommit=True) as conn:
        conn.execute(
            "UPDATE okg.subtypes SET metadata = coalesce(metadata, '{}'::jsonb)"
            " || '{\"pii_classes\": {\"assignee\": \"free_text\"}}'::jsonb"
            " WHERE name = 'comment'"
        )
        conn.execute(
            "UPDATE okg.attribute_versions SET attrs = attrs - 'nomos_source_lineage',"
            " raw_attrs = CASE WHEN raw_attrs IS NULL THEN NULL"
            " ELSE raw_attrs - 'nomos_source_lineage' END"
            " WHERE target_kind = 'node' AND target_id = ANY(%s)", (NB_NO_LINEAGE,),
        )
        conn.execute(
            f"UPDATE {_segment(conn, 'okg.graph_nodes')}"
            " SET attrs = attrs - 'nomos_source_lineage', source = NULL"
            " WHERE node_id = ANY(%s)", (NB_NO_LINEAGE,),
        )
        active = conn.execute(
            "SELECT count(*) FROM okg.nomos_policy_activations"
            " WHERE deployment_name = %s AND status = 'active'", (deployment,),
        ).fetchone()[0]
        assert active == 0, "the no-bundle fixture must have no active bundle"
        open_session(conn, deployment_name=deployment)
    return deployment


def _nb_request(operator: str, predicate: dict[str, Any]) -> tuple[str, dict[str, Any]]:
    return _request(operator, predicate, limit=LIMIT)


def _nb_eq(value: str) -> dict[str, Any]:
    return {"op": "eq", "field": FIELD, "value": value}


@pytest.mark.parametrize("operator", ["filter", "map", "lookup_where"])
def test_no_bundle_audit_decides_attribute_clauses_in_sql(
    clean_runtime, tmp_path, monkeypatch, operator,
):
    """Under audit with no active bundle nothing is redacted, so a clause on
    a key the subtype would mask is decided in SQL: a value no row stores
    scans no row and a cap of four is not reached, where the per-row path
    scanned all seven comments; and a stored value is served as stored. The
    per-row filter serves those rows unredacted (verified here, not
    assumed)."""
    from okg.substrate.mcp import server as srv

    deployment = _no_bundle_deployment(tmp_path, clean_runtime)
    _set_mode(monkeypatch, "audit")
    opened = _record_views(monkeypatch)
    monkeypatch.setattr(srv, "READ_FILTER_SCAN_CAP", len(NB_NO_LINEAGE) + 1)
    mcp = _server(srv, clean_runtime, deployment)
    try:
        nobody = _call(mcp, *_nb_request(operator, {
            "op": "eq", "field": FIELD, "value": NB_CONTROL}))
        visible = _call(mcp, *_nb_request(operator, {
            "op": "eq", "field": FIELD, "value": NB_VISIBLE}))
        plain = _call(mcp, "filter", {
            "input": {"kind": "nodes", "subtype": "comment"},
            "predicate": {"op": "in", "field": "node_id", "values": NB_READABLE},
            "limit": 50,
        })
    finally:
        _close(mcp)
    assert {(v.mode, v.reason) for v in opened if v.row_kind == "node"} == {
        ("no_bundle", "no_active_bundle")}, [(v.mode, v.reason) for v in opened]
    assert "ok" in nobody and "ok" in visible and "ok" in plain, (nobody, visible)
    assert _ids(nobody) == []
    assert "read_filter_scan_bound" not in nobody["ok"], nobody
    assert nobody["ok"]["truncated"] is False, nobody
    assert _ids(visible) == [NB_READABLE[0]]
    items = plain["ok"]["items"]
    assert sorted(_ids(plain)) == NB_READABLE
    for row in items:
        assert row["attrs"].get("nomos_redacted") is not True, row
        assert "assignee" in row["attrs"], row


def test_known_limitation_no_bundle_rows_without_lineage_decide_scanning(
    clean_runtime, tmp_path, monkeypatch,
):
    """Disclosed limitation case (1) under no bundle + audit (slice 7b,
    issue #3203, closes it): every key is decided in SQL on the stored value
    and candidate narrowing stays on, so a row without source lineage (which
    the per-row filter hides) is scanned when its value matches and counts
    toward the scan bound. The rows themselves are never served: NB_SECRET
    returns nothing, like a value nobody stored, and only the scan-bound
    facts can differ. This pins the current behavior."""
    from okg.substrate.mcp import server as srv

    deployment = _no_bundle_deployment(tmp_path, clean_runtime)
    _set_mode(monkeypatch, "audit")
    monkeypatch.setattr(srv, "READ_FILTER_SCAN_CAP", 2)
    mcp = _server(srv, clean_runtime, deployment)
    try:
        secret = _call(mcp, *_nb_request("filter", _nb_eq(NB_SECRET)))
        control = _call(mcp, *_nb_request("filter", _nb_eq(NB_CONTROL)))
        monkeypatch.setattr(srv, "READ_FILTER_SCAN_CAP", 11_001)
        everything = _call(mcp, "filter", {
            "input": {"kind": "nodes", "subtype": "comment"},
            "predicate": {"op": "exists", "field": FIELD}, "limit": 50,
        })
    finally:
        _close(mcp)
    assert "ok" in secret and "ok" in control and "ok" in everything
    assert _ids(secret) == _ids(control) == []
    assert secret["ok"].get("read_filter_scan_bound") == 2, secret
    assert "read_filter_scan_bound" not in control["ok"], control
    assert sorted(_ids(everything)) == NB_READABLE


@pytest.mark.parametrize("operator", ["filter", "map", "lookup_where"])
def test_no_bundle_audit_pages_with_the_nomos_off_sql_and_narrowing(
    clean_runtime, tmp_path, monkeypatch, operator,
):
    """Under audit with no active bundle the page SQL is the Nomos-off SQL:
    the candidate narrowing is planned (it is withheld in class mode) and no
    reader-view WHERE wrapper is built; the hidden rows without lineage are
    still never served."""
    from okg.substrate.mcp import attr_filter_source as afs
    from okg.substrate.mcp import server as srv

    deployment = _no_bundle_deployment(tmp_path, clean_runtime)
    _set_mode(monkeypatch, "audit")
    monkeypatch.setenv(afs.SWITCH_ENV, "expression")
    planned: list[Any] = []
    wrapped: list[Any] = []
    real_plan = afs.plan_expression_candidates
    real_where = srv._reader_view_where

    def plan(*args, **kwargs):
        planned.append(kwargs.get("target_kind"))
        return real_plan(*args, **kwargs)

    def where(**kwargs):
        wrapped.append(kwargs.get("predicate"))
        return real_where(**kwargs)

    monkeypatch.setattr(afs, "plan_expression_candidates", plan)
    monkeypatch.setattr(srv, "_reader_view_where", where)
    mcp = _server(srv, clean_runtime, deployment)
    try:
        visible = _call(mcp, *_nb_request(operator, _nb_eq(NB_VISIBLE)))
        secret = _call(mcp, *_nb_request(operator, _nb_eq(NB_SECRET)))
    finally:
        _close(mcp)
    assert "ok" in visible and "ok" in secret, (visible, secret)
    assert _ids(visible) == [NB_READABLE[0]]
    assert _ids(secret) == []
    assert planned and set(planned) == {"node"}, planned
    assert wrapped == [], wrapped


@pytest.mark.parametrize("mode", ["audit", "enforce"])
def test_class_mode_withholds_narrowing_and_wraps_the_where(
    clean_runtime, tmp_path, monkeypatch, mode,
):
    """The control for the test above: with an active bundle (class mode)
    the narrowing is not planned and the reader-view WHERE is built."""
    from okg.substrate.mcp import attr_filter_source as afs
    from okg.substrate.mcp import server as srv

    deployment = _parity_deployment(tmp_path, clean_runtime)
    _set_mode(monkeypatch, mode)
    monkeypatch.setenv(afs.SWITCH_ENV, "expression")
    planned: list[Any] = []
    wrapped: list[Any] = []
    real_plan = afs.plan_expression_candidates
    real_where = srv._reader_view_where
    monkeypatch.setattr(afs, "plan_expression_candidates",
                        lambda *a, **k: planned.append(1) or real_plan(*a, **k))
    monkeypatch.setattr(srv, "_reader_view_where",
                        lambda **k: wrapped.append(1) or real_where(**k))
    mcp = _server(srv, clean_runtime, deployment)
    try:
        answer = _call(mcp, *_request("filter", _eq(VISIBLE_VALUE), limit=LIMIT))
    finally:
        _close(mcp)
    assert "ok" in answer, answer
    assert planned == [] and wrapped, (planned, wrapped)


def test_no_bundle_under_enforce_is_refused_before_any_read(
    clean_runtime, tmp_path, monkeypatch,
):
    """Enforce with no active bundle: the tool call is refused at
    `mcp.tool.call` (default deny), the same refusal for any value, so the
    view's FALSE predicate is only a backstop."""
    from okg.substrate.mcp import server as srv

    deployment = _no_bundle_deployment(tmp_path, clean_runtime)
    _set_mode(monkeypatch, "enforce")
    mcp = _server(srv, clean_runtime, deployment)
    problems: list[str] = []
    try:
        for operator in ("filter", "map", "lookup_where"):
            secret = _call(mcp, *_nb_request(operator, _nb_eq(NB_SECRET)))
            control = _call(mcp, *_nb_request(operator, _nb_eq(NB_CONTROL)))
            if secret.get("code") != "nomos_runtime_denied":
                problems.append(f"{operator}: not refused: {secret}")
            problems += [f"{operator}: {line}" for line in _diff(
                _scrub(secret, [(NB_SECRET, NB_CONTROL)]), _scrub(control, []))]
    finally:
        _close(mcp)
    assert not problems, "\n".join(problems[:40])


@pytest.mark.parametrize("mode", ["audit", "enforce"])
def test_no_bundle_shadow_and_per_row_answer_alike(
    clean_runtime, tmp_path, monkeypatch, mode,
):
    """The no-bundle shadow check (the per-row filter beside the view:
    nothing served under enforce, nothing redacted) and the forced per-row
    path answer as the default."""
    from okg.substrate.mcp import server as srv

    deployment = _no_bundle_deployment(tmp_path, clean_runtime)
    _set_mode(monkeypatch, mode)
    mcp = _server(srv, clean_runtime, deployment)
    answers: dict[str | None, dict[str, Any]] = {}
    try:
        for force in (None, "shadow", "per_row"):
            monkeypatch.setattr(srv, "READER_VIEW_FORCE", force)
            answers[force] = {
                f"{operator}:{name}:{value}": _scrub(_call(mcp, *_nb_request(operator, build)), [])
                for operator in ("filter", "map", "lookup_where")
                for value in (NB_VISIBLE, NB_SECRET, NB_CONTROL)
                for name, build in (
                    ("eq", {"op": "eq", "field": FIELD, "value": value}),
                    ("not", {"op": "not", "clause": {"op": "eq", "field": FIELD, "value": value}}),
                )
            }
    finally:
        _close(mcp)
    for key, answer in answers[None].items():
        # Under enforce the tool call is refused before any read.
        assert ("ok" in answer) if mode == "audit" else (
            answer.get("code") == "nomos_runtime_denied"), (key, answer)
    for force in ("shadow", "per_row"):
        problems = _diff(answers[force], answers[None])
        assert not problems, f"force={force}:\n" + "\n".join(problems[:40])


def test_no_bundle_view_refuses_when_a_bundle_is_activated_mid_call(
    clean_runtime, tmp_path, monkeypatch,
):
    """The view decided keys on the no-bundle answer; a bundle activated
    after it opened refuses the read instead of serving a page whose scan
    used that answer."""
    from okg.substrate.mcp import server as srv
    from okg.substrate.nomos import reader_view

    deployment = _no_bundle_deployment(tmp_path, clean_runtime)
    _set_mode(monkeypatch, "audit")
    real = reader_view.open

    def open_then_activate(*args, **kwargs):
        view = real(*args, **kwargs)
        _activate(clean_runtime, deployment, _plain_rules([]))
        return view

    monkeypatch.setattr(reader_view, "open", open_then_activate)
    mcp = _server(srv, clean_runtime, deployment)
    try:
        answer = _call(mcp, *_nb_request("filter", {
            "op": "eq", "field": FIELD, "value": NB_VISIBLE}))
    finally:
        _close(mcp)
    assert "ok" not in answer, answer
    # The view's own refusal (the read filter's says "read filter: ...").
    assert str(answer.get("message")).startswith("reader view: "), answer
    assert "one is active now" in str(answer.get("message")), answer


def _declare_assignee_ordered(dsn: str) -> None:
    with psycopg.connect(dsn, autocommit=True) as conn:
        conn.execute(
            "UPDATE okg.subtypes SET metadata = coalesce(metadata, '{}'::jsonb)"
            " || '{\"filterable_attrs\": {\"assignee\": \"text\"}}'::jsonb"
            " WHERE name = 'comment'"
        )


def _activate_in_final_read(monkeypatch, srv, activate: Callable[[], None]) -> list[bool]:
    """Wrap the page's node read filter so ``activate`` runs once, right
    before the first NON-count call (the final read of the page's rows),
    after every count batch has run. Returns a list that holds True once it
    has fired."""
    real = srv.filter_node_rows_for_session
    fired: list[bool] = []

    def wrapped(*args, **kwargs):
        if not kwargs.get("count") and not fired:
            fired.append(True)
            activate()
        return real(*args, **kwargs)

    monkeypatch.setattr(srv, "filter_node_rows_for_session", wrapped)
    return fired


@pytest.mark.parametrize("order_by", [None, "attrs.assignee asc"])
def test_no_bundle_refuses_a_bundle_activated_during_the_final_read(
    clean_runtime, tmp_path, monkeypatch, order_by,
):
    """A bundle activated after the last count batch, inside the final read
    of the page's rows, would have that read redact rows the no-bundle SQL
    matched and ordered on their stored values (an order_by oracle on now
    masked values). The view's check runs after that read and refuses."""
    from okg.substrate.mcp import server as srv

    deployment = _no_bundle_deployment(tmp_path, clean_runtime)
    _declare_assignee_ordered(clean_runtime)
    _set_mode(monkeypatch, "audit")
    redact_open = {
        "rule_id": "redact_open", "effect": "redact", "actions": ["graph.read.mcp"],
        "subjects": [READER], "match": {"source_id": OPEN}, "priority": 200,
        "obligations": [{"type": "redaction"}, {"type": "audit"}],
    }
    fired = _activate_in_final_read(monkeypatch, srv, lambda: _activate(
        clean_runtime, deployment, _plain_rules([redact_open])))
    mcp = _server(srv, clean_runtime, deployment)
    try:
        tool, kwargs = _request("filter", {"op": "exists", "field": FIELD}, limit=LIMIT,
                                order_by=order_by)
        answer = _call(mcp, tool, kwargs)
    finally:
        _close(mcp)
    assert fired == [True]
    assert "ok" not in answer, answer
    assert str(answer.get("message")).startswith("reader view: "), answer
    assert "one is active now" in str(answer.get("message")), answer


@pytest.mark.parametrize("mode", ["audit", "enforce"])
def test_class_mode_refuses_a_bundle_swapped_during_the_final_read(
    clean_runtime, tmp_path, monkeypatch, mode,
):
    """Class mode: the SQL decided keys on bundle A; a bundle B activated
    inside the final read would have that read apply B to rows A's SQL
    matched and ordered. The view compares the active fingerprint after the
    last read with the one it opened with and refuses."""
    from okg.substrate.mcp import server as srv

    deployment = _parity_deployment(tmp_path, clean_runtime)
    _declare_assignee_ordered(clean_runtime)
    _set_mode(monkeypatch, mode)

    def swap() -> None:
        # The operator's activation is itself a governed action: under the
        # test's enforce mode bundle A denies it, so it runs under audit and
        # the reader's mode is restored before the read continues.
        saved = os.environ.get("OKG_NOMOS_RUNTIME_ENFORCEMENT")
        os.environ["OKG_NOMOS_RUNTIME_ENFORCEMENT"] = "audit"
        try:
            _activate(clean_runtime, deployment, _plain_rules([]))
        finally:
            if saved is None:
                os.environ.pop("OKG_NOMOS_RUNTIME_ENFORCEMENT", None)
            else:
                os.environ["OKG_NOMOS_RUNTIME_ENFORCEMENT"] = saved

    fired = _activate_in_final_read(monkeypatch, srv, swap)
    mcp = _server(srv, clean_runtime, deployment)
    try:
        answer = _call(mcp, *_request("filter", {"op": "exists", "field": FIELD},
                                      limit=LIMIT, order_by="attrs.assignee asc"))
    finally:
        _close(mcp)
    assert fired == [True]
    assert "ok" not in answer, answer
    assert str(answer.get("message")).startswith(
        "reader view: the active policy bundle changed"), answer
