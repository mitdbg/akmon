"""A record's `text_hash` is served only to a reader who may read its text.

PACT nomos-withheld-text-hash-3378. `text_hash` is the unsalted MD5 of the
stored `text`. Under Nomos `audit` or `enforce`, a row whose `text` the read
filter masks or drops must not carry that digest, or a reader could confirm a
guessed text offline by hashing it. The read filter removes the digest where
it masks the row, so every operator that serves filtered rows inherits it.

The fixture holds, for one reader:

* `note:masked` -- redacted, `note.text` is `sensitive_attribute` (served
  as the mask token);
* `page:dropped` -- redacted, `page.text` is `free_text` (not served);
* `memo:revealed` -- redacted, but `memo.text` is `non_pii`, so the text is
  served as stored and its digest stays;
* `note:vis` and `page:vis` -- readable, not redacted.

Each check first reads the same rows with Nomos off, where every digest is
served, so a passing check is never an empty answer.
"""
from __future__ import annotations

import hashlib
import json
from typing import Any

import psycopg
import pytest

from tests.pytest.catalog_fixture_builders import SubtypeSpec
from tests.substrate.mcp.search_fixture import make_search_deployment, publish_nodes
from tests.substrate.mcp.test_search_records import (
    MAIN,
    MASKED,
    NARROWINGS,
    OPEN,
    READER,
    SUBTYPES,
    _activate,
    _allow,
    _edge,
    _node,
)

pytestmark = pytest.mark.postgres

TERM = "zirconium"
MEMO = SubtypeSpec("memo", metadata={"searchable_text_fields": ["text"]})

# node_id -> (subtype, stored text)
TEXTS: dict[str, tuple[str, str]] = {
    "note:masked": ("note", f"{TERM} masked note quasarmsk."),
    "comment:masked": ("comment", f"{TERM} masked comment quasarmsk."),
    "page:dropped": ("page", f"{TERM} dropped page quasardrp."),
    "memo:revealed": ("memo", f"{TERM} revealed memo quasarrev."),
    "note:vis": ("note", f"{TERM} visible note quasarvis."),
    "page:vis": ("page", f"{TERM} visible page quasarvis."),
    "comment:vis": ("comment", f"{TERM} visible comment quasarvis."),
    "ticket:hub": ("ticket", f"{TERM} hub ticket."),
}
WITHHELD = ("note:masked", "comment:masked", "page:dropped")
READABLE = ("memo:revealed", "note:vis", "page:vis")


def _md5(node_id: str) -> str:
    return hashlib.md5(TEXTS[node_id][1].encode("utf-8")).hexdigest()


def _rules() -> list[dict[str, Any]]:
    """READER reads OPEN and MAIN; MASKED rows are redacted."""
    return [_allow(OPEN), _allow(MAIN), {
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
    }]


def _deployment(tmp_path, dsn: str) -> str:
    groups = {
        OPEN: [_node(i, *TEXTS[i]) for i in ("note:vis", "page:vis", "comment:vis",
                                                "ticket:hub")],
        MASKED: [_node(i, *TEXTS[i]) for i in ("note:masked", "comment:masked",
                                                  "page:dropped", "memo:revealed")],
    }
    edges = [_edge("ticket:hub", "comment:vis"), _edge("ticket:hub", "comment:masked")]
    deployment = make_search_deployment(tmp_path, dsn=dsn)
    publish_nodes(
        dsn, deployment, subtypes=[*SUBTYPES, MEMO], narrowings=NARROWINGS,
        nodes=[], edges=edges, node_groups=list(groups.items()),
    )
    _activate(dsn, deployment, _rules())
    with psycopg.connect(dsn, autocommit=True) as conn:
        for subtype, pii in (("note", "sensitive_attribute"),
                             ("comment", "sensitive_attribute"),
                             ("page", "free_text"), ("memo", "non_pii")):
            conn.execute(
                "UPDATE okg.subtypes SET metadata = coalesce(metadata, '{}'::jsonb)"
                " || jsonb_build_object('pii_classes', jsonb_build_object('text', %s::text))"
                " WHERE name = %s",
                (pii, subtype),
            )
    return deployment


def _mode(monkeypatch, mode: str) -> None:
    if mode == "off":
        monkeypatch.delenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", raising=False)
        return
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", mode)
    monkeypatch.setenv("OKG_NOMOS_SUBJECT", READER)


def _read(dsn: str, deployment: str, operator: str, node_id: str) -> Any:
    from okg.substrate.mcp import server as srv

    subtype = TEXTS[node_id][0]
    mcp = srv.build_server(dsn=dsn, deployment_name=deployment)
    tools = mcp._tool_manager._tools
    by_id = {"op": "eq", "field": "node_id", "value": node_id}
    try:
        if operator == "filter":
            return tools["filter"].fn(input={"kind": "nodes", "subtype": subtype},
                                      predicate=by_id, limit=10)
        if operator == "map":
            return tools["map"].fn(input={"kind": "nodes", "subtype": subtype,
                                          "filter": by_id},
                                   projection=["node_id", "text_hash"], limit=10)
        if operator == "map_alias":
            # An aliased digest column: withheld by its source, not its name.
            return tools["map"].fn(input={"kind": "nodes", "subtype": subtype,
                                          "filter": by_id},
                                   projection={"node_id": "node_id", "h": "text_hash"},
                                   limit=10)
        if operator == "filter_select":
            return tools["filter"].fn(input={"kind": "nodes", "subtype": subtype},
                                      predicate=by_id, select=["node_id", "text_hash"],
                                      limit=10)
        if operator == "inspect_node":
            return tools["inspect"].fn(target={"kind": "node", "node_id": node_id})
        if operator == "lookup":
            return tools["search"].fn(method="attrs", limit=10, subtype_filter=subtype,
                                      where=by_id,
                                      select=["node_id", "subtype", "attrs", "text_hash"])
        if operator in ("lexical", "grep"):
            return tools["search"].fn(query=TERM, method=operator, limit=50,
                                      filter={"subtype": subtype},
                                      select=["node_id", "subtype", "attrs", "text_hash"])
        raise AssertionError(operator)
    finally:
        mcp._okg_pool.close()
        if mcp._okg_write_pool:
            mcp._okg_write_pool.close()


_OPERATORS = ("filter", "filter_select", "map", "map_alias", "inspect_node",
              "lookup", "lexical", "grep")


@pytest.mark.parametrize("mode", ["enforce", "audit"])
@pytest.mark.parametrize("operator", _OPERATORS)
def test_a_withheld_text_is_not_served_as_its_hash(
    clean_runtime, tmp_path, monkeypatch, operator, mode,
):
    """Masked and dropped texts lose their digest on every operator; a
    readable or revealed text keeps it; Nomos off serves every digest."""
    deployment = _deployment(tmp_path, clean_runtime)
    for node_id in (*WITHHELD, *READABLE):
        _mode(monkeypatch, "off")
        published = json.dumps(_read(clean_runtime, deployment, operator, node_id),
                               default=str)
        # A search that does not reach the row with Nomos off would make the
        # withheld check vacuous for it; only rows it does reach are judged.
        reached = node_id in published
        if operator in ("filter", "filter_select", "map", "map_alias",
                        "inspect_node", "lookup"):
            assert reached, (node_id, published[:400])
        if not reached:
            continue
        assert _md5(node_id) in published, (node_id, "Nomos off serves the digest")
        _mode(monkeypatch, mode)
        served = json.dumps(_read(clean_runtime, deployment, operator, node_id),
                            default=str)
        if node_id in WITHHELD:
            assert _md5(node_id) not in served, (operator, mode, node_id, served[:600])
        elif node_id in served:
            assert _md5(node_id) in served, (operator, mode, node_id, served[:600])


def test_the_fixture_masks_drops_and_reveals(clean_runtime, tmp_path, monkeypatch):
    """Fixture guard: under enforce the masked note is served with the mask
    token, the dropped page without `text`, and the redacted memo with its
    text as stored, all as readable rows. A filter row has fixed columns,
    so a withheld digest is null there; `inspect` leaves the key out."""
    deployment = _deployment(tmp_path, clean_runtime)
    _mode(monkeypatch, "enforce")
    rows = {}
    for node_id in ("note:masked", "page:dropped", "memo:revealed", "note:vis"):
        items = _read(clean_runtime, deployment, "filter", node_id)["items"]
        assert [row["node_id"] for row in items] == [node_id]
        rows[node_id] = items[0]
    assert "[nomos:masked]" in str(rows["note:masked"]["attrs"].get("text"))
    assert "text" not in rows["page:dropped"]["attrs"]
    assert TEXTS["memo:revealed"][1] in str(rows["memo:revealed"]["attrs"].get("text"))
    assert rows["memo:revealed"]["attrs"].get("nomos_redacted") is True
    assert rows["note:masked"].get("text_hash") is None
    assert rows["page:dropped"].get("text_hash") is None
    assert rows["memo:revealed"]["text_hash"] == _md5("memo:revealed")
    assert rows["note:vis"]["text_hash"] == _md5("note:vis")
    for node_id in ("note:masked", "page:dropped"):
        inspected = _read(clean_runtime, deployment, "inspect_node", node_id)
        assert inspected["node_id"] == node_id
        assert "text_hash" not in inspected
    inspected = _read(clean_runtime, deployment, "inspect_node", "memo:revealed")
    assert inspected["text_hash"] == _md5("memo:revealed")



# ---------- a text digest as a predicate, sort or group key ----------
#
# SQL evaluates these on the stored digest before the read check, so under
# Nomos each is refused by name before any SQL runs. With Nomos off each
# still answers.

def _eq(value: str) -> dict[str, Any]:
    return {"op": "eq", "field": "text_hash", "value": value}


_COMMENTS = {"kind": "nodes", "subtype": "comment"}

# name -> (tool, kwargs from a digest value, refused clause path or None when
# search's own `where` grammar refuses the field)
_DIGEST_CASES: dict[str, tuple[str, Any, str | None]] = {
    "filter_eq": ("filter", lambda v: {
        "input": _COMMENTS, "predicate": _eq(v), "limit": 10}, "predicate.field"),
    "filter_not": ("filter", lambda v: {
        "input": _COMMENTS, "predicate": {"op": "not", "clause": _eq(v)},
        "limit": 10}, "predicate.clause.field"),
    "filter_not_prefix": ("filter", lambda v: {
        "input": _COMMENTS, "predicate": {"op": "not", "clause": {
            "op": "prefix", "field": "text_hash", "value": v[:1]}},
        "limit": 10}, "predicate.clause.field"),
    "filter_or_limit_1": ("filter", lambda v: {
        "input": _COMMENTS, "predicate": {"op": "or", "clauses": [
            _eq(v), {"op": "eq", "field": "node_id", "value": "comment:vis"}]},
        "limit": 1}, "predicate.clauses[0].field"),
    "filter_input_filter": ("filter", lambda v: {
        "input": {**_COMMENTS, "filter": _eq(v)},
        "predicate": {"op": "exists", "field": "node_id"}, "limit": 10},
        "input.filter.field"),
    "filter_order_by": ("filter", lambda v: {
        "input": _COMMENTS, "predicate": {"op": "exists", "field": "node_id"},
        "order_by": "text_hash desc", "limit": 10}, "order_by"),
    "filter_reference_page": ("filter", lambda v: {
        "input": {"ref": "REF"}, "predicate": _eq(v), "limit": 10},
        "predicate.field"),
    "map_input_filter": ("map", lambda v: {
        "input": {**_COMMENTS, "filter": _eq(v)},
        "projection": ["node_id", "attrs.text"], "limit": 10}, "input.filter.field"),
    "aggregate_by": ("aggregate", lambda v: {
        "input": _COMMENTS, "by": ["text_hash"], "metrics": ["count"]}, "by[0]"),
    "expand_filter": ("expand", lambda v: {
        "input": "ticket:hub", "edge_type": "contains", "direction": "out",
        "filter": _eq(v), "limit": 10}, "filter.field"),
    "expand_order_by": ("expand", lambda v: {
        "input": "ticket:hub", "edge_type": "contains", "direction": "out",
        "order_by": "text_hash", "limit": 10}, "order_by"),
    "lookup_where": ("search", lambda v: {
        "method": "attrs", "limit": 10, "subtype_filter": "comment",
        "where": _eq(v)}, None),
}
# Cases whose Nomos-off answer is checked to still find the readable comment.
_OFF_ANSWERS = ("filter_eq", "filter_input_filter", "filter_order_by",
                "map_input_filter", "expand_filter", "expand_order_by")


def _call(mcp, tool: str, kwargs: dict[str, Any]) -> dict[str, Any]:
    try:
        return {"ok": mcp._tool_manager._tools[tool].fn(**kwargs)}
    except Exception as error:
        return {"raised": type(error).__name__, "message": str(error),
                "code": getattr(error, "code", None)}


def _retained_reference(mcp) -> str:
    page = mcp._tool_manager._tools["search"].fn(query=TERM, retain=True, limit=10)
    return page["result_reference"]["id"]


@pytest.mark.parametrize("mode", ["enforce", "audit"])
@pytest.mark.parametrize("case", sorted(_DIGEST_CASES))
def test_a_text_digest_clause_is_refused_under_nomos(
    clean_runtime, tmp_path, monkeypatch, case, mode,
):
    from okg.substrate.mcp import server as srv

    deployment = _deployment(tmp_path, clean_runtime)
    tool, build, clause = _DIGEST_CASES[case]
    _mode(monkeypatch, mode)
    mcp = srv.build_server(dsn=clean_runtime, deployment_name=deployment)
    try:
        kwargs = build(_md5("comment:masked"))
        if kwargs.get("input") == {"ref": "REF"}:
            kwargs["input"] = {"ref": _retained_reference(mcp)}
        result = _call(mcp, tool, kwargs)
    finally:
        mcp._okg_pool.close()
        if mcp._okg_write_pool:
            mcp._okg_write_pool.close()
    if clause is None:
        # search's `where` grammar has no text_hash field at all.
        assert "raised" in result and "text_hash" in result["message"], result
        return
    refusal = result.get("ok")
    assert isinstance(refusal, dict) and refusal.get("status") == "refused", (
        json.dumps(result, default=str)[:800])
    assert refusal["error"]["code"] == "unsupported_operator_form"
    assert refusal["error"]["data"] == {"field": "text_hash", "clause": clause}


@pytest.mark.parametrize("case", _OFF_ANSWERS)
def test_a_text_digest_clause_still_answers_with_nomos_off(
    clean_runtime, tmp_path, monkeypatch, case,
):
    from okg.substrate.mcp import server as srv

    deployment = _deployment(tmp_path, clean_runtime)
    tool, build, _ = _DIGEST_CASES[case]
    _mode(monkeypatch, "off")
    mcp = srv.build_server(dsn=clean_runtime, deployment_name=deployment)
    try:
        result = _call(mcp, tool, build(_md5("comment:vis")))
    finally:
        mcp._okg_pool.close()
        if mcp._okg_write_pool:
            mcp._okg_write_pool.close()
    assert "ok" in result and result["ok"].get("status") != "refused", (
        json.dumps(result, default=str)[:800])
    assert "comment:vis" in json.dumps(result["ok"], default=str)
