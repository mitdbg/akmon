"""expand's destination filter and order read the reader's view of the
destination (issue #3393; PACT read-model-slice7a-expand-3393).

Under Nomos `audit` and `enforce` a one-hop `expand` with a destination
`filter` or `order_by` decides on the destination node as the reader would be
served it: a hidden or masked value never matches and never orders, and a
destination the reader is not served (hidden, or absent at the pin) is wholly
withheld, so no filter matches it (hidden equals absent). Payloads for a secret value equal those for a value nobody stored.
`force="shadow"` and `force="per_row"` serve the same edges as the class
path. With Nomos off nothing changes.

The fixture is read-model slice 7a's (test_nomos_reader_view_predicates.py):
`ticket:hub` has a `contains` edge to one readable comment, four hidden ones
(HIDDEN_VALUE) and four masked ones (MASKED_VALUE, mask-tokened `code`). The
edges themselves are readable.
"""
from __future__ import annotations

import json
import os
from typing import Any

import psycopg
import pytest

from tests.substrate.mcp.test_nomos_reader_view_predicates import (
    CONTROL_VALUE,
    FIELD,
    HIDDEN_IDS,
    HIDDEN_VALUE,
    LIMIT,
    MASKED_IDS,
    MASKED_VALUE,
    VISIBLE_VALUE,
    _call,
    _declare_code_filterable,
    _diff,
    _parity_deployment,
    _plain_rules,
    _probes,
    _record_views,
    _scrub,
    _secrets,
    _segment,
    _server,
    _set_mode,
    _swaps,
)
from tests.substrate.mcp.test_nomos_reader_view_predicates import (
    _close as _close_server,
)
from tests.substrate.mcp.test_search_records import (
    OPEN,
    QUERY,
    SECRET,
    _activate,
    _edge,
    _node,
    _publish,
)

pytestmark = pytest.mark.postgres

HUB = "ticket:hub"
ALL_CHILDREN = ["comment:visible", *HIDDEN_IDS, *MASKED_IDS]


def _expand(predicate: dict[str, Any] | None, *, limit: int = LIMIT, offset: int = 0,
            order_by: str | None = None) -> dict[str, Any]:
    kwargs: dict[str, Any] = {
        "input": HUB, "edge_type": "contains", "direction": "out",
        "limit": limit, "offset": offset,
        # Issue #3666: a 1-hop row is the linked record unless `select`
        # names only the link's own fields. This file is about the link
        # read (`_expand_one_hop_effective`), so every call asks for the
        # whole link row, as the default was.
        "select": ["edge_id", "src", "dst", "edge_type", "direction",
                   "link.attrs", "read_provenance"],
    }
    if predicate is not None:
        kwargs["filter"] = predicate
    if order_by is not None:
        kwargs["order_by"] = order_by
    return kwargs


def _dsts(answer: dict[str, Any]) -> list[str]:
    assert "ok" in answer, answer
    return [str(row["dst"]).strip() for row in answer["ok"]["neighbors"]]


def _all_pages(mcp, predicate, *, order_by: str | None = None,
               limit: int = LIMIT) -> list[dict[str, Any]]:
    pages: list[dict[str, Any]] = []
    offset: int | None = 0
    while offset is not None:
        answer = _call(mcp, "expand", _expand(predicate, limit=limit, offset=offset,
                                              order_by=order_by))
        assert "ok" in answer, answer
        pages.append(answer)
        offset = answer["ok"]["next_offset"]
        assert len(pages) <= 12, "paging did not end"
    return pages


def _served(pages: list[dict[str, Any]]) -> list[str]:
    return [dst for page in pages for dst in _dsts(page)]


# --------------------------------------------------------------------------- #
# Secret-versus-control parity                                                 #
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("mode", ["enforce", "audit"])
def test_secret_and_control_give_identical_expand_payloads(
    clean_runtime, tmp_path, monkeypatch, mode,
):
    """Every probe of slice 7a (eq, lt, gt, between, an `or` with a stable
    branch, an `or` pinned to one hidden and one masked destination, `not`,
    a many-valued `in`) on a hidden (enforce) or masked (enforce, audit)
    destination value gives the same payload as the control - rows,
    total_count, next_offset, truncated - on the first and second page."""
    from okg.substrate.mcp import server as srv

    deployment = _parity_deployment(tmp_path, clean_runtime)
    _set_mode(monkeypatch, mode)
    opened = _record_views(monkeypatch)
    mcp = _server(srv, clean_runtime, deployment)
    problems: list[str] = []
    try:
        for name, build in sorted(_probes("node_id").items()):
            for secret in _secrets(mode):
                for offset in (0, LIMIT):
                    s_page = _call(mcp, "expand", _expand(build(secret), offset=offset))
                    c_page = _call(mcp, "expand", _expand(build(CONTROL_VALUE), offset=offset))
                    label = f"{name} secret={secret} offset={offset}"
                    if "ok" not in s_page or "ok" not in c_page:
                        problems.append(f"{label}: refused {s_page} {c_page}")
                        continue
                    problems += [f"{label}: {line}" for line in _diff(
                        _scrub(s_page, _swaps(secret)), _scrub(c_page, []))]
    finally:
        _close_server(mcp)
    assert {v.mode for v in opened if v.row_kind == "node"} == {"class"}, [
        (v.mode, v.reason) for v in opened]
    assert not problems, "\n".join(problems[:40])


@pytest.mark.parametrize("mode", ["enforce", "audit"])
def test_a_clause_on_a_masked_destination_value_never_matches(
    clean_runtime, tmp_path, monkeypatch, mode,
):
    """`eq` on the masked value serves no edge; `not` on it serves no edge to
    a masked destination (the clause is unknown there) but every edge to a
    readable one; an `or` whose stable branch names a masked destination
    still serves that edge."""
    from okg.substrate.mcp import server as srv

    deployment = _parity_deployment(tmp_path, clean_runtime)
    _set_mode(monkeypatch, mode)
    mcp = _server(srv, clean_runtime, deployment)
    try:
        equal = _call(mcp, "expand", _expand(
            {"op": "eq", "field": FIELD, "value": MASKED_VALUE}, limit=50))
        negated = _call(mcp, "expand", _expand(
            {"op": "not", "clause": {"op": "eq", "field": FIELD, "value": MASKED_VALUE}},
            limit=50))
        stable = _call(mcp, "expand", _expand({"op": "or", "clauses": [
            {"op": "eq", "field": "node_id", "value": MASKED_IDS[0]},
            {"op": "eq", "field": FIELD, "value": CONTROL_VALUE},
        ]}, limit=50))
    finally:
        _close_server(mcp)
    assert _dsts(equal) == []
    served = set(_dsts(negated))
    assert not served & set(MASKED_IDS), served
    assert "comment:visible" in served
    assert _dsts(stable) == [MASKED_IDS[0]]


def test_an_edge_to_a_hidden_destination_is_served_only_without_a_filter(
    clean_runtime, tmp_path, monkeypatch,
):
    """Under enforce the hidden comments' edges are readable but the comments
    are not. Without a filter the edge is served; under any filter the
    hidden destination is wholly withheld - every clause on it is unknown,
    `not` included - so its edge is not served, whatever the destination
    stores and whatever the filter names (its id included); a masked
    destination is served, so a clause on its id still decides."""
    from okg.substrate.mcp import server as srv

    deployment = _parity_deployment(tmp_path, clean_runtime)
    _set_mode(monkeypatch, "enforce")
    mcp = _server(srv, clean_runtime, deployment)
    try:
        plain = _call(mcp, "expand", _expand(None, limit=50))
        answers = {
            name: _call(mcp, "expand", _expand(predicate, limit=50))
            for name, predicate in {
                "eq": {"op": "eq", "field": FIELD, "value": HIDDEN_VALUE},
                "id": {"op": "eq", "field": "node_id", "value": HIDDEN_IDS[0]},
                "exists": {"op": "exists", "field": FIELD},
                "not_secret": {"op": "not", "clause": {
                    "op": "eq", "field": FIELD, "value": HIDDEN_VALUE}},
                "not_control": {"op": "not", "clause": {
                    "op": "eq", "field": FIELD, "value": CONTROL_VALUE}},
                "not_id": {"op": "not", "clause": {
                    "op": "eq", "field": "node_id", "value": "comment:visible"}},
            }.items()
        }
    finally:
        _close_server(mcp)
    assert set(HIDDEN_IDS) <= set(_dsts(plain)), _dsts(plain)
    for name, answer in answers.items():
        assert not set(_dsts(answer)) & set(HIDDEN_IDS), (name, answer)
    assert _dsts(answers["eq"]) == _dsts(answers["id"]) == []
    assert set(_dsts(answers["exists"])) == {"comment:visible"}
    assert set(_dsts(answers["not_secret"])) == {"comment:visible"}
    assert _diff(_scrub(answers["not_secret"], _swaps(HIDDEN_VALUE)),
                 _scrub(answers["not_control"], [])) == []
    # The id is not withheld on a served (masked) destination.
    assert set(_dsts(answers["not_id"])) == set(MASKED_IDS)


# --------------------------------------------------------------------------- #
# Order                                                                        #
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("direction", ["asc", "desc"])
@pytest.mark.parametrize("mode", ["enforce", "audit"])
def test_ordering_on_a_destination_attribute_uses_the_served_value(
    clean_runtime, tmp_path, monkeypatch, mode, direction,
):
    """`order_by attrs.code` sorts an edge by its destination's code as the
    reader is served it: a masked or hidden destination sorts as no value
    (after the readable one, by edge id), never by its stored code, across
    every page."""
    from okg.substrate.mcp import server as srv

    deployment = _parity_deployment(tmp_path, clean_runtime)
    _declare_code_filterable(clean_runtime)
    _set_mode(monkeypatch, mode)
    mcp = _server(srv, clean_runtime, deployment)
    try:
        plain = _call(mcp, "expand", _expand(None, limit=50))
        pages = _all_pages(mcp, None, order_by=f"attrs.code {direction}")
    finally:
        _close_server(mcp)
    by_edge = [str(row["dst"]).strip() for row in sorted(
        plain["ok"]["neighbors"], key=lambda row: int(row["edge_id"]))]
    assert _served(pages) == ["comment:visible"] + [
        dst for dst in by_edge if dst != "comment:visible"]


def test_under_nomos_off_expand_decides_on_stored_values(
    clean_runtime, tmp_path, monkeypatch,
):
    """With Nomos off nothing is hidden or masked: the filter and the order
    read the stored destination (the payload replay in the PACT evidence is
    the byte-level check)."""
    from okg.substrate.mcp import server as srv

    deployment = _parity_deployment(tmp_path, clean_runtime)
    _declare_code_filterable(clean_runtime)
    _set_mode(monkeypatch, "off")
    mcp = _server(srv, clean_runtime, deployment)
    try:
        hidden = _call(mcp, "expand", _expand(
            {"op": "eq", "field": FIELD, "value": HIDDEN_VALUE}, limit=50))
        masked = _call(mcp, "expand", _expand(
            {"op": "eq", "field": FIELD, "value": MASKED_VALUE}, limit=50))
        negated = _call(mcp, "expand", _expand(
            {"op": "not", "clause": {"op": "eq", "field": FIELD, "value": MASKED_VALUE}},
            limit=50))
        ordered = _call(mcp, "expand", _expand(
            {"op": "exists", "field": "attrs.code"}, limit=50, order_by="attrs.code asc"))
    finally:
        _close_server(mcp)
    assert sorted(_dsts(hidden)) == HIDDEN_IDS
    assert sorted(_dsts(masked)) == MASKED_IDS
    assert set(_dsts(negated)) == {"comment:visible", *HIDDEN_IDS}
    # Stored codes: KILO < alpha < kilo < zulu = zulu (then by edge id).
    assert _dsts(ordered)[:3] == ["comment:masked:1", "comment:visible", "comment:masked:0"]


# --------------------------------------------------------------------------- #
# shadow / per_row                                                             #
# --------------------------------------------------------------------------- #
def test_shadow_and_per_row_serve_the_edges_of_the_class_path(
    clean_runtime, tmp_path, monkeypatch,
):
    """On a real publish the destination view is in class mode. Forcing the
    shadow check (the per-row filter beside the class path on the page's
    destinations, refusing any difference) gives the same payloads and the
    same attribute order; forcing the per-row path serves the same edges in
    the same order over all pages for every filter (its SQL keeps undecided
    destinations, so its page boundaries differ: see the PACT). Per-row
    places no destination, so `code` is undecided on every comment and an
    order on it degrades to edge-id order (slice 7a's disclosure): that
    order is checked to be exactly the edge-id order, never a stored one."""
    from okg.substrate.mcp import server as srv

    deployment = _parity_deployment(tmp_path, clean_runtime)
    _declare_code_filterable(clean_runtime)
    _set_mode(monkeypatch, "enforce")
    opened = _record_views(monkeypatch)
    mcp = _server(srv, clean_runtime, deployment)
    first: dict[str | None, dict[str, Any]] = {}
    served: dict[str | None, dict[str, Any]] = {}
    try:
        plain = _call(mcp, "expand", _expand(None, limit=50))
        for force in (None, "shadow", "per_row"):
            monkeypatch.setattr(srv, "READER_VIEW_FORCE", force)
            calls = {
                f"{name}:{value}": build(value)
                for name, build in _probes("node_id").items()
                for value in (HIDDEN_VALUE, MASKED_VALUE, VISIBLE_VALUE)
            }
            first[force] = {
                key: _scrub(_call(mcp, "expand", _expand(predicate)), [])
                for key, predicate in calls.items()
            }
            served[force] = {
                key: _served(_all_pages(mcp, predicate)) for key, predicate in calls.items()
            }
            served[force]["order"] = _served(_all_pages(
                mcp, None, order_by="attrs.code desc"))
            modes = {v.mode for v in opened if v.row_kind == "node"}
            assert modes == ({"per_row"} if force == "per_row" else {"class"}), modes
            opened.clear()
    finally:
        _close_server(mcp)
    for key, answer in first[None].items():
        assert "ok" in answer, (key, answer)
    problems = _diff(first["shadow"], first[None])
    assert not problems, "force=shadow:\n" + "\n".join(problems[:40])
    per_row_order = served["per_row"].pop("order")
    for force in ("shadow", "per_row"):
        expected = {key: value for key, value in served[None].items()
                    if force == "shadow" or key != "order"}
        problems = _diff(served[force], expected)
        assert not problems, f"force={force}:\n" + "\n".join(problems[:40])
    by_edge = [str(row["dst"]).strip() for row in sorted(
        plain["ok"]["neighbors"], key=lambda row: int(row["edge_id"]))]
    assert per_row_order == by_edge, per_row_order


# --------------------------------------------------------------------------- #
# Read decisions                                                               #
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("mode", ["enforce", "audit"])
def test_destinations_are_counted_not_read(
    clean_runtime, tmp_path, monkeypatch, mode,
):
    """expand returns edges, not their destinations: the destination nodes
    the filter is checked on get `_count` decisions, never `_read` ones. The
    one exception is a served row's far end, whose subtype and title the row
    returns (PACT expand-neighbor-titles): it is read, so it gets a `_read`
    decision; a destination whose edge is not served never does."""
    from okg.substrate.mcp import server as srv

    deployment = _parity_deployment(tmp_path, clean_runtime)
    _set_mode(monkeypatch, mode)
    mcp = _server(srv, clean_runtime, deployment)
    try:
        answer = _call(mcp, "expand", _expand(
            {"op": "eq", "field": FIELD, "value": VISIBLE_VALUE}, limit=50))
    finally:
        _close_server(mcp)
    assert _dsts(answer) == ["comment:visible"]
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        rows = conn.execute(
            "SELECT target->>'node_id', target->>'kind' FROM okg.nomos_decisions"
            " WHERE deployment_name = %s AND target->>'node_id' IS NOT NULL",
            (deployment,),
        ).fetchall()
    read = {node_id for node_id, kind in rows if str(kind).endswith("_read")}
    counted = {node_id for node_id, kind in rows if str(kind).endswith("_count")}
    assert not read & (set(ALL_CHILDREN) - {"comment:visible"}), read
    assert "comment:visible" in read, read
    assert "comment:visible" in counted, counted


# --------------------------------------------------------------------------- #
# No active bundle                                                             #
# --------------------------------------------------------------------------- #
NB_VISIBLE = "nb-visible@x"
NB_SECRET = "nb-secret@x"
NB_CONTROL = "nb-nobody@x"
NB_READABLE = [f"comment:nb:{i}" for i in range(4)]
NB_NO_LINEAGE = [f"comment:nolin:{i}" for i in range(3)]


def _no_bundle_deployment(tmp_path, dsn: str) -> str:
    """No policy bundle is ever activated. The hub has an edge to four
    comments with lineage (readable under audit), one holding NB_VISIBLE,
    and to three comments without source lineage (the per-row filter hides
    them) sharing NB_SECRET."""
    from okg.substrate.mcp import open_session

    groups = {OPEN: [
        _node(HUB, "ticket", f"{QUERY} hub ticket."),
        *[_node(node_id, "comment", f"{QUERY} nb comment {i}.",
                assignee=NB_VISIBLE if i == 0 else f"nb-other-{i}@x")
          for i, node_id in enumerate(NB_READABLE)],
        *[_node(node_id, "comment", f"{QUERY} no-lineage comment {i}.", assignee=NB_SECRET)
          for i, node_id in enumerate(NB_NO_LINEAGE)],
    ]}
    deployment = _publish(tmp_path, dsn, groups=groups, main=[
        _edge(HUB, child) for child in (*NB_READABLE, *NB_NO_LINEAGE)])
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
        # A row without lineage carries no read class (read_class.class_facts).
        conn.execute(
            f"UPDATE {_segment(conn, 'okg.graph_nodes')}"
            " SET attrs = attrs - 'nomos_source_lineage', source = NULL,"
            " nomos_read_class = NULL, nomos_read_class_version = NULL"
            " WHERE node_id = ANY(%s)", (NB_NO_LINEAGE,),
        )
        active = conn.execute(
            "SELECT count(*) FROM okg.nomos_policy_activations"
            " WHERE deployment_name = %s AND status = 'active'", (deployment,),
        ).fetchone()[0]
        assert active == 0, "the no-bundle fixture must have no active bundle"
        open_session(conn, deployment_name=deployment)
    return deployment


def _nb_eq(value: str) -> dict[str, Any]:
    return {"op": "eq", "field": FIELD, "value": value}


def test_no_bundle_audit_uses_the_nomos_off_sql(
    clean_runtime, tmp_path, monkeypatch,
):
    """Audit with no active bundle: on a destination with a read class the
    clauses are the Nomos-off ones (no reader-view WHERE wrapper); a
    destination without lineage is hidden (and undecided in SQL),
    so it is withheld: `eq` on its value serves nothing, and `not` on its
    value serves the same edges as `not` on a value nobody stored (none to
    a hidden destination)."""
    from okg.substrate.mcp import server as srv

    deployment = _no_bundle_deployment(tmp_path, clean_runtime)
    _set_mode(monkeypatch, "audit")
    opened = _record_views(monkeypatch)
    wrapped: list[Any] = []
    real_where = srv._reader_view_where
    monkeypatch.setattr(srv, "_reader_view_where",
                        lambda **k: wrapped.append(1) or real_where(**k))
    mcp = _server(srv, clean_runtime, deployment)
    try:
        visible = _call(mcp, "expand", _expand(_nb_eq(NB_VISIBLE), limit=50))
        secret = _call(mcp, "expand", _expand(_nb_eq(NB_SECRET), limit=50))
        not_secret = _call(mcp, "expand", _expand(
            {"op": "not", "clause": _nb_eq(NB_SECRET)}, limit=50))
        not_control = _call(mcp, "expand", _expand(
            {"op": "not", "clause": _nb_eq(NB_CONTROL)}, limit=50))
    finally:
        _close_server(mcp)
    assert {(v.mode, v.reason) for v in opened if v.row_kind == "node"} == {
        ("no_bundle", "no_active_bundle")}, [(v.mode, v.reason) for v in opened]
    assert wrapped == [], wrapped
    assert _dsts(visible) == [NB_READABLE[0]]
    assert _dsts(secret) == []
    assert set(_dsts(not_secret)) == set(NB_READABLE)
    assert _diff(_scrub(not_secret, [(NB_SECRET, NB_CONTROL)]),
                 _scrub(not_control, [])) == []


@pytest.mark.parametrize("force", ["shadow", "per_row"])
def test_no_bundle_shadow_and_per_row_answer_alike(
    clean_runtime, tmp_path, monkeypatch, force,
):
    from okg.substrate.mcp import server as srv

    deployment = _no_bundle_deployment(tmp_path, clean_runtime)
    _set_mode(monkeypatch, "audit")
    mcp = _server(srv, clean_runtime, deployment)
    answers: dict[str | None, dict[str, Any]] = {}
    try:
        for which in (None, force):
            monkeypatch.setattr(srv, "READER_VIEW_FORCE", which)
            answers[which] = {
                f"{name}:{value}": _served(_all_pages(mcp, predicate))
                for value in (NB_VISIBLE, NB_SECRET, NB_CONTROL)
                for name, predicate in (
                    ("eq", _nb_eq(value)),
                    ("not", {"op": "not", "clause": _nb_eq(value)}),
                )
            }
    finally:
        _close_server(mcp)
    problems = _diff(answers[force], answers[None])
    assert not problems, "\n".join(problems[:40])


def test_no_bundle_view_refuses_when_a_bundle_is_activated_mid_call(
    clean_runtime, tmp_path, monkeypatch,
):
    """The destination SQL ran on the no-bundle answer; a bundle activated
    after the view opened refuses the read."""
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
        answer = _call(mcp, "expand", _expand(_nb_eq(NB_VISIBLE)))
    finally:
        _close_server(mcp)
    assert "ok" not in answer, answer
    assert str(answer.get("message")).startswith("reader view: "), answer
    assert "one is active now" in str(answer.get("message")), answer


@pytest.mark.parametrize("mode", ["audit", "enforce"])
def test_class_mode_refuses_a_bundle_swapped_during_the_final_read(
    clean_runtime, tmp_path, monkeypatch, mode,
):
    """A bundle B activated inside the final read (the edges') after the SQL
    decided destination keys on bundle A refuses the read."""
    from okg.substrate.mcp import server as srv

    deployment = _parity_deployment(tmp_path, clean_runtime)
    _set_mode(monkeypatch, mode)
    real = srv.filter_edge_rows_for_session
    fired: list[bool] = []

    def swap_then_read(*args, **kwargs):
        if not fired:
            fired.append(True)
            saved = os.environ.get("OKG_NOMOS_RUNTIME_ENFORCEMENT")
            os.environ["OKG_NOMOS_RUNTIME_ENFORCEMENT"] = "audit"
            try:
                _activate(clean_runtime, deployment, _plain_rules([]))
            finally:
                if saved is None:
                    os.environ.pop("OKG_NOMOS_RUNTIME_ENFORCEMENT", None)
                else:
                    os.environ["OKG_NOMOS_RUNTIME_ENFORCEMENT"] = saved
        return real(*args, **kwargs)

    monkeypatch.setattr(srv, "filter_edge_rows_for_session", swap_then_read)
    mcp = _server(srv, clean_runtime, deployment)
    try:
        answer = _call(mcp, "expand", _expand(
            {"op": "eq", "field": FIELD, "value": VISIBLE_VALUE}))
    finally:
        _close_server(mcp)
    assert fired == [True]
    assert "ok" not in answer, answer
    assert str(answer.get("message")).startswith(
        "reader view: the active policy bundle changed"), answer


@pytest.mark.parametrize("mode", ["audit", "enforce"])
def test_class_mode_refuses_a_bundle_swapped_during_the_neighbour_title_read(
    clean_runtime, tmp_path, monkeypatch, mode,
):
    """PACT expand-neighbor-titles: the neighbours' titles are the page's
    last read, so a bundle B activated inside it refuses the read rather
    than serve titles under a different bundle than the page's edges."""
    from okg.substrate.mcp import passage_rollup
    from okg.substrate.mcp import server as srv

    deployment = _parity_deployment(tmp_path, clean_runtime)
    _set_mode(monkeypatch, mode)
    real = passage_rollup.resolve_neighbor_summaries
    fired: list[bool] = []

    def swap_then_read(*args, **kwargs):
        if not fired:
            fired.append(True)
            saved = os.environ.get("OKG_NOMOS_RUNTIME_ENFORCEMENT")
            os.environ["OKG_NOMOS_RUNTIME_ENFORCEMENT"] = "audit"
            try:
                _activate(clean_runtime, deployment, _plain_rules([]))
            finally:
                if saved is None:
                    os.environ.pop("OKG_NOMOS_RUNTIME_ENFORCEMENT", None)
                else:
                    os.environ["OKG_NOMOS_RUNTIME_ENFORCEMENT"] = saved
        return real(*args, **kwargs)

    monkeypatch.setattr(
        passage_rollup, "resolve_neighbor_summaries", swap_then_read)
    mcp = _server(srv, clean_runtime, deployment)
    try:
        answer = _call(mcp, "expand", _expand(
            {"op": "eq", "field": FIELD, "value": VISIBLE_VALUE}))
    finally:
        _close_server(mcp)
    assert fired == [True]
    assert "ok" not in answer, answer
    assert str(answer.get("message")).startswith(
        "reader view: the active policy bundle changed"), answer


@pytest.mark.parametrize("direction", ["asc", "desc"])
def test_an_ordered_page_titles_only_what_the_reader_is_served(
    clean_runtime, tmp_path, monkeypatch, direction,
):
    """PACT expand-neighbor-titles under enforce, on an `order_by`-only page
    (the destination check counts; the title read reads): a hidden
    destination gets `neighbor: null` and none of its text, a masked one its
    subtype but no free-text title, and the readable one its title."""
    from okg.substrate.mcp import server as srv

    deployment = _parity_deployment(tmp_path, clean_runtime)
    _declare_code_filterable(clean_runtime)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        conn.execute(
            "UPDATE okg.subtypes SET metadata = metadata"
            " || jsonb_build_object('pii_classes', metadata->'pii_classes'"
            " || '{\"name\": \"free_text\"}'::jsonb) WHERE name = 'comment'"
        )
        titles = {child: f"title of {child}" for child in ALL_CHILDREN}
        segment = _segment(conn, "okg.graph_nodes")
        for child, title in titles.items():
            conn.execute(
                f"UPDATE {segment} SET attrs = attrs || jsonb_build_object('name', %s::text)"
                " WHERE node_id = %s", (title, child),
            )
            conn.execute(
                "UPDATE okg.attribute_versions SET attrs = attrs"
                " || jsonb_build_object('name', %s::text)"
                " WHERE target_kind = 'node' AND target_id = %s", (title, child),
            )
    _set_mode(monkeypatch, "enforce")
    mcp = _server(srv, clean_runtime, deployment)
    try:
        pages = _all_pages(mcp, None, order_by=f"attrs.code {direction}")
    finally:
        _close_server(mcp)
    rows = {str(row["dst"]).strip(): row for page in pages
            for row in page["ok"]["neighbors"]}
    assert set(rows) == set(ALL_CHILDREN), sorted(rows)
    visible = rows["comment:visible"]["neighbor"]
    assert visible["subtype"] == "comment", visible
    # The fixture's source is untrusted, so the title is fenced.
    assert titles["comment:visible"] in visible["title"], visible
    for hidden in HIDDEN_IDS:
        assert rows[hidden]["neighbor"] is None, rows[hidden]
    for masked in MASKED_IDS:
        assert rows[masked]["neighbor"]["subtype"] == "comment", rows[masked]
        assert rows[masked]["neighbor"]["title"] != titles[masked], rows[masked]
    served = json.dumps(pages)
    for child in (*HIDDEN_IDS, *MASKED_IDS):
        assert titles[child] not in served, child
    for value in (HIDDEN_VALUE, MASKED_VALUE):
        assert value not in served, value


# --------------------------------------------------------------------------- #
# Destinations the view cannot place (security review B1)                      #
# --------------------------------------------------------------------------- #
RANK = "attrs.rank"
SECRET_RANK = "s-secret-rank"
CONTROL_RANK = "t-nobody-rank"  # sorts above every stored rank
VISIBLE_IDS = [f"comment:v:{i}" for i in range(3)]
UNSEEN_IDS = [f"comment:h:{i}" for i in range(3)]


def _revealed_deployment(tmp_path, dsn: str, *, case: str) -> str:
    """The hub has a readable edge to three readable comments (ranks
    a-visible-i) and three comments the reader may not see, sharing
    SECRET_RANK. `rank` is non_pii, so the subtype REVEALS it (a redacted
    row would be served it as stored), and it is declared for ordering.

    * case "per_row" and "unplaced": a bundle allows OPEN and MAIN only, so
      the SECRET comments are hidden; "unplaced" also clears their stored
      read class, so the view cannot place them (it still hides them);
    * case "no_bundle": no bundle; the unseen comments lose their source
      lineage (and so their read class) and are hidden by the read filter.
    """
    from okg.substrate.mcp import open_session

    unseen_group = OPEN if case == "no_bundle" else SECRET
    groups: dict[str, list] = {OPEN: [
        _node(HUB, "ticket", f"{QUERY} hub ticket."),
        *[_node(node_id, "comment", f"{QUERY} visible comment {i}.", rank=f"a-visible-{i}")
          for i, node_id in enumerate(VISIBLE_IDS)],
    ]}
    groups.setdefault(unseen_group, []).extend(
        _node(node_id, "comment", f"{QUERY} unseen comment {i}.", rank=SECRET_RANK)
        for i, node_id in enumerate(UNSEEN_IDS)
    )
    deployment = _publish(tmp_path, dsn, groups=groups, main=[
        _edge(HUB, child) for child in (*VISIBLE_IDS, *UNSEEN_IDS)])
    if case != "no_bundle":
        _activate(dsn, deployment, _plain_rules([]))
    with psycopg.connect(dsn, autocommit=True) as conn:
        conn.execute(
            "UPDATE okg.subtypes SET metadata = coalesce(metadata, '{}'::jsonb)"
            " || '{\"pii_classes\": {\"assignee\": \"free_text\", \"rank\": \"non_pii\"},"
            " \"filterable_attrs\": {\"rank\": \"text\"}}'::jsonb"
            " WHERE name = 'comment'"
        )
        segment = _segment(conn, "okg.graph_nodes")
        if case == "unplaced":
            conn.execute(
                f"UPDATE {segment} SET nomos_read_class = NULL,"
                " nomos_read_class_version = NULL WHERE node_id = ANY(%s)", (UNSEEN_IDS,),
            )
        if case == "no_bundle":
            conn.execute(
                "UPDATE okg.attribute_versions SET attrs = attrs - 'nomos_source_lineage',"
                " raw_attrs = CASE WHEN raw_attrs IS NULL THEN NULL"
                " ELSE raw_attrs - 'nomos_source_lineage' END"
                " WHERE target_kind = 'node' AND target_id = ANY(%s)", (UNSEEN_IDS,),
            )
            conn.execute(
                f"UPDATE {segment} SET attrs = attrs - 'nomos_source_lineage', source = NULL,"
                " nomos_read_class = NULL, nomos_read_class_version = NULL"
                " WHERE node_id = ANY(%s)", (UNSEEN_IDS,),
            )
            open_session(conn, deployment_name=deployment)
    return deployment


def _below_rank(value: str) -> str:
    return value[:-1] + chr(ord(value[-1]) - 1)


def _decoys(value: str) -> dict[str, dict[str, Any]]:
    """Filters that admit a readable destination AND, on the guessed value, an
    unseen one: with limit 1 the page facts would say whether an unseen
    destination stores something above `value`."""
    anchor = {"op": "eq", "field": "node_id", "value": VISIBLE_IDS[-1]}
    return {
        "range": {"op": "or", "clauses": [
            {"op": "gt", "field": RANK, "value": _below_rank(value)}, anchor]},
        "eq": {"op": "or", "clauses": [
            {"op": "eq", "field": RANK, "value": value}, anchor]},
        # A stable field of the unseen destination (its subtype) must not
        # decide either; `value` picks a subtype only on the secret side.
        "stable": {"op": "or", "clauses": [{"op": "and", "clauses": [
            {"op": "eq", "field": "subtype",
             "value": "comment" if value == SECRET_RANK else "no-such-subtype"},
            {"op": "in", "field": "node_id", "values": UNSEEN_IDS},
        ]}, anchor]},
    }


def _unseen_case(monkeypatch, case: str) -> dict[str, Any] | None:
    """Mode for `case` and the authority the calls pass."""
    if case == "no_bundle":
        _set_mode(monkeypatch, "audit")
        return None
    _set_mode(monkeypatch, "enforce")
    # Any access context takes the view's per-row path (read_filter
    # _read_class_session_refusal), which places no row.
    return {"access_context": {}} if case == "per_row" else None


def _with_authority(kwargs: dict[str, Any], authority: dict[str, Any] | None) -> dict[str, Any]:
    return kwargs if authority is None else {**kwargs, "authority": authority}


@pytest.mark.parametrize("case", ["per_row", "unplaced", "no_bundle"])
def test_an_unplaced_destination_value_never_decides_the_page(
    clean_runtime, tmp_path, monkeypatch, case,
):
    """Security review B1. Where the view cannot place a destination - the
    per-row path a caller selects with any access context, a hidden row with
    no current read class, a lineage-less row under no-bundle audit - its
    stored values (a revealed key, a stable field) must not decide which
    edges the SQL keeps: a filter that admits one readable edge plus, on the
    guessed value, the unseen ones gives the same payload - neighbors,
    truncated, next_offset, counts - for the secret as for the control, with
    and without an order, on both pages."""
    from okg.substrate.mcp import server as srv

    deployment = _revealed_deployment(tmp_path, clean_runtime, case=case)
    authority = _unseen_case(monkeypatch, case)
    opened = _record_views(monkeypatch)
    mcp = _server(srv, clean_runtime, deployment)
    problems: list[str] = []
    try:
        for name in sorted(_decoys(SECRET_RANK)):
            for order_by in (None, "attrs.rank desc"):
                for offset in (0, 1):
                    pages = {
                        value: _call(mcp, "expand", _with_authority(_expand(
                            _decoys(value)[name], limit=1, offset=offset, order_by=order_by),
                            authority))
                        for value in (SECRET_RANK, CONTROL_RANK)
                    }
                    label = f"{name} order={order_by} offset={offset}"
                    if any("ok" not in page for page in pages.values()):
                        problems.append(f"{label}: refused {pages}")
                        continue
                    swaps = [(SECRET_RANK, CONTROL_RANK),
                             (_below_rank(SECRET_RANK), _below_rank(CONTROL_RANK)),
                             # The echoed filter value only: a served row's
                             # neighbor.subtype is "comment" on both sides.
                             ('"value": "comment"', '"value": "no-such-subtype"')]
                    problems += [f"{label}: {line}" for line in _diff(
                        _scrub(pages[SECRET_RANK], swaps), _scrub(pages[CONTROL_RANK], []))]
            # The decoy does match: the readable edge is served (one page).
            whole = _call(mcp, "expand", _with_authority(_expand(
                _decoys(SECRET_RANK)[name], limit=50), authority))
            assert _dsts(whole) == [VISIBLE_IDS[-1]], (name, whole)
    finally:
        _close_server(mcp)
    modes = {v.mode for v in opened if v.row_kind == "node"}
    assert modes == {"per_row" if case == "per_row" else (
        "no_bundle" if case == "no_bundle" else "class")}, [
        (v.mode, v.reason) for v in opened]
    assert not problems, "\n".join(problems[:40])


@pytest.mark.parametrize("case", ["per_row", "unplaced", "no_bundle"])
def test_an_unplaced_destination_value_never_orders_the_edges(
    clean_runtime, tmp_path, monkeypatch, case,
):
    """Security review B1. With no filter the edges to the unseen comments are
    served (the edges are readable), and an order on their revealed `rank`
    must not read the stored rank: they sort as no value, after every
    readable rank, by edge id (SECRET_RANK would put them first descending).
    On the per-row path no destination is placed, so the whole order is the
    edge-id order."""
    from okg.substrate.mcp import server as srv

    deployment = _revealed_deployment(tmp_path, clean_runtime, case=case)
    authority = _unseen_case(monkeypatch, case)
    mcp = _server(srv, clean_runtime, deployment)
    try:
        plain = _call(mcp, "expand", _with_authority(_expand(None, limit=50), authority))
        pages = []
        offset: int | None = 0
        while offset is not None:
            answer = _call(mcp, "expand", _with_authority(_expand(
                None, limit=2, offset=offset, order_by="attrs.rank desc"), authority))
            assert "ok" in answer, answer
            pages.append(answer)
            offset = answer["ok"]["next_offset"]
            assert len(pages) <= 6
    finally:
        _close_server(mcp)
    by_edge = [str(row["dst"]).strip() for row in sorted(
        plain["ok"]["neighbors"], key=lambda row: int(row["edge_id"]))]
    assert set(by_edge) == {*VISIBLE_IDS, *UNSEEN_IDS}, by_edge
    if case == "per_row":
        expected = by_edge
    else:
        expected = sorted(VISIBLE_IDS, reverse=True) + [
            dst for dst in by_edge if dst in UNSEEN_IDS]
    assert _served(pages) == expected


_ENVELOPES: list[tuple[Any, bool]] = [
    # (nomos_source_lineage value, whether the SQL guard places the row)
    (None, False),
    ("src-a", False),  # a string envelope is served, but left undecided
    ({"status": "ok", "sources": [{"source_id": "src-a"}]}, True),
    ({"sources": [{"source_id": "src-a"}]}, True),  # no status reads as "ok"
    ({"status": 7, "sources": [{"source_id": "src-a"}]}, True),
    ({"sources": [{"source_id": "src-a"}, {"source_id": "src-b"}]}, True),
    ({"sources": ["src-a", {"source_id": "src-b"}]}, True),
    ({"sources": [{"source_id": "   "}]}, False),
    ({"sources": [{"source_id": "　"}]}, False),
    ({"sources": [{"id": "src-a"}]}, False),
    ({"status": "malformed", "sources": [{"source_id": "src-a"}]}, False),
    ({"sources": []}, False),
    ({"sources": ["src-a"]}, False),
    ({"sources": [{"source_id": "src-a"}, {"source_id": None}]}, False),
    ({"sources": [{"source_id": 5}]}, False),
    ({"source_ids": ["src-a"]}, False),
]


def test_the_lineage_guard_never_places_a_row_the_read_filter_hides(clean_runtime):
    """The no-bundle placement condition is sufficient: wherever the SQL
    says a destination's lineage yields a source id, the per-row filter's own
    reading (normalize_source_lineage) finds one, so a placed destination is
    never one the read filter hides for want of lineage."""
    from okg.substrate.mcp import server as srv
    from okg.substrate.nomos.source_lineage import lineage_for_row, source_ids_from_lineage

    guard = srv._LINEAGE_SERVES_SQL.format(rel="n")
    problems: list[str] = []
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        for envelope, expected in _ENVELOPES:
            attrs = {} if envelope is None else {"nomos_source_lineage": envelope}
            placed = conn.execute(
                f"SELECT ({guard}) IS TRUE FROM (SELECT %s::jsonb AS attrs) AS n",
                (json.dumps(attrs),),
            ).fetchone()[0]
            served = bool(source_ids_from_lineage(lineage_for_row(attrs, source_id=None)))
            if placed and not served:
                problems.append(f"placed but hidden: {envelope!r}")
            if placed is not expected:
                problems.append(f"placed={placed} expected={expected}: {envelope!r}")
    assert not problems, "\n".join(problems)
