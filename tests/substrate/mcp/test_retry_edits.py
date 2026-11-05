"""A refusal's declared change applied to the caller's call (PACT read-refusal-retry-calls).

No database: the change itself. That each declared change is offered and
served through a real server is `test_read_grammar_db.py`'s
`test_a_refusal_one_change_fixes_names_that_call_and_it_is_served`.
"""
from __future__ import annotations

import pytest

from okg.substrate.errors import SubstrateError
from okg.substrate.mcp.retry_edits import declare_retry_edit, edited_call


def test_a_drop_removes_only_what_the_caller_sent():
    sent = {"query": "x", "within": "record", "select": ["node_id"]}
    assert edited_call(sent, {"drop": ("within",), "use": {}}) == {
        "query": "x", "select": ["node_id"],
    }
    assert sent == {"query": "x", "within": "record", "select": ["node_id"]}


def test_a_change_that_changes_nothing_is_no_call():
    sent = {"query": "x", "method": "grep"}
    assert edited_call(sent, {"drop": ("within",), "use": {}}) is None
    assert edited_call(sent, {"drop": (), "use": {"method": "grep"}}) is None


def test_a_named_value_replaces_the_callers():
    assert edited_call({"query": "x", "method": "lexical"}, {
        "drop": (), "use": {"method": "grep"},
    }) == {"query": "x", "method": "grep"}


def test_declarations_are_kept_in_order_and_the_error_is_returned():
    error = SubstrateError("refused", fix_hint="drop within")
    assert declare_retry_edit(error, drop=("within",)) is error
    declare_retry_edit(error, drop=("within", "select"), mode="guess")
    assert [edit["drop"] for edit in error.retry_edits] == [("within",), ("within", "select")]
    assert [edit["mode"] for edit in error.retry_edits] == ["corrected", "guess"]


def test_an_unknown_mode_is_refused():
    with pytest.raises(ValueError):
        declare_retry_edit(SubstrateError("refused"), drop=("x",), mode="maybe")


def test_a_run_call_is_never_offered():
    """A one-change `run` is never offered: its tool body is never run to
    check it, since its steps hold calls."""
    from okg.substrate.mcp.response_bounds import ByteBoundedFastMCP

    server = ByteBoundedFastMCP("offer-test")
    assert server._okg_tool_checks_pass("run", {"steps": [{"search": {}}]}) is False
