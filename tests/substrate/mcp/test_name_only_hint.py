"""A read of file names names the grep over file texts (lane ri, CompOps q020).

The plans are built from the calls the agents sent in the crfix3 and sweep
runs, rewritten exactly as the read tools receive them (`rewrite_call`). The
served replies over a real publish are in `test_name_only_hint_db.py`.
"""
from __future__ import annotations

from typing import Any, Optional

import pytest

from okg.substrate.mcp import name_only_hint as names
from okg.substrate.mcp.read_grammar import rewrite_call
from okg.substrate.mcp.result_text import _grep_searched_notice


def _filter_predicate(arguments: dict[str, Any]) -> Any:
    """The predicate `filter` composes from a call (`_compose_scope_predicate`)."""
    args = rewrite_call("filter", arguments).arguments
    clauses: list[Any] = []
    scope = args.get("input")
    if isinstance(scope, dict) and scope.get("subtype"):
        clauses.append({"op": "eq", "field": "subtype", "value": scope["subtype"]})
    if isinstance(scope, dict) and scope.get("filter"):
        clauses.append(scope["filter"])
    predicate = args.get("predicate")
    if predicate not in (None, True, {}):
        clauses.append(predicate)
    if not clauses:
        return None
    return clauses[0] if len(clauses) == 1 else {"op": "and", "clauses": clauses}


def _plan(tool: str, arguments: dict[str, Any]) -> Optional[names.Plan]:
    if tool == "filter":
        return names.plan(_filter_predicate(arguments), query=None, method="filter")
    args = rewrite_call("search", arguments).arguments
    query = args.get("query")
    method = args.get("method", "lexical")
    if method == "lexical" and not query:
        method = "attrs"
    return names.plan(
        args.get("where"), query=query, method=method,
        subtype_filter=args.get("subtype_filter"), path_prefix=args.get("path_prefix"),
    )


FLUX_DOCS = {"type": "document", "id": {"op": "prefix", "value": "doc:rucioflux:"}}

#: (label, tool, the agent's arguments, reason, grep query, named `where`)
TRANSCRIPT_SHAPES = [
    ("R1 crfix3-medium a1 call 5", "filter",
     {"where": {"type": "source_file", "attrs.repo": "rucioflux",
                "attrs.path": {"op": "contains", "value": "reaper"}},
      "limit": 100, "select": ["node_id", "attrs.path"]},
     "path_test", "reaper", FLUX_DOCS),
    ("R2 crfix3-medium a3 call 6", "search",
     {"method": "grep", "query": "reaper",
      "where": {"type": "source_file", "attrs.repo": "rucioflux"}, "limit": 60,
      "select": ["node_id", "attrs.path", "attrs.last_modified_at", "attrs.citation_url"]},
     "file_records", "reaper", FLUX_DOCS),
    ("R3 crfix3-medium a3 call 5", "search",
     {"method": "grep", "query": "reaper", "where": {"type": "source_file"}, "limit": 20,
      "select": ["node_id", "attrs.path", "attrs.name", "source"]},
     "file_records", "reaper", {"type": "document"}),
    ("R4 crfix3-medium a1 call 3", "search",
     {"query": "reaper", "where": {"type": "source_file"}, "limit": 20,
      "select": ["node_id", "attrs.path", "attrs.repo"]},
     "file_records", "reaper", {"type": "document"}),
    ("R5 csw-okg-low a3 call 6", "filter",
     {"where": {"type": "document",
                "attrs.path": {"op": "contains", "value": "reaper-rucio-daemons"}},
      "limit": 30, "select": ["node_id", "attrs.path"]},
     "path_test", '"reaper-rucio-daemons"', {"type": "document"}),
    ("R6 crfix3-medium a2 call 5", "filter",
     {"where": {"type": "document", "attrs.path": {"op": "contains", "value": "reaper"}},
      "limit": 50, "select": ["node_id", "attrs.path", "attrs.name", "source"]},
     "path_test", "reaper", {"type": "document"}),
    ("R7 cokg-nemotron35s-high a3 call 5", "search",
     {"query": "tier1-reaper-rucio-daemons configmap chunk_size rses", "limit": 20,
      "where": {"type": "source_file"}},
     "file_records", "tier1-reaper-rucio-daemons configmap chunk_size rses",
     {"type": "document"}),
    # r36d new q020 t1 call 8: a text query AND a path test over documents.
    ("R8 r36d-new q020 t1 call 8", "search",
     {"query": "reaper",
      "where": {"type": "document", "attrs.path": {"op": "contains", "value": "reaper"}},
      "limit": 50, "select": ["node_id", "attrs.path", "attrs.name"], "max_chars": 200},
     "path_filter", "reaper", {"type": "document"}),
    ("R9 path test over passages", "filter",
     {"where": {"type": "document_chunk",
                "attrs.path": {"op": "contains", "value": "reaper"}}},
     "path_test", "reaper", {"type": "document"}),
]


@pytest.mark.parametrize(
    "label, tool, arguments, reason, query, where", TRANSCRIPT_SHAPES,
    ids=[shape[0].split()[0] for shape in TRANSCRIPT_SHAPES],
)
def test_each_transcript_shape_names_the_grep_over_file_texts(
    label: str, tool: str, arguments: dict, reason: str, query: str, where: dict,
) -> None:
    found = _plan(tool, arguments)
    assert found is not None, label
    assert (found.reason, found.query) == (reason, query)
    assert found.arguments() == {"query": query, "method": "grep", "where": where}
    # The named call is itself a call the read tools take as written.
    assert rewrite_call("search", found.arguments()).arguments["where"] is not None


def test_a_grep_over_texts_is_not_a_read_of_names() -> None:
    """C1 (csw-okg-low a2 call 7) searched texts: no line."""
    assert _plan("search", {"query": "reaper", "method": "grep", "path_prefix": "apps/",
                            "limit": 50, "max_chars": 2500}) is None
    assert _plan("search", {"query": "reaperCount",
                            "where": {"type": "document_chunk"}, "limit": 30}) is None
    assert _plan("search", {"query": "reaper", "where": {"type": "document"}}) is None


@pytest.mark.parametrize("tool, arguments", [
    # Another attribute cannot be carried to a document as written.
    ("filter", {"where": {"type": "source_file", "attrs.language": "yaml",
                          "attrs.path": {"op": "contains", "value": "reaper"}}}),
    # A path test that is not `contains` lists a folder or one file.
    ("filter", {"where": {"type": "source_file",
                          "attrs.path": {"op": "prefix", "value": "apps/options/"}}}),
    ("filter", {"where": {"type": "source_file",
                          "attrs.path": {"op": "suffix", "value": "kustomization.yaml"}}}),
    # Two path tests, two repositories, a type that is not a file.
    ("filter", {"where": {"op": "and", "clauses": [
        {"op": "contains", "field": "attrs.path", "value": "reaper"},
        {"op": "contains", "field": "attrs.path", "value": "tier0"}]}}),
    ("filter", {"where": {"type": "source_file", "attrs.repo": {"op": "in", "values": ["a", "b"]},
                          "attrs.path": {"op": "contains", "value": "reaper"}}}),
    ("filter", {"where": {"type": "git_commit",
                          "attrs.path": {"op": "contains", "value": "reaper"}}}),
    # A condition under `or`.
    ("filter", {"where": {"op": "or", "clauses": [
        {"op": "contains", "field": "attrs.path", "value": "reaper"},
        {"op": "eq", "field": "attrs.repo", "value": "rucioflux"}]}}),
    # Grep cannot take the text as written.
    ("filter", {"where": {"type": "source_file",
                          "attrs.path": {"op": "contains", "value": 'say "hi"'}}}),
    # A query over files AND a path test; a query over other types.
    ("search", {"query": "reaper", "where": {
        "type": "source_file", "attrs.path": {"op": "contains", "value": "flux"}}}),
    ("search", {"query": "reaper", "where": {"type": "git_commit"}}),
    # A text query with a path test over documents, but two path tests, a
    # type that is not file text, no type, or a query grep refuses.
    ("search", {"query": "reaper", "where": {"op": "and", "clauses": [
        {"op": "eq", "field": "type", "value": "document"},
        {"op": "contains", "field": "attrs.path", "value": "reaper"},
        {"op": "contains", "field": "attrs.path", "value": "tier0"}]}}),
    ("search", {"query": "reaper", "where": {
        "type": "git_commit", "attrs.path": {"op": "contains", "value": "reaper"}}}),
    ("search", {"query": "reaper", "where": {
        "attrs.path": {"op": "contains", "value": "reaper"}}}),
    ("search", {"query": '"unclosed', "where": {
        "type": "document", "attrs.path": {"op": "contains", "value": "reaper"}}}),
])
def test_a_condition_the_text_search_cannot_carry_names_no_call(tool: str, arguments: dict) -> None:
    assert _plan(tool, arguments) is None


def test_the_repository_carries_from_an_id_prefix_a_source_id_and_path_prefix() -> None:
    by_id = names.plan({"op": "and", "clauses": [
        {"op": "prefix", "field": "node_id", "value": "file:flux:"},
        {"op": "contains", "field": "attrs.path", "value": "reaper"},
    ]}, method="filter")
    assert by_id is not None and by_id.repo == "flux"
    by_source = names.plan({"op": "and", "clauses": [
        {"op": "eq", "field": "type", "value": "source_file"},
        {"op": "eq", "field": "source", "value": "flux.git-files"},
    ]}, query="reaper", method="grep", path_prefix="apps/")
    assert by_source is not None and by_source.repo == "flux"
    assert by_source.arguments() == {
        "query": "reaper", "method": "grep",
        "where": {"type": "document", "id": {"op": "prefix", "value": "doc:flux:"}},
        "path_prefix": "apps/",
    }
    assert by_source.canonical_where() == {"op": "and", "clauses": [
        {"op": "eq", "field": "type", "value": "document"},
        {"op": "prefix", "field": "id", "value": "doc:flux:"},
    ]}
    # A prefix that is not a whole repository segment is not read as one.
    assert names.plan({"op": "and", "clauses": [
        {"op": "prefix", "field": "id", "value": "file:flux:apps/"},
        {"op": "contains", "field": "attrs.path", "value": "reaper"},
    ]}, method="filter") is None


R1_CALL = (
    'search(query="reaper", method="grep", '
    'where={"type":"document","id":{"op":"prefix","value":"doc:rucioflux:"}})'
)


def test_the_line_names_the_call_and_its_count() -> None:
    r1 = _plan("filter", TRANSCRIPT_SHAPES[0][2])
    assert r1 is not None
    assert names.render(names.hint_block(r1, total=18, basis="exact")) == (
        "! matched file names only (the path test reads names, not file text); "
        f'18 file texts in rucioflux contain "reaper": {R1_CALL}'
    )
    assert names.render(names.hint_block(r1, total=1, basis="exact")).endswith(
        f'1 file text in rucioflux contains "reaper": {R1_CALL}'
    )
    assert names.render(names.hint_block(r1, total=10000, basis="lower_bound")).endswith(
        f'at least 10,000 file texts in rucioflux contain "reaper": {R1_CALL}'
    )
    r2 = _plan("search", TRANSCRIPT_SHAPES[1][2])
    assert r2 is not None
    assert names.render(names.hint_block(r2, total=3, basis="exact")).startswith(
        "! matched file names only (a source_file holds a file's path, not its text); "
    )


def test_no_count_still_names_the_call_and_zero_names_none() -> None:
    """Failure cases: an uncounted call is never printed as 0, and a count of
    0 offers no call that would return nothing."""
    r1 = _plan("filter", TRANSCRIPT_SHAPES[0][2])
    assert r1 is not None
    for basis in ("timed_out", None):
        block = names.hint_block(r1, total=None, basis=basis)
        assert block["count"] is None and block["count_basis"] == "not_counted"
        assert names.render(block) == (
            "! matched file names only (the path test reads names, not file text); "
            f'file texts in rucioflux holding "reaper" were not counted in time: {R1_CALL}'
        )
    zero = names.hint_block(r1, total=0, basis="exact")
    assert "call" not in zero
    assert names.render(zero) == (
        "! matched file names only (the path test reads names, not file text); "
        'no file text in rucioflux contains "reaper"'
    )


def test_several_terms_say_every_term() -> None:
    r7 = _plan("search", TRANSCRIPT_SHAPES[6][2])
    assert r7 is not None and r7.terms == 4
    line = names.render(names.hint_block(r7, total=2, basis="exact"))
    assert line is not None
    assert '2 file texts hold every term of "tier1-reaper-rucio-daemons configmap chunk_size rses"' in line
    assert names.render(names.hint_block(r7, total=0, basis="exact")).endswith(
        'no file text holds every term of "tier1-reaper-rucio-daemons configmap chunk_size rses"'
    )


def test_a_malformed_block_prints_nothing() -> None:
    assert names.render(None) is None
    assert names.render({"reason": "other", "query": "x", "count": 1}) is None
    assert names.render({"reason": "path_test", "query": "x", "count": 3}) is None


def test_a_grep_over_file_records_says_it_searched_path_words() -> None:
    """The q020 reply said "searched: text, comments" over source_file
    records, whose text is their path."""
    payload = {"grep_searched": {
        "fields": ["text"], "comments": True, "name_only": True,
        "spellings_matched": ["reaper"], "spellings_tried": ["reaper"],
    }}
    assert _grep_searched_notice(payload, 9) == (
        "! searched: path words (a source_file holds no file contents; contents are "
        'document records); spellings matched: "reaper"'
    )
    assert _grep_searched_notice(payload, 0) == (
        "! no match; searched: path words (a source_file holds no file contents; "
        'contents are document records); spellings tried: "reaper"'
    )
    # Failure case: any other grep keeps its wording.
    other = {"grep_searched": {**payload["grep_searched"], "name_only": False}}
    assert _grep_searched_notice(other, 9) == '! searched: text, comments; spellings matched: "reaper"'


def test_the_line_for_a_text_search_with_a_path_test_says_names_kept_the_files() -> None:
    found = _plan("search", next(s[2] for s in TRANSCRIPT_SHAPES if s[0].startswith("R8")))
    assert found is not None and found.reason == "path_filter"
    assert names.render(names.hint_block(found, total=14, basis="exact")) == (
        "! matched file names only (the path test kept only files with matching "
        'names); 14 file texts contain "reaper": '
        'search(query="reaper", method="grep", where={"type":"document"})'
    )
