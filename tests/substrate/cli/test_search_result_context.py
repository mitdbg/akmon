from __future__ import annotations

from okg.substrate.cli._read_side import _print_result


def test_human_search_renders_method_and_terminal_safe_excerpt(
    capsys,
) -> None:
    _print_result(
        {
            "results": [
                {
                    "node_id": "alias:row",
                    "canonical_node_id": "concept:jes",
                    "subtype": "concept",
                    "method": "lexical",
                    "score": -2.5,
                    "excerpt": (
                        "jet energy\x1b[31m scale\ncontext "
                        "\u009b32m c1 \u202ereversed\u2028end"
                    ),
                }
            ]
        },
        as_json=False,
        select=["node_id", "canonical_node_id", "subtype", "method", "score", "excerpt"],
    )
    output = capsys.readouterr().out
    # read-passage-rows-3666: a text search row is its head line and its
    # passage; the selected bookkeeping fields (canonical id, method, score)
    # are in the JSON form only.
    assert "[1] alias:row (concept)" in output
    assert "concept:jes" not in output
    assert "method=lexical" not in output and "lexical" not in output
    assert "-2.5" not in output
    assert "jet energy [31m scale" in output
    assert "\x1b" not in output
    assert "\u009b" not in output
    assert "\u202e" not in output
    assert "\u2028" not in output


#: A free-text `okg search` envelope as the search function returns it, with
#: the fields the transcripts of issue #3666 showed dumped as Python reprs.
_FREE_TEXT_PAGE = {
    "query": "CMSTRANSF-1172",
    "method": "lexical",
    "generation_id": "g:1",
    "pinned_generation_id": "g:1",
    "limit": 2,
    "offset": 0,
    "next_offset": 2,
    "results": [
        {"node_id": "jira:A-1", "subtype": "jira_issue", "method": "lexical",
         "score": -21.2, "excerpt": "first"},
        {"node_id": "jira:A-2", "subtype": "jira_issue", "method": "lexical",
         "score": -20.2, "excerpt": "second"},
    ],
    "total_count": None,
    "truncated": True,
    "rank_capped": True,
    "diversity_applied": False,
    "max_per_parent": None,
    "total_count_basis": "unknown",
    "ranked_cap": {"rows": 200},
    "result_unit": "record",
    "completeness": "bounded",
    "coverage": {"retrieval": {"status": "bounded", "basis": "bm25_index",
                               "bounds": ["candidate_cap"]}},
}


def test_free_text_search_prints_the_status_line_not_a_dict_dump(capsys) -> None:
    """Issue #3666: free-text `okg search` printed every envelope key as a
    Python repr (`total_count: None`, `{'rows': 200}`), unlike every other
    read command. It prints the text form they print."""
    _print_result(_FREE_TEXT_PAGE, as_json=False)
    out = capsys.readouterr().out
    lines = out.splitlines()
    # read-passage-rows-3666: the short status line (query, rows, next).
    assert lines[0].startswith('search ok · "CMSTRANSF-1172" · rows 1-2 of count unknown')
    assert "next: offset=2" in lines[0]
    assert "[1] jira:A-1 (jira_issue)" in out
    assert "[2] jira:A-2 (jira_issue)" in out
    # Text rows carry no score or method (JSON only).
    assert "-21.2" not in out and "-20.2" not in out and "method=" not in out
    for repr_only in ("None", "True", "False", "{'", "results (2)", "query: CMSTRANSF"):
        assert repr_only not in out, repr_only
