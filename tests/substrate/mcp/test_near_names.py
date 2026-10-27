"""Issue 4053, without a database: the stem a near-name read uses, the
ranking of near names, the search line that says a name names no record,
and the inspect not-found wording.

PACT read-absent-names-4053.
"""
from __future__ import annotations

import pytest

from okg.substrate.mcp.near_names import NEAR_NAMES_SHOWN, name_stem, rank_near
from okg.substrate.mcp.result_text import _exact_name_absent_line
from okg.substrate.mcp.tools import _sibling_stem, node_not_visible


@pytest.mark.parametrize(("name", "stem"), [
    ("ReleaseNotesAgentJune2013", "ReleaseNotesAgent"),
    ("WorkshopAtLISBON2024", "WorkshopAt"),
    ("ABCDEFAtLISBON2024", "ABCDEFAt"),
    ("LisbonWORKSHOP2024", "LisbonWORKSHOP"),
    ("pkg_release_14_0_9", "pkg_release_14"),
    ("UrlSafeSerializr", "UrlSafe"),
    ("ABC-12-345", "ABC-12"),
    ("Zebra", None),
    ("x", None),
    ("", None),
])
def test_the_stem_drops_the_last_runs_and_refuses_a_short_one(name, stem) -> None:
    assert name_stem(name) == stem


def test_near_names_rank_closest_first_and_never_list_the_name_itself() -> None:
    found = [
        ("page:ReleaseNotesAgentApril2013", "ReleaseNotesAgentApril2013"),
        ("page:ReleaseNotesAgentMay2013", "ReleaseNotesAgentMay2013"),
        ("page:ReleaseNotesAgentJune2013", "ReleaseNotesAgentJune2013"),
        ("page:ReleaseNotesAgentMay2013", "ReleaseNotesAgentMay2013"),
        ("page:ReleaseNotesAgent", "ReleaseNotesAgent"),
    ]
    ranked = rank_near("ReleaseNotesAgentJune2013", found)
    assert ranked[0] == "page:ReleaseNotesAgentMay2013"
    assert "page:ReleaseNotesAgentJune2013" not in ranked
    assert len(ranked) == len(set(ranked)) == 3
    many = [(f"id:{i}", f"Name{i:02d}") for i in range(20)]
    assert len(rank_near("Name99", many)) == NEAR_NAMES_SHOWN


def test_near_names_ignore_case_when_ranking() -> None:
    ranked = rank_near("workshopatlisbon2024", [
        ("page:WorkshopAtLisbon2023", "WorkshopAtLisbon2023"),
        ("page:WorkshopLisbon2024Stats", "WorkshopLisbon2024Stats"),
    ])
    assert ranked[0] == "page:WorkshopAtLisbon2023"


def _absent(**extra) -> dict:
    return {
        "name": "ReportJune2013",
        "checked": ["name", "qualified_name", "title"],
        "within_filter": False,
        **extra,
    }


def test_the_absence_line_says_no_record_is_named_and_lists_near_ids() -> None:
    line = _exact_name_absent_line({
        "exact_name_absent": _absent(near_ids=["page:ReportMay2013", "page:ReportApril2013"]),
        "results": [],
    })
    assert line == (
        '! no record has the name or id "ReportJune2013": names, titles, labels, '
        "keys and ids checked exactly; ids and text also in any case or with up "
        "to three separators between its words; other attributes not checked; ids "
        "with a near name: page:ReportMay2013, page:ReportApril2013"
    )


def test_the_absence_line_names_the_filter_and_says_nothing_of_the_rows() -> None:
    with_rows = _exact_name_absent_line({
        "exact_name_absent": _absent(), "results": [{"node_id": "page:Home"}],
    })
    assert "rows below" not in with_rows
    assert with_rows.endswith("other attributes not checked")
    filtered = _exact_name_absent_line({"exact_name_absent": _absent(within_filter=True)})
    assert filtered.startswith(
        '! no record this search\'s filters keep has the name or id "ReportJune2013"',
    )


def test_no_absence_field_gives_no_line() -> None:
    assert _exact_name_absent_line({}) is None
    assert _exact_name_absent_line({"exact_name_absent": {"name": 3}}) is None


def test_the_not_found_message_is_plain_where_every_row_is_readable() -> None:
    plain = node_not_visible("page:X2024", "gen-1", every_row_readable=True)
    assert plain.message == "no record has id 'page:X2024' at generation gen-1 (exists: false at this pin)"
    assert plain.data["exists"] is False
    hidden = node_not_visible("page:X2024", "gen-1", every_row_readable=False)
    assert hidden.message.startswith("no record you can read has id 'page:X2024'")
    unknown = node_not_visible("page:X2024", "gen-1")
    assert unknown.message.startswith("node 'page:X2024' is not visible at generation gen-1")


@pytest.mark.parametrize(("cleaned", "expected"), [
    ("page:WorkshopAtLisbon2024", ("page:", "WorkshopAt")),
    ("wiki:Team:ABCDEFAtLISBON2024", ("wiki:Team:", "ABCDEFAt")),
    ("file:repo:src/x.py", None),
    ("commit:repo:abc123def456", None),
    ("page:Zebra", None),
    ("NoHead2024", None),
])
def test_the_sibling_stem_needs_a_head_and_a_named_last_part(cleaned, expected) -> None:
    assert _sibling_stem(cleaned) == expected


def test_a_row_holds_a_token_in_any_spelling() -> None:
    from okg.substrate.mcp.search import _holds_token

    row = {"node_id": "page:Home", "attrs": {"text": "see [[Report-June-2013]] here",
                                            "tags": ["x", "Alpha_7"]}}
    assert _holds_token(row, "ReportJune2013")
    assert _holds_token(row, "report_june_2013")
    assert _holds_token(row, "Alpha-7")
    assert not _holds_token(row, "ReportJuly2013")
    assert _holds_token({"node_id": "file:repo:src/x/near_names.py"}, "near_names")
    assert _holds_token({"node_id": "page:x", "attrs": {"title": "Team charter 2019"}},
                        "TeamCharter2019")
    assert not _holds_token({"node_id": "page:x", "attrs": {"n": 3}}, "TeamCharter2019")


@pytest.mark.parametrize(("token", "doubtful"), [
    ("search.py", True), ("src/x", True), ("Foo.bar", True),
    ("0001302a4fbe", True), ("deadbeef12", True), ("0abc1234ef", True),
    ("3fa85f64-5717-4562-b3fc-2c963f66afa6", True), ("3FA85F64-5717-4562", True),
    ("ReportJune2013", False), ("OPS-1234", False), ("pkg_release_14_0_9", False),
    ("abcdef", False),
])
def test_paths_dotted_names_and_hashes_are_doubtful_tokens(token, doubtful) -> None:
    from okg.substrate.mcp.search import _PATH_OR_DOTTED, _hash_shaped

    assert bool(_PATH_OR_DOTTED.search(token) or _hash_shaped(token)) is doubtful


@pytest.mark.parametrize(("name", "has_pattern"), [
    ("WorkshopAtLisbon2024", True), ("ReleaseNotesAgentJune2013", True),
    ("pkg_release_14_0_9", True), ("ABCPlan2024", True),
    ("teamcharter2036", False), ("TEAMCHARTER2037", False), ("Teamcharter2038", False),
    ("releasenotesjune2041", False), ("Ab_C1", False),
    ("teamplan2060", False), ("OPSPLAN2062", False), ("roadmap2025", False),
])
def test_a_single_case_run_of_nine_letters_gives_no_spelling_pattern(name, has_pattern) -> None:
    from okg.substrate.mcp.search import _spelling_pattern

    assert (_spelling_pattern(name) is not None) is has_pattern
