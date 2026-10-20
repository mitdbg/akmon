"""The human CLI must never scope an operator without showing it (issue #797).

No database: these drive the renderer directly, so they run in the `fast` lane.

The assertions are deliberately against the HEAD of the output (the status
line and the note under it), not against the body. `--- other fields` lists
every envelope key the grammar has no place for, so a test that merely looked
for the string "analyst" somewhere in the output would go green the moment the
envelope carried the field, with the operator still reading a head that says
nothing about the scope.
"""
from __future__ import annotations

from okg.substrate.cli._read_side import _print_result, _profile_note


def _head(capsys, result: dict) -> list[str]:
    _print_result(result, as_json=False)
    return capsys.readouterr().out.splitlines()[:2]


def test_human_search_head_names_the_applied_profile(capsys) -> None:
    """The DEFAULT case: the caller passed no --profile and was scoped anyway.

    This is the case the note exists for. A note built from `args` rather
    than from the result would say nothing here.
    """
    result = {
        "search_profile": "analyst",
        "scoped_subtypes": ["analysis_chunk", "chunk", "entity"],
        "results": [
            {
                "node_id": "chunk:sr",
                "subtype": "chunk",
                "method": "lexical",
                "score": -3.0,
                "excerpt": "the signal region is defined as ...",
            }
        ],
    }
    status, note = _head(capsys, result)
    # read-passage-rows-3666: the status line no longer repeats the
    # profile's type list (the default scope is not said); the note under
    # it names the profile that chose them and how many types it holds.
    assert status.startswith("search ok")
    assert "type in [" not in status
    assert not any(name in status for name in ("analysis_chunk", "chunk", "entity"))
    assert note == "! search profile analyst applied: 3 subtypes"


def test_the_head_says_unscoped_when_no_profile_applied(capsys) -> None:
    """Silence is ambiguous: the operator cannot tell "no profile was
    applied" from "this build has no profiles". The note under the status
    line says unscoped."""
    result = {"search_profile": None, "scoped_subtypes": None, "results": []}
    status, note = _head(capsys, result)
    assert status.startswith("search ok")
    assert note == "! search profile: none (unscoped)"


def test_the_reserved_all_profile_is_said_as_unscoped(capsys) -> None:
    status, note = _head(capsys, {"search_profile": "all", "scoped_subtypes": None, "results": []})
    assert status.startswith("search ok")
    assert note == "! search profile all applied: unscoped"


def test_results_that_carry_no_profile_field_get_no_note() -> None:
    """Envelopes from other verbs are untouched."""
    assert _profile_note({"results": []}) is None
    assert _profile_note("not a dict") is None


def test_search_parser_exposes_profile() -> None:
    import argparse

    from okg.substrate.cli._read_side import register_subparsers

    parser = argparse.ArgumentParser()
    register_subparsers(parser.add_subparsers(dest="cmd"))
    args = parser.parse_args([
        "search", "--deployment", "d", "--query", "q", "--scope", "analyst",
    ])
    assert args.profile == "analyst"

    default = parser.parse_args(
        ["search", "--deployment", "d", "--query", "q"]
    )
    assert default.profile is None
