"""The `okg code-repos sync` verb is not on this branch.

code-repos-auto-acquire supersedes the standalone sync verb (PR #483, never
merged): ingest auto-acquires managed checkouts, so there is no `code-repos`
command. Guards against the verb being (re)introduced.
"""
from __future__ import annotations

import argparse

import pytest

from okg.substrate.cli.main import _build_parser, main


def _top_level_verbs() -> set[str]:
    parser = _build_parser()
    verbs: set[str] = set()
    for action in parser._actions:
        if isinstance(action, argparse._SubParsersAction):
            verbs.update(action.choices)
    return verbs


def test_code_repos_is_not_a_registered_verb() -> None:
    assert "code-repos" not in _top_level_verbs()


def test_code_repos_sync_is_unknown_command() -> None:
    # argparse rejects an unregistered subcommand with a non-zero exit.
    with pytest.raises(SystemExit) as exc_info:
        main(["code-repos", "sync", "--deployment", "cms"])
    assert exc_info.value.code != 0
