"""Parametrized argparse + dispatch smoke test for the substrate CLI.

For EVERY verb in the substrate's argparse tree, asserts:

  1. `okg <verb> --help` parses and exits 0 (argparse short-circuits
     required args, so this exercises the parser tree without
     invoking the handler).
  2. The top-level verb resolves to a registered argparse subparser.

This is the "wired-up" bare-bones check that runs universally —
parametrize discovers verbs directly from `_build_parser()`, not
from the manifest [CLI_SMOKE_INDEX.yaml](CLI_SMOKE_INDEX.yaml),
so a verb gets this smoke even when its manifest entry routes at
a behavioral test file elsewhere.

Per-verb behavioral tests (no-DSN failure, happy path, …) live
in the file named by the manifest's `test_file:`.

Per `audit-test-and-document-cli-surface` Phase 2.
"""
from __future__ import annotations

import argparse

import pytest

from okg.substrate.cli.main import _build_parser, main


def _walk_argparse_verbs() -> list[str]:
    """Walk the parser tree two levels deep; return verb names."""
    parser = _build_parser()
    out: list[str] = []
    for action in parser._actions:
        if not isinstance(action, argparse._SubParsersAction):
            continue
        for name, sub_parser in action.choices.items():
            out.append(name)
            for inner in sub_parser._actions:
                if not isinstance(inner, argparse._SubParsersAction):
                    continue
                for sub_name in inner.choices:
                    out.append(f"{name} {sub_name}")
    return out


_SMOKE_VERBS = _walk_argparse_verbs()


@pytest.mark.parametrize("verb", _SMOKE_VERBS, ids=lambda v: v.replace(" ", "_"))
def test_verb_argparse_help_succeeds(verb: str) -> None:
    """`okg <verb> --help` parses and exits 0.

    --help short-circuits required args, so this exercises the full
    argparse path (including any nested subparsers) without needing
    fixtures for the handler.
    """
    argv = verb.split() + ["--help"]
    with pytest.raises(SystemExit) as exc:
        main(argv)
    assert exc.value.code == 0, (
        f"`okg {verb} --help` returned exit code {exc.value.code} "
        f"(expected 0). Argparse rejected the verb or its subparser."
    )


@pytest.mark.parametrize("verb", _SMOKE_VERBS, ids=lambda v: v.replace(" ", "_"))
def test_verb_registered_in_subparsers(verb: str) -> None:
    """The top-level verb appears in the argparse subparsers.

    Also: for sub-verbs (e.g. ``db status``), the second token is a
    valid choice under the top-level verb's nested subparser.
    """
    parser = _build_parser()
    top_sub = next(
        (
            a for a in parser._actions
            if isinstance(a, argparse._SubParsersAction)
        ),
        None,
    )
    assert top_sub is not None, "substrate CLI has no top-level subparsers"

    top, _, sub = verb.partition(" ")
    assert top in top_sub.choices, (
        f"top-level verb {top!r} not in argparse subparsers "
        f"(have: {sorted(top_sub.choices)})"
    )
    if sub:
        nested = next(
            (
                a for a in top_sub.choices[top]._actions
                if isinstance(a, argparse._SubParsersAction)
            ),
            None,
        )
        assert nested is not None, (
            f"verb {top!r} has no nested subparsers but manifest "
            f"lists sub-verb {sub!r}"
        )
        assert sub in nested.choices, (
            f"sub-verb {sub!r} not in {top!r}'s nested subparsers "
            f"(have: {sorted(nested.choices)})"
        )


def test_argparse_tree_non_empty() -> None:
    """Sanity: argparse exposes at least one verb.

    Guards against accidental edits that gut `_build_parser()`
    (which would silently collect 0 parametrized tests).
    """
    assert _SMOKE_VERBS, (
        "argparse tree exposes no verbs — `_build_parser()` may "
        "have regressed"
    )
