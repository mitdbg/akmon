"""Reference entries a reader relies on agree with the code.

public-release-docs, task reference-accurate: the `okg mcp-serve` option
table in docs/reference/cli.md lists exactly the parser's visible options,
and the `inspect` kinds table in docs/reference/mcp.md lists exactly the
kinds the server serves, grouped as the server groups them.
"""

from __future__ import annotations

import argparse
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
CLI = ROOT / "docs" / "reference" / "cli.md"
MCP = ROOT / "docs" / "reference" / "mcp.md"
FLAG = re.compile(r"`(--[a-z][a-z-]*)")


DEFAULT = re.compile(r"\(default `?([^`,;)]+)`?")


def _section(text: str, heading: str) -> str:
    start = text.index(heading)
    after = re.search(r"\n#{1,3} ", text[start + len(heading):])
    return text[start:] if after is None else text[start:start + len(heading) + after.start()]


def _mcp_serve_actions() -> dict[str, argparse.Action]:
    from okg.substrate.cli.main import _build_parser

    parser = _build_parser()
    sub = next(a for a in parser._actions if isinstance(a, argparse._SubParsersAction))
    return {
        flag: action
        for action in sub.choices["mcp-serve"]._actions
        if action.help != argparse.SUPPRESS
        for flag in action.option_strings
        if flag.startswith("--") and flag != "--help"
    }


def _visible_mcp_serve_flags() -> set[str]:
    return set(_mcp_serve_actions())


def _option_rows(text: str) -> dict[str, str]:
    section = _section(text, "### `okg mcp-serve`")
    rows = [line for line in section.splitlines() if line.startswith("| `--")]
    return {FLAG.match(line[2:]).group(1): line for line in rows}


def _documented_mcp_serve_flags(text: str) -> set[str]:
    return set(_option_rows(text))


def test_mcp_serve_option_table_matches_the_parser() -> None:
    assert _documented_mcp_serve_flags(CLI.read_text(encoding="utf-8")) == (
        _visible_mcp_serve_flags()
    )


def test_a_missing_option_row_is_caught() -> None:
    text = CLI.read_text(encoding="utf-8")
    dropped = "\n".join(
        line for line in text.splitlines() if not line.startswith("| `--oidc-auth`")
    )
    assert _documented_mcp_serve_flags(dropped) != _visible_mcp_serve_flags()


def test_documented_defaults_match_the_parser() -> None:
    actions = _mcp_serve_actions()
    documented = {
        flag: match.group(1)
        for flag, row in _option_rows(CLI.read_text(encoding="utf-8")).items()
        if (match := DEFAULT.search(row))
    }
    assert documented, "the option table states no defaults"
    assert documented == {flag: str(actions[flag].default) for flag in documented}


def _documented_inspect_kinds(text: str) -> dict[str, set[str]]:
    table = text.split("| Kinds | Served when |", 1)[1].split("\n\n", 1)[0]
    rows: dict[str, set[str]] = {}
    for line in table.splitlines()[2:]:
        kinds, when = line.strip("|").split("|", 1)
        rows[when.strip()] = set(re.findall(r"`([a-z_]+)`", kinds))
    return rows


def test_inspect_kinds_table_matches_the_served_kinds() -> None:
    from okg.substrate.mcp import effective_surface as surface

    documented = list(_documented_inspect_kinds(MCP.read_text(encoding="utf-8")).values())
    assert documented == [
        set(surface.READ_INSPECT_KINDS),
        set(surface.WRITE_ADMIN_INSPECT_KINDS),
        set(surface.RECORD_INSPECT_KINDS),
        set(surface.CEILING_INSPECT_KINDS),
        set(surface.IMAGE_INSPECT_KINDS),
    ]
