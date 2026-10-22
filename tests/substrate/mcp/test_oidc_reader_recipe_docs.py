"""The security runbook still shows the read-only OIDC recipe the DB test runs.

``test_oidc_reader_baseline_policy.py`` proves the recipe works against the
real rollout baseline, but it runs in a PostgreSQL lane whose scope closure
does not include the docs tree, so it holds the recipe as literals in
``_oidc_reader_recipe.py``. This file needs no database: it parses the
runbook's "CERN recipe for a read-only server" section and asserts it
equals those literals, so a runbook edit that changes the recipe fails here.
"""
from __future__ import annotations

import re
import shlex
from pathlib import Path
from typing import Any

import yaml

from tests.substrate.mcp._oidc_reader_recipe import recipe as documented_recipe

RUNBOOK = Path(__file__).resolve().parents[3] / "docs/runbooks/security/nomos-iam.md"
RECIPE_HEADING = "### CERN recipe for a read-only server"


def _dedent(body: str) -> str:
    lines = body.splitlines()
    indent = min(
        (len(line) - len(line.lstrip()) for line in lines if line.strip()), default=0,
    )
    return "\n".join(line[indent:] for line in lines) + "\n"


def _without_target(argv: list[str]) -> list[str]:
    """Drop --dsn/--deployment/--json; the DB test supplies its own."""
    out: list[str] = []
    skip = False
    for arg in argv:
        if skip:
            skip = False
            continue
        if arg in {"--dsn", "--deployment"}:
            skip = True
            continue
        if arg == "--json":
            continue
        out.append(arg)
    return out


def parse_runbook_recipe(text: str) -> dict[str, Any]:
    """The runbook section's nomos block and its provider/binding/grant argv."""
    start = text.index(RECIPE_HEADING)
    end_match = re.compile(r"^#{2,3} ", re.MULTILINE).search(
        text, start + len(RECIPE_HEADING),
    )
    section = text[start:end_match.start() if end_match else len(text)]
    blocks = re.findall(r"```(yaml|bash)\n(.*?)```", section, re.DOTALL)
    yaml_blocks = [yaml.safe_load(_dedent(body)) for lang, body in blocks if lang == "yaml"]
    bash = "\n".join(_dedent(body) for lang, body in blocks if lang == "bash")
    nomos = next(block["nomos"] for block in yaml_blocks if "nomos" in (block or {}))
    commands: dict[str, list[str]] = {}
    for line in bash.replace("\\\n", " ").splitlines():
        argv = shlex.split(line)
        if argv[:4] == ["okg", "nomos", "binding", "add"]:
            commands.setdefault("binding", _without_target(argv[4:]))
        elif argv[:4] == ["okg", "nomos", "grant", "add"]:
            commands.setdefault("grant", _without_target(argv[4:]))
        elif argv[:4] == ["okg", "nomos", "provider", "add"]:
            commands.setdefault("provider", _without_target(argv[4:]))
    assert set(commands) == {"provider", "binding", "grant"}, commands
    binding = commands["binding"]
    return {
        "nomos": nomos,
        "provider": commands["provider"],
        "binding": binding,
        "grant": commands["grant"],
        "role": binding[binding.index("--claim-value") + 1],
        "subject": binding[binding.index("--subject") + 1],
        "grant_id": commands["grant"][commands["grant"].index("--grant-id") + 1],
    }


def test_runbook_recipe_equals_the_literals_the_db_test_runs() -> None:
    parsed = parse_runbook_recipe(RUNBOOK.read_text(encoding="utf-8"))
    assert parsed == documented_recipe()


def test_recipe_binds_the_baseline_subject_and_the_read_grant() -> None:
    """The two values the rollout baseline depends on, named outright."""
    parsed = parse_runbook_recipe(RUNBOOK.read_text(encoding="utf-8"))
    assert parsed["subject"] == "subject:mcp_client"
    assert parsed["grant_id"] == "grant:my-graph:mcp-client:read"
    assert parsed["nomos"]["oidc_auth"]["subject_claim"] == "cern_upn"


def test_a_changed_runbook_subject_is_caught() -> None:
    """A runbook that binds another subject no longer equals the literals."""
    text = RUNBOOK.read_text(encoding="utf-8")
    start = text.index(RECIPE_HEADING)
    section = text[start:]
    edited = text[:start] + section.replace(
        "--subject subject:mcp_client", "--subject subject:example-kb:reader", 1,
    )
    assert edited != text
    assert parse_runbook_recipe(edited) != documented_recipe()
