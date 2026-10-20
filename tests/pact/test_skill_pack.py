"""ship-harness-integration-pack: the one in-package skills bundle
is agentskills.io-conformant and its CLI references don't drift."""
from __future__ import annotations

import re
import shlex
import subprocess
import sys

import yaml

from okg.substrate.pact.agent_install import bundled_skills


def _frontmatter(text: str) -> dict:
    match = re.match(r"\A---\n(.*?)\n---\n", text, re.DOTALL)
    assert match, "SKILL.md must open with YAML frontmatter"
    parsed = yaml.safe_load(match.group(1))
    assert isinstance(parsed, dict)
    return parsed


def _okg_verbs_from_block(block: str) -> list[str]:
    verbs: list[str] = []
    for raw_line in block.splitlines():
        try:
            parts = shlex.split(raw_line.strip())
        except ValueError:
            continue
        while parts and parts[0].startswith("OKG_") and "=" in parts[0]:
            parts = parts[1:]
        if parts[:2] == ["uv", "run"]:
            parts = parts[2:]
        if len(parts) >= 2 and parts[0] == "okg":
            verbs.append(parts[1])
    return verbs


def test_agentskills_frontmatter_contract() -> None:
    """agentskills.io requirements: name + description frontmatter,
    name agrees with the directory, and the always-loaded metadata
    stays inside the ~100-token progressive-disclosure budget."""
    skills = bundled_skills()
    assert skills, "bundle is empty"
    for skill in skills:
        text = (skill / "SKILL.md").read_text(encoding="utf-8")
        front = _frontmatter(text)
        assert front.get("name") == skill.name, skill.name
        description = front.get("description") or ""
        assert len(description) >= 20, f"{skill.name}: thin description"
        # ~100 tokens ≈ 500 chars for the always-loaded metadata.
        assert len(front.get("name", "")) + len(description) <= 600, (
            f"{skill.name}: metadata exceeds progressive-disclosure budget"
        )


def test_skill_cli_references_resolve() -> None:
    """Drift catcher: every `okg <verb>` invocation a skill teaches
    must be a real top-level CLI verb — a renamed command fails the
    pack in CI instead of shipping a broken skill."""
    # Every registered top-level verb, read from the parser rather than
    # from `okg --help`: default help leaves out internal verbs such as
    # `pact` (issue 3109 slice 0), which still run and which skills teach.
    listing = subprocess.run(
        [sys.executable, "-c",
         "import argparse\n"
         "from okg.substrate.cli.main import _build_parser\n"
         "p = _build_parser()\n"
         "sub = next(a for a in p._actions\n"
         "           if isinstance(a, argparse._SubParsersAction))\n"
         "print(','.join(sub.choices))"],
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
    ).stdout.strip()
    assert re.fullmatch(r"[a-z0-9_,\-]+", listing), (
        f"could not list CLI verbs from the parser: {listing[:200]}"
    )
    known_verbs = set(listing.split(","))

    # Only fenced code blocks count: that's where skills teach
    # commands for execution. Prose mentions (trigger phrases,
    # "there is no `okg query` verb" warnings) are not invocations.
    fence = re.compile(r"```[a-z]*\n(.*?)```", re.DOTALL)
    failures: list[str] = []
    for skill in bundled_skills():
        text = (skill / "SKILL.md").read_text(encoding="utf-8")
        for block in fence.findall(text):
            for verb in _okg_verbs_from_block(block):
                if verb not in known_verbs:
                    failures.append(f"{skill.name}: okg {verb}")
    assert not failures, (
        "skills teach unknown okg CLI verbs (drift): "
        f"{sorted(set(failures))}; known: {sorted(known_verbs)}"
    )
