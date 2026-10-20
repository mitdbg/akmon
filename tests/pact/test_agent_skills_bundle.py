"""The PACT/OKG agent skills bundle is exactly the usage-grounded essential set.

Per `refine-agent-skills`: the bundle under src/okg/substrate/pact/skills/ must
be exactly the essential skills covering the full agent<->OKG lifecycle,
and every skill must be a valid Claude Code skill (`<name>/SKILL.md` with
non-empty `name` matching its dir + `description`) whose body invents no tool
and never shows the non-existent `okg query` CLI invocation. The exact-set guard
fails if a speculative skill is added (or an essential one removed) without
updating the contract.
"""
from __future__ import annotations

from pathlib import Path
import re

import pytest
import yaml

import okg.substrate.pact as _pact_pkg

_BUNDLE = Path(_pact_pkg.__file__).parent / "skills"

# The usage-grounded essential set: the operating workflow (agent-workflow),
# the PACT loop (pact), the per-issue dev process (dev-process), graph read
# (okg-query), and the OKG lifecycle — provision, populate (add-source),
# write-back (record), troubleshoot, extend (ontology-design).
_ESSENTIAL = {
    "agent-workflow",
    "pact",
    "dev-process",
    "okg-query",
    "okg-provision",
    "okg-add-source",
    "okg-record",
    "okg-troubleshoot",
    "okg-ontology-design",
}


def _skill_dirs() -> list[Path]:
    if not _BUNDLE.is_dir():
        return []
    return sorted(
        p for p in _BUNDLE.iterdir()
        if p.is_dir() and (p / "SKILL.md").is_file()
    )


def _frontmatter(text: str) -> dict:
    assert text.startswith("---\n"), "SKILL.md must open with a --- frontmatter block"
    end = text.index("\n---", 4)
    fm = yaml.safe_load(text[4:end])
    assert isinstance(fm, dict), "frontmatter must be a YAML mapping"
    return fm


def test_bundle_is_exactly_the_essential_set() -> None:
    names = {p.name for p in _skill_dirs()}
    assert names == _ESSENTIAL, (
        f"skills bundle drift — extra: {sorted(names - _ESSENTIAL)}; "
        f"missing: {sorted(_ESSENTIAL - names)}. Update the refine-agent-skills "
        f"contract before changing the essential set."
    )


def test_agent_workflow_skill_is_the_workflow_contract_entry_point() -> None:
    text = (_BUNDLE / "agent-workflow" / "SKILL.md").read_text(encoding="utf-8")
    for phrase in [
        "Lead with the result",
        "okg pact view <pact-id> --format=bootstrap --json",
        # v5 replaced the task-completion gate with the one check.
        "okg pact evidence attach <pact-id>",
        "okg pact check <pact-id> --strict --json",
        "longest useful bounded wait",
        "one human report",
        "exact commit",
        "gh run watch <run-id>",
        # The skill ships in the package, so it names the repository's own
        # rules instead of this repository's runbooks and CI scripts (#2823);
        # CLAUDE.md carries the exact paths here.
        "the repository's receipt validator",
        "the repository's own reporting rules",
        "the repository's own coordination rules",
        "use the `okg-query` skill",
    ]:
        assert phrase in text


def test_okg_add_source_skill_enforces_incremental_admission() -> None:
    text = (_BUNDLE / "okg-add-source" / "SKILL.md").read_text(encoding="utf-8")
    for phrase in [
        "change_probe_kind",
        "self.change_probe",
        "git_scope",
        "git_refs",
        "union",
        "mutable_api",
        "content_hash",
        "sync.poll.probe: change_probe",
        "always_changed",
        "producer-owned `stat_tree`",
        "`content_hash`, `ttl`, or `remote_etag` poll selectors",
        "mtime-preserved edit",
        "probe-error",
        "uv run okg deployment lint",
        "probe_missing",
        "probe_runtime_missing",
        "probe_unsound",
        "poll_probe_unsound",
    ]:
        assert phrase in text


def test_source_authoring_templates_use_closed_poll_probe_contract() -> None:
    substrate = Path(_pact_pkg.__file__).parents[1]
    roots = [
        substrate / "library" / "templates",
        substrate / "library" / "profiles",
    ]
    yaml_files = sorted(
        path
        for root in roots
        for path in root.rglob("*.yaml")
    )
    assert yaml_files
    legacy_poll_probe = re.compile(
        r"^\s+probe:\s+(?:stat_tree|content_hash|ttl|remote_etag)\s*$",
        re.MULTILINE,
    )
    offenders = [
        path
        for path in yaml_files
        if legacy_poll_probe.search(path.read_text(encoding="utf-8"))
    ]
    assert offenders == []


@pytest.mark.parametrize("skill_dir", _skill_dirs(), ids=lambda p: p.name)
def test_bundle_skills_valid_and_no_invented_tools(skill_dir: Path) -> None:
    text = (skill_dir / "SKILL.md").read_text(encoding="utf-8")
    fm = _frontmatter(text)
    assert fm.get("name"), f"{skill_dir.name}: missing frontmatter name"
    assert fm.get("description"), f"{skill_dir.name}: missing frontmatter description"
    assert fm["name"] == skill_dir.name, (
        f"{skill_dir.name}: frontmatter name {fm['name']!r} != directory name"
    )
    # `okg query "..."` is NOT a real CLI verb (the query operator is MCP-only;
    # shell SQL is psql). A skill may explain that, but must never show the bad
    # invocation form — the same drift guard the canonical-dev-okg runbook uses.
    assert 'okg query "' not in text, (
        f"{skill_dir.name}: shows the non-existent `okg query` CLI invocation"
    )
