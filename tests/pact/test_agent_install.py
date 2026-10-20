"""task.pai.install-command: `okg pact install` places the bundle + a
marker-delimited instruction section per agent target, idempotently, without
disturbing unrelated content.
"""
from __future__ import annotations

import json
import shutil
from pathlib import Path

import pytest

from okg.substrate.cli.main import main
from okg.substrate.pact.agent_install import (
    _MARKER_BEGIN,
    agent_skill_freshness,
    bundled_skills,
    install_agent_setup,
    is_installed,
)


def test_claude_code_places_skills_and_section(tmp_path: Path) -> None:
    res = install_agent_setup("claude-code", tmp_path)
    bundle = {p.name for p in bundled_skills()}
    assert bundle  # sanity: the bundle is non-empty
    for name in bundle:
        assert (tmp_path / ".claude" / "skills" / name / "SKILL.md").is_file()
    assert res.skill_count == len(bundle)
    claude = (tmp_path / "CLAUDE.md").read_text()
    assert _MARKER_BEGIN in claude
    assert "okg pact" in claude
    assert "docs/runbooks/agents/ui.md" in claude
    # the corrected query surface, never the bad CLI form
    assert 'okg query "' not in claude
    assert is_installed(tmp_path)


def test_codex_places_skill_files_under_agents_skills(tmp_path: Path) -> None:
    res = install_agent_setup("codex", tmp_path)
    # Codex / portable agents read .agents/skills/ — the skill FILES land there
    # (per bundle skill), not just a prose section.
    bundle = {p.name for p in bundled_skills()}
    assert bundle
    for name in bundle:
        assert (tmp_path / ".agents" / "skills" / name / "SKILL.md").is_file()
    assert res.skill_count == len(bundle)
    # plus the marker-delimited PACT/OKG section in AGENTS.md
    agents = tmp_path / "AGENTS.md"
    assert agents.is_file()
    agents_text = agents.read_text()
    assert _MARKER_BEGIN in agents_text
    assert "docs/runbooks/agents/ui.md" in agents_text
    # the codex target does not create .claude/
    assert not (tmp_path / ".claude").exists()
    assert is_installed(tmp_path)


def test_install_is_idempotent_per_target(tmp_path: Path) -> None:
    install_agent_setup("claude-code", tmp_path)
    second = install_agent_setup("claude-code", tmp_path)
    claude = (tmp_path / "CLAUDE.md").read_text()
    assert claude.count(_MARKER_BEGIN) == 1  # refreshed, not duplicated
    assert second.instruction_action == "refreshed"
    # skills still present after the re-run
    for name in {p.name for p in bundled_skills()}:
        assert (tmp_path / ".claude" / "skills" / name / "SKILL.md").is_file()


def test_preserves_unrelated_content_outside_marker(tmp_path: Path) -> None:
    claude = tmp_path / "CLAUDE.md"
    claude.write_text("# My project\n\nExisting instructions.\n")
    res = install_agent_setup("claude-code", tmp_path)
    text = claude.read_text()
    assert "Existing instructions." in text
    assert _MARKER_BEGIN in text
    assert res.instruction_action == "appended"


def test_unknown_agent_raises(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="unknown --agent"):
        install_agent_setup("emacs", tmp_path)


def test_cli_install_via_main(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    # The full bundle is for the okg repository; a foreign repository gets
    # the `pact` skill only (tests/pact/test_pact_install_other_repos.py).
    (tmp_path / "src" / "okg" / "substrate" / "pact" / "skills").mkdir(parents=True)
    rc = main([
        "pact", "--root", str(tmp_path), "install",
        "--agent", "claude-code", "--no-gh-alias", "--json",
    ])
    out = json.loads(capsys.readouterr().out)
    assert rc == 0
    assert out["ok"] is True
    assert out["agent"] == "claude-code"
    assert out["skill_count"] >= 3
    assert (tmp_path / ".claude" / "skills" / "pact" / "SKILL.md").is_file()
    assert (tmp_path / "CLAUDE.md").is_file()


def test_install_sync_updates_prunes_and_preserves_orphans(
    tmp_path: Path,
) -> None:
    canon = tmp_path / "canon"
    dest = tmp_path / "repo"
    _write_skill(canon, "alpha", "v1")
    _write_skill(canon, "beta", "v1")

    first = install_agent_setup("codex", dest, skills_dir=canon)
    assert first.skills_added == ["alpha", "beta"]
    assert first.skills_updated == []
    assert first.skills_pruned == []
    assert first.skills_orphaned == []
    assert first.to_json()["diff_summary"] == {
        "added": 2,
        "updated": 0,
        "unchanged": 0,
        "pruned": 0,
        "orphaned": 0,
    }

    _write_skill(canon, "alpha", "v2")
    shutil.rmtree(canon / "beta")
    _write_skill(dest / ".agents" / "skills", "local-only", "keep")

    second = install_agent_setup("codex", dest, skills_dir=canon)
    assert second.skills_added == []
    assert second.skills_updated == ["alpha"]
    assert second.skills_pruned == ["beta"]
    assert second.skills_orphaned == ["local-only"]
    assert not (dest / ".agents" / "skills" / "beta").exists()
    assert (dest / ".agents" / "skills" / "local-only" / "SKILL.md").is_file()
    assert "v2" in (
        dest / ".agents" / "skills" / "alpha" / "SKILL.md"
    ).read_text(encoding="utf-8")

    third = install_agent_setup("codex", dest, skills_dir=canon)
    assert third.skills_added == []
    assert third.skills_updated == []
    assert third.skills_pruned == []
    assert third.skills_unchanged == ["alpha"]
    assert third.skills_orphaned == ["local-only"]

    shutil.rmtree(dest / ".agents" / "skills" / "alpha")
    (dest / ".agents" / "skills" / "alpha").write_text(
        "corrupted install",
        encoding="utf-8",
    )
    repaired = install_agent_setup("codex", dest, skills_dir=canon)
    assert repaired.skills_updated == ["alpha"]
    assert (dest / ".agents" / "skills" / "alpha" / "SKILL.md").is_file()


def test_agent_skill_freshness_classifies_installed_states(
    tmp_path: Path,
) -> None:
    canon = tmp_path / "canon"
    dest = tmp_path / "repo"
    for name in ("current", "stale", "missing"):
        _write_skill(canon, name, "v1")
    install_agent_setup("codex", dest, skills_dir=canon)

    _write_skill(dest / ".agents" / "skills", "stale", "v2")
    shutil.rmtree(dest / ".agents" / "skills" / "missing")
    _write_skill(dest / ".agents" / "skills", "orphan", "local")

    report = agent_skill_freshness(dest, skills_dir=canon, targets=("codex",))

    assert report["ok"] is False
    assert report["status"] == "blocked"
    assert report["stale_count"] == 1
    assert report["missing_count"] == 1
    assert report["orphaned_count"] == 1
    skills = report["targets"]["codex"]["skills"]
    assert skills["current"]["status"] == "current"
    assert skills["stale"]["status"] == "stale"
    assert skills["missing"]["status"] == "missing"
    assert report["targets"]["codex"]["orphaned"] == ["orphan"]
    assert "okg pact install --agent codex" in report["fix_hint"]


def test_doctor_agent_skills_check(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    install_agent_setup("codex", tmp_path)
    install_agent_setup("claude-code", tmp_path)
    monkeypatch.chdir(tmp_path)

    assert main(["doctor", "--check", "agent-skills", "--json"]) == 0
    current = json.loads(capsys.readouterr().out)
    assert current["check"] == "agent-skills"
    assert current["status"] == "current"
    assert current["ok"] is True

    skill = tmp_path / ".agents" / "skills" / "pact" / "SKILL.md"
    skill.write_text(
        skill.read_text(encoding="utf-8") + "\nlocal edit\n",
        encoding="utf-8",
    )
    assert main(["doctor", "--check", "agent-skills", "--json"]) == 1
    stale = json.loads(capsys.readouterr().out)
    assert stale["code"] == "agent_skills_stale_or_missing"
    assert stale["stale_count"] == 1
    assert "okg pact install --agent codex" in stale["fix_hint"]


def _write_skill(root: Path, name: str, body: str) -> None:
    skill = root / name
    skill.mkdir(parents=True, exist_ok=True)
    skill.joinpath("SKILL.md").write_text(
        f"---\nname: {name}\ndescription: Test skill {name}\n---\n{body}\n",
        encoding="utf-8",
    )
