"""OpenSpecSource change-probe (#323) — git subtree scoping + non-git fallback."""
from __future__ import annotations

import subprocess

import pytest

from okg.substrate.library.sources.change_probe import tokens_equal
from okg.substrate.library.sources.content_hash_probe import ContentHashProbe
from okg.substrate.library.sources.openspec import OpenSpecSource


@pytest.fixture
def repo_with_openspec(tmp_path):
    repo = tmp_path / "repo"
    (repo / "openspec" / "changes" / "demo").mkdir(parents=True)
    (repo / "openspec" / "changes" / "demo" / "proposal.md").write_text("# Demo\n")
    (repo / "README.md").write_text("top\n")  # outside the openspec subtree
    subprocess.run(["git", "-C", str(repo), "init", "-q"], check=True)
    subprocess.run(["git", "-C", str(repo), "config", "user.email", "t@t"], check=True)
    subprocess.run(["git", "-C", str(repo), "config", "user.name", "t"], check=True)
    subprocess.run(["git", "-C", str(repo), "add", "-A"], check=True)
    subprocess.run(["git", "-C", str(repo), "commit", "-q", "-m", "i"], check=True)
    return repo


def test_declares_captured_content_probe(repo_with_openspec):
    assert OpenSpecSource.change_probe_kind == "content_hash"
    s = OpenSpecSource(repo="demo", openspec_root=str(repo_with_openspec / "openspec"))
    assert isinstance(s.change_probe, ContentHashProbe)


def test_edit_outside_subtree_is_ignored(repo_with_openspec):
    s = OpenSpecSource(repo="demo", openspec_root=str(repo_with_openspec / "openspec"))
    t0 = s.change_probe.build_token()
    (repo_with_openspec / "README.md").write_text("top-edited\n")
    assert tokens_equal(t0, s.change_probe.build_token()), "non-openspec changes ignored"


def test_edit_inside_subtree_detected(repo_with_openspec):
    s = OpenSpecSource(repo="demo", openspec_root=str(repo_with_openspec / "openspec"))
    t0 = s.change_probe.build_token()
    (repo_with_openspec / "openspec" / "changes" / "demo" / "proposal.md").write_text("# Edited\n")
    assert not tokens_equal(t0, s.change_probe.build_token())


def test_delete_inside_subtree_detected(repo_with_openspec):
    s = OpenSpecSource(repo="demo", openspec_root=str(repo_with_openspec / "openspec"))
    t0 = s.change_probe.build_token()
    (repo_with_openspec / "openspec" / "changes" / "demo" / "proposal.md").unlink()
    assert not tokens_equal(t0, s.change_probe.build_token())


def test_non_git_openspec_dir_falls_back_to_content_hash(tmp_path):
    osr = tmp_path / "openspec"
    (osr / "changes").mkdir(parents=True)
    (osr / "changes" / "x.md").write_text("hi\n")
    s = OpenSpecSource(repo="demo", openspec_root=str(osr))
    assert isinstance(s.change_probe, ContentHashProbe)
    t0 = s.change_probe.build_token()
    (osr / "changes" / "x.md").write_text("hi-edited\n")
    assert not tokens_equal(t0, s.change_probe.build_token())


def test_filtered_crossref_probe_covers_external_dirty_add_delete_and_content(
    repo_with_openspec,
):
    proposal = (
        repo_with_openspec
        / "openspec"
        / "changes"
        / "demo"
        / "proposal.md"
    )
    proposal.write_text(
        "# Change: Demo\n\n"
        "## Why\n\n"
        "The implementation is in @/src/demo.py.\n",
        encoding="utf-8",
    )
    source = OpenSpecSource(
        repo="demo",
        openspec_root=repo_with_openspec / "openspec",
        filter_unresolved_crossrefs=True,
    )
    assert source.change_probe_kind == "content_hash"
    assert isinstance(source.change_probe, ContentHashProbe)

    missing = source.change_probe.build_token()
    external = repo_with_openspec / "src" / "demo.py"
    external.parent.mkdir()
    external.write_text("value = 1\n", encoding="utf-8")
    added = source.change_probe.build_token()
    assert not tokens_equal(missing, added)

    external.write_text("value = 2\n", encoding="utf-8")
    rewritten = source.change_probe.build_token()
    assert not tokens_equal(added, rewritten)

    external.unlink()
    deleted = source.change_probe.build_token()
    assert not tokens_equal(rewritten, deleted)
    assert tokens_equal(missing, deleted)
