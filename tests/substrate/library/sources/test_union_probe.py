"""Tests for UnionProbe (#323) — the hybrid-scope combinator.

Models the pact (git + ledger) and session (git + transcripts) shape: a change
in EITHER half busts the token, neither leaves it unchanged, and a sub-probe
error fails toward a full read.
"""
from __future__ import annotations

import os
import subprocess

import pytest

from okg.substrate.library.sources.change_probe import (
    ChangeProbeError,
    run_probe,
    tokens_equal,
)
from okg.substrate.library.sources.content_hash_probe import (
    ContentHashProbe,
    file_content_items,
)
from okg.substrate.library.sources.git_scope_probe import GitScopeProbe
from okg.substrate.library.sources.union_probe import UnionProbe


def _emit(x):
    return x


@pytest.fixture
def git_plus_content(tmp_path):
    # half A: a git repo;  half B: a separate non-git content dir.
    repo = tmp_path / "repo"
    repo.mkdir()
    subprocess.run(["git", "-C", str(repo), "init", "-q"], check=True)
    subprocess.run(["git", "-C", str(repo), "config", "user.email", "t@t"], check=True)
    subprocess.run(["git", "-C", str(repo), "config", "user.name", "t"], check=True)
    (repo / "a.py").write_text("import os\n")
    subprocess.run(["git", "-C", str(repo), "add", "-A"], check=True)
    subprocess.run(["git", "-C", str(repo), "commit", "-q", "-m", "i"], check=True)
    content = tmp_path / "external"
    content.mkdir()
    (content / "t.jsonl").write_text('{"e":1}\n')
    git = GitScopeProbe(repo_root=repo, config={"r": "x"}, emit_targets=_emit)
    chp = ContentHashProbe(
        content_items=lambda: file_content_items(content, include_globs=("*.jsonl",)),
        config={"c": "x"}, emit_targets=_emit,
    )
    return repo, content, UnionProbe([git, chp])


def test_requires_a_subprobe():
    with pytest.raises(ChangeProbeError):
        UnionProbe([])


def test_neither_half_changed_is_unchanged(git_plus_content):
    _, _, u = git_plus_content
    t0 = u.build_token()
    assert tokens_equal(t0, u.build_token())


def test_exact_child_tokens_reproduce_union_token(git_plus_content):
    _, _, union = git_plus_content
    child_tokens = [probe.build_token() for probe in union._probes]
    assert tokens_equal(
        union.token_from_subtokens(child_tokens),
        union.build_token(),
    )


def test_exact_child_token_count_must_match_union(git_plus_content):
    _, _, union = git_plus_content
    with pytest.raises(ChangeProbeError):
        union.token_from_subtokens([union._probes[0].build_token()])


def test_git_half_change_busts_token(git_plus_content):
    repo, _, u = git_plus_content
    t0 = u.build_token()
    (repo / "a.py").write_text("import os\nX = os.getenv('A')\n")
    assert not tokens_equal(t0, u.build_token())


def test_content_half_change_busts_token(git_plus_content):
    _, content, u = git_plus_content
    t0 = u.build_token()
    (content / "t.jsonl").write_text('{"e":1}\n{"e":2}\n')  # transcript append
    assert not tokens_equal(t0, u.build_token())


def test_content_half_delete_busts_token(git_plus_content):
    _, content, u = git_plus_content
    (content / "u.jsonl").write_text('{"e":9}\n')
    t0 = u.build_token()
    os.remove(content / "u.jsonl")  # DELETE in the content half
    assert not tokens_equal(t0, u.build_token())


def test_mtime_churn_does_not_bust_token(git_plus_content):
    repo, content, u = git_plus_content
    t0 = u.build_token()
    os.utime(repo / "a.py", (2_000_000_000, 2_000_000_000))
    os.utime(content / "t.jsonl", (2_000_000_000, 2_000_000_000))
    assert tokens_equal(t0, u.build_token()), "union of content-identity probes is churn-immune"


class _RaisingProbe:
    def build_token(self):
        raise RuntimeError("ledger connection failed")

    def probe(self, *, cursor, sync_scope):
        raise RuntimeError("ledger connection failed")


def test_subprobe_error_fails_toward_full_read():
    u = UnionProbe([_RaisingProbe()])
    v = run_probe(u, cursor=None, sync_scope=None)
    assert v.changed is True
    assert v.reason.startswith("probe_error:")
