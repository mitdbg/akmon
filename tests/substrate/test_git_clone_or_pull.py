"""clone_or_pull() — structured, never-raises-operationally checkout acquisition.

Exercised against a local fixture remote (no network, no credentials):
clone-if-missing, fast-forward pull, non-fast-forward -> stale (tree untouched),
unreachable url -> failed (no hang), per-path lock serialises concurrent calls,
and no credential token is persisted into .git/config.
"""
from __future__ import annotations

import subprocess
import threading
from pathlib import Path

from okg.substrate.library.sources._git_helpers import clone_or_pull


def _git(cwd: Path, *args: str) -> None:
    subprocess.run(
        ["git", "-C", str(cwd), *args], check=True, capture_output=True
    )


def _commit(repo: Path, name: str, body: str) -> None:
    (repo / name).write_text(body, encoding="utf-8")
    _git(repo, "add", "-A")
    _git(repo, "-c", "user.email=t@t", "-c", "user.name=t", "commit", "-qm", name)


def _init_remote(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    _git(path, "init", "-q")
    _commit(path, "a.txt", "one\n")
    return path


def test_clone_then_fast_forward(tmp_path: Path) -> None:
    remote = _init_remote(tmp_path / "remote")
    dest = tmp_path / "dest"

    r1 = clone_or_pull(dest, str(remote))
    assert r1.status == "cloned"
    assert (dest / ".git").exists()
    assert (dest / "a.txt").exists()

    _commit(remote, "b.txt", "two\n")
    r2 = clone_or_pull(dest, str(remote))
    assert r2.status == "updated"
    assert (dest / "b.txt").exists()  # fast-forwarded to the new commit


def test_non_fast_forward_returns_stale(tmp_path: Path) -> None:
    remote = _init_remote(tmp_path / "remote")
    dest = tmp_path / "dest"
    clone_or_pull(dest, str(remote))

    # Diverge: a local-only commit on dest AND a new commit on the remote.
    _commit(dest, "local.txt", "local\n")
    _commit(remote, "c.txt", "three\n")

    r = clone_or_pull(dest, str(remote))
    assert r.status == "stale"
    assert r.reason  # carries git's refusal message
    # The local tree is left untouched — the local-only file survives, and the
    # un-fast-forwardable remote file was NOT merged in.
    assert (dest / "local.txt").exists()
    assert not (dest / "c.txt").exists()


def test_unreachable_url_fails_fast(tmp_path: Path) -> None:
    dest = tmp_path / "dest"
    r = clone_or_pull(dest, str(tmp_path / "no-such-repo.git"))
    assert r.status == "failed"
    assert not (dest / ".git").exists()


def test_concurrent_calls_serialise(tmp_path: Path) -> None:
    remote = _init_remote(tmp_path / "remote")
    dest = tmp_path / "dest"
    results: list[str] = []
    lock = threading.Lock()

    def worker() -> None:
        out = clone_or_pull(dest, str(remote))
        with lock:
            results.append(out.status)

    threads = [threading.Thread(target=worker) for _ in range(2)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert (dest / ".git").exists()
    # Serialised by the per-path lock: one clones, the other sees .git and pulls.
    assert "cloned" in results
    assert all(s in ("cloned", "updated") for s in results)


def test_clone_persists_no_credential_token(tmp_path: Path) -> None:
    remote = _init_remote(tmp_path / "remote")
    dest = tmp_path / "dest"
    clone_or_pull(dest, str(remote))
    config = (dest / ".git" / "config").read_text(encoding="utf-8")
    # The clean url is stored as-is; no ``user:token@host`` credential embedded.
    assert "@" not in config
    assert str(remote) in config


def _git_out(repo: Path, *args: str) -> str:
    return subprocess.run(
        ["git", "-C", str(repo), *args], capture_output=True, text=True
    ).stdout.strip()


def test_wrong_branch_does_not_fast_forward(tmp_path: Path) -> None:
    """A checkout on a branch other than tracked_branch must degrade to stale,
    NOT silently fast-forward the current branch onto another branch's tip."""
    remote = _init_remote(tmp_path / "remote")
    dest = tmp_path / "dest"
    clone_or_pull(dest, str(remote))
    default = _git_out(dest, "rev-parse", "--abbrev-ref", "HEAD")

    # Switch to an unrelated local branch, then advance the remote's default.
    _git(dest, "checkout", "-q", "-b", "other")
    _commit(remote, "b.txt", "two\n")
    head_before = _git_out(dest, "rev-parse", "HEAD")

    out = clone_or_pull(dest, str(remote), tracked_branch=default)
    assert out.status == "stale"
    # The current branch is untouched — still 'other', at the same commit.
    assert _git_out(dest, "rev-parse", "HEAD") == head_before
    assert _git_out(dest, "rev-parse", "--abbrev-ref", "HEAD") == "other"
    assert not (dest / "b.txt").exists()  # remote's new commit was NOT merged in


def test_detached_head_does_not_fast_forward(tmp_path: Path) -> None:
    remote = _init_remote(tmp_path / "remote")
    dest = tmp_path / "dest"
    clone_or_pull(dest, str(remote))
    _git(dest, "checkout", "-q", "--detach", "HEAD")
    out = clone_or_pull(dest, str(remote))
    assert out.status == "stale"
    assert "detached" in (out.reason or "").lower()


def test_scrub_userinfo_strips_credentials() -> None:
    from okg.substrate.library.sources._git_helpers import _scrub_userinfo
    msg = "fatal: unable to access 'https://user:s3cr3t@gitlab.example.org/x.git/'"
    scrubbed = _scrub_userinfo(msg)
    assert "s3cr3t" not in scrubbed
    assert "user" not in scrubbed
    assert "***@gitlab.example.org" in scrubbed


def test_clean_clone_url_strips_credentials() -> None:
    from okg.substrate.library.sources._git_helpers import _clean_clone_url
    # http(s) userinfo (token@ or user:pass@) is a credential -> stripped
    assert _clean_clone_url("https://user:tok@gitlab.example.org/x.git") == "https://gitlab.example.org/x.git"
    assert _clean_clone_url("https://ghp_abc123@github.example.com/o/r.git") == "https://github.example.com/o/r.git"
    assert _clean_clone_url("https://gitlab.example.org/x.git") == "https://gitlab.example.org/x.git"
    # SSH login is NOT a credential -> preserved (both url-style and scp-style)
    assert _clean_clone_url("ssh://git@gitlab.example.org:7999/x.git") == "ssh://git@gitlab.example.org:7999/x.git"
    assert _clean_clone_url("git@github.com:org/repo.git") == "git@github.com:org/repo.git"


def test_pull_repoints_when_configured_url_changes(tmp_path: Path) -> None:
    """If the configured url changes to a DIFFERENT repo, clone_or_pull must not
    return 'updated' while indexing the old repo — it re-points origin and the
    ff-pull cleanly refuses the unrelated history (-> stale)."""
    remote_a = _init_remote(tmp_path / "remote_a")            # has a.txt
    remote_b = tmp_path / "remote_b"                          # unrelated repo
    remote_b.mkdir()
    _git(remote_b, "init", "-q")
    _commit(remote_b, "b.txt", "bbb\n")

    dest = tmp_path / "dest"
    assert clone_or_pull(dest, str(remote_a)).status == "cloned"
    out = clone_or_pull(dest, str(remote_b))                  # url now points at B
    assert out.status != "updated"                           # NOT silently "fresh"
    # origin was re-pointed to the configured (B) url
    assert _git_out(dest, "remote", "get-url", "origin").rstrip("/") == str(remote_b).rstrip("/")


def test_timeout_maps_to_failed(tmp_path: Path, monkeypatch) -> None:
    import subprocess as sp
    from okg.substrate.library.sources import _git_helpers as gh

    def _boom(*a, **k):
        raise sp.TimeoutExpired(cmd="git", timeout=1)

    monkeypatch.setattr(gh.subprocess, "run", _boom)
    out = gh.clone_or_pull(tmp_path / "dest", "https://example.invalid/x.git")
    assert out.status == "failed"
    assert "timed out" in (out.reason or "")
