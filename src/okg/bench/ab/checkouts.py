"""Repo mirror cache + cutoff-surgical checkouts.

Two consumers:

- the runner, provisioning a task workdir at ``base_commit``;
- the corpus builder, which needs the checkout's ref surface ITSELF to be
  the cutoff: ``GitHistorySource`` walks refs/heads, refs/tags, refs/remotes
  and detached HEAD unconditionally, so leakage control means there is
  exactly one branch ref, parked at ``base_commit``, and nothing else.

Mirrors are cached under ``<data_root>/mirrors/<safe>`` (``git clone
--mirror``) so N instances of the same repo pay for one network clone.
"""

from __future__ import annotations

import subprocess
from pathlib import Path

from .errors import CorpusBuildError


def _run(cmd: list[str], *, cwd: Path | None = None) -> str:
    proc = subprocess.run(
        cmd, capture_output=True, text=True, cwd=str(cwd) if cwd else None, check=False
    )
    if proc.returncode != 0:
        raise CorpusBuildError(
            f"command failed ({proc.returncode}): {' '.join(cmd)}\n{proc.stderr.strip()}"
        )
    return proc.stdout


def _safe(repo: str) -> str:
    return repo.replace("/", "__")


def repo_url(repo: str) -> str:
    if repo.startswith(("https://", "git@", "ssh://")):
        return repo
    if repo.startswith(("/", "./", "file://")):  # local repos (tests, donors)
        return repo
    return f"https://github.com/{repo}.git"


def ensure_mirror(repo: str, *, data_root: Path, fetch: bool = False) -> Path:
    """Clone-or-reuse a bare mirror of ``repo``. ``fetch=True`` updates it."""
    mirror = data_root / "mirrors" / _safe(repo)
    if not mirror.exists():
        mirror.parent.mkdir(parents=True, exist_ok=True)
        _run(["git", "clone", "--mirror", repo_url(repo), str(mirror)])
    elif fetch:
        _run(["git", "-C", str(mirror), "fetch", "--prune", "origin"])
    return mirror


def surgical_checkout(
    repo: str, base_commit: str, dest: Path, *, data_root: Path
) -> Path:
    """Materialize ``repo`` at ``base_commit`` with a cutoff-clean ref surface.

    Postconditions (verified here, re-verified by the leakage audit):
    - ``refs/heads/main`` is the ONLY ref, pointing at ``base_commit``;
    - no tags, no remotes, no reflog entries beyond the checkout itself;
    - HEAD == base_commit.
    """
    if dest.exists():
        raise CorpusBuildError(f"checkout dest already exists: {dest}")
    mirror = ensure_mirror(repo, data_root=data_root)
    if not _commit_exists(mirror, base_commit):
        # One refresh attempt for a stale mirror; a still-missing commit is fatal.
        ensure_mirror(repo, data_root=data_root, fetch=True)
        if not _commit_exists(mirror, base_commit):
            raise CorpusBuildError(
                f"commit {base_commit} not found in mirror of {repo}"
            )
    dest.parent.mkdir(parents=True, exist_ok=True)
    _run(["git", "clone", "--no-tags", "--no-checkout", str(mirror), str(dest)])
    _run(["git", "-C", str(dest), "checkout", "-B", "main", base_commit])
    _run(["git", "-C", str(dest), "remote", "remove", "origin"])
    for ref in _list_refs(dest):
        if ref != "refs/heads/main":
            _run(["git", "-C", str(dest), "update-ref", "-d", ref])
    _run(["git", "-C", str(dest), "reflog", "expire", "--all", "--expire=now"])
    # Deleting refs is not enough: a local clone copies the WHOLE object
    # store, so post-cutoff objects (including gold-fix commits) would stay
    # discoverable via `git fsck --unreachable`. Prune them physically.
    _run(["git", "-C", str(dest), "gc", "--prune=now", "--quiet"])
    verify_ref_surface(dest, base_commit)
    verify_no_unreachable_objects(dest)
    return dest


def verify_ref_surface(checkout: Path, base_commit: str) -> None:
    """Assert the cutoff postconditions; raise CorpusBuildError otherwise."""
    refs = _list_refs(checkout)
    if refs != ["refs/heads/main"]:
        raise CorpusBuildError(
            f"checkout {checkout} has extra refs (leakage surface): {refs}"
        )
    head = _run(["git", "-C", str(checkout), "rev-parse", "HEAD"]).strip()
    want = _run(["git", "-C", str(checkout), "rev-parse", base_commit]).strip()
    if head != want:
        raise CorpusBuildError(
            f"checkout {checkout} HEAD {head} != base_commit {want}"
        )
    remotes = _run(["git", "-C", str(checkout), "remote"]).strip()
    if remotes:
        raise CorpusBuildError(f"checkout {checkout} still has remotes: {remotes}")


def task_workdir(
    repo: str, base_commit: str | None, dest: Path, *, data_root: Path
) -> Path:
    """A plain task workdir: full clone from the mirror, parked at
    ``base_commit`` when given (surgical surface), else the mirror's HEAD."""
    if base_commit is not None:
        return surgical_checkout(repo, base_commit, dest, data_root=data_root)
    if dest.exists():
        raise CorpusBuildError(f"workdir dest already exists: {dest}")
    mirror = ensure_mirror(repo, data_root=data_root)
    dest.parent.mkdir(parents=True, exist_ok=True)
    _run(["git", "clone", "--no-tags", "--single-branch", str(mirror), str(dest)])
    _run(["git", "-C", str(dest), "remote", "remove", "origin"])
    _run(["git", "-C", str(dest), "reflog", "expire", "--all", "--expire=now"])
    _run(["git", "-C", str(dest), "gc", "--prune=now", "--quiet"])
    return dest


def verify_no_unreachable_objects(checkout: Path) -> None:
    """Assert the object store holds nothing beyond the ref surface — the
    counterpart of verify_ref_surface for `git fsck --unreachable` leakage."""
    out = _run(
        ["git", "-C", str(checkout), "fsck", "--unreachable", "--no-progress"]
    )
    unreachable = [
        line for line in out.splitlines() if line.startswith("unreachable ")
    ]
    if unreachable:
        raise CorpusBuildError(
            f"checkout {checkout} retains {len(unreachable)} unreachable "
            f"objects (post-cutoff leakage surface); first: {unreachable[:3]}"
        )


def working_diff(workdir: Path) -> str:
    """The working-tree diff vs HEAD (the SWE-bench deliverable), including
    untracked files."""
    _run(["git", "-C", str(workdir), "add", "-N", "."])
    return _run(["git", "-C", str(workdir), "diff", "HEAD"])


def resolve_pin(repo: str, *, data_root: Path) -> str:
    """Reproducible sha for repos whose benchmark provides none (SWE-QA):
    pinned at first resolution into ``<data_root>/pins.json`` and reused by
    every later run. The pin file is part of the run manifest's dataset
    identity."""
    import json

    pins_path = data_root / "pins.json"
    pins: dict[str, str] = (
        json.loads(pins_path.read_text(encoding="utf-8")) if pins_path.exists() else {}
    )
    if repo in pins:
        return pins[repo]
    mirror = ensure_mirror(repo, data_root=data_root)
    sha = _run(["git", "-C", str(mirror), "rev-parse", "HEAD"]).strip()
    pins[repo] = sha
    pins_path.parent.mkdir(parents=True, exist_ok=True)
    pins_path.write_text(
        json.dumps(pins, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    return sha


def _commit_exists(git_dir: Path, sha: str) -> bool:
    proc = subprocess.run(
        ["git", "-C", str(git_dir), "cat-file", "-e", f"{sha}^{{commit}}"],
        capture_output=True,
        check=False,
    )
    return proc.returncode == 0


def _list_refs(checkout: Path) -> list[str]:
    out = _run(
        ["git", "-C", str(checkout), "for-each-ref", "--format=%(refname)"]
    )
    return [line for line in out.splitlines() if line.strip()]
