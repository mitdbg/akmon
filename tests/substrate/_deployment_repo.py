"""Commit a staged fixture deployment so the publisher's provenance guard admits it.

``build_deployment_publisher`` derives repository provenance
(:mod:`okg.substrate.publisher.repository_provenance`) from the deployment
directory: the directory must sit inside a git repository, and neither that
repository nor the code repository may carry uncommitted or untracked paths.
A fixture staged under ``tmp_path`` is not a git repository at all, so every
test that provisions or publishes one must commit it first -- and commit it
AGAIN after any later write into the deployment tree (an advanced fixture
revision, a state file), because the guard reads ``git status`` on each
publish.
"""
from __future__ import annotations

import os
import subprocess
from pathlib import Path

# The fixture repositories are built with the machine's git configuration
# switched OFF. A developer or runner global config can attach hooks, a gpg
# signing key, or a filesystem monitor to every `git commit`, none of which
# this fixture wants and all of which would charge the lane for whatever the
# host happens to have installed. Identity comes from the GIT_*_NAME/EMAIL
# variables below, which git accepts without any config file. The embedded
# `warning: adding embedded git repository` line is git's own and stays;
# only its multi-line hint is suppressed.
HERMETIC_GIT = (
    "git",
    "-c", "core.hooksPath=/dev/null",
    "-c", "commit.gpgsign=false",
    "-c", "core.fsmonitor=false",
    "-c", "advice.addEmbeddedRepo=false",
)
GIT_ENV = {
    "GIT_CONFIG_GLOBAL": os.devnull,
    "GIT_CONFIG_SYSTEM": os.devnull,
    "GIT_AUTHOR_NAME": "e2e", "GIT_AUTHOR_EMAIL": "e2e@okg.example",
    "GIT_COMMITTER_NAME": "e2e", "GIT_COMMITTER_EMAIL": "e2e@okg.example",
}


def commit_deployment_tree(root: Path, message: str = "fixture deployment") -> str:
    """Commit everything under ``root`` as one clean revision, hermetically.

    Initialises the repository on first use (``git init``) and is safe to call
    again after the tree changes: a later call only adds and commits, and a
    tree with nothing to commit is left at its current HEAD. Returns the HEAD
    commit id.

    Fails closed when ``root`` already lies inside some OTHER git repository:
    a ``git init`` there would nest a repository inside a real checkout (the
    okg worktree, for one) and then commit whatever that checkout held.
    Call it right after staging and before ``catalog ownership claim`` /
    ``provision``, and after any later write into the deployment directory.
    """
    root = Path(root).resolve()
    env = {**os.environ, **GIT_ENV}

    def run(*args: str, check: bool = True) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            [*HERMETIC_GIT, *args], cwd=root, env=env, check=check,
            capture_output=True, text=True, timeout=60,
        )

    toplevel = run("rev-parse", "--show-toplevel", check=False)
    if toplevel.returncode == 0:
        enclosing = Path(toplevel.stdout.strip()).resolve()
        if enclosing != root:
            raise RuntimeError(
                f"refusing to git-init {root}: it is already inside the git "
                f"repository at {enclosing}; stage fixture deployments under "
                "tmp_path, never inside a checkout"
            )
    else:
        run("init", "-q", "-b", "main")
    run("add", "-A")
    if run("status", "--porcelain", "--untracked-files=all").stdout.strip():
        run("commit", "-q", "-m", message)
    return run("rev-parse", "HEAD").stdout.strip()
