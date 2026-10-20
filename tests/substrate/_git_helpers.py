"""Small git-repository builders shared by substrate tests."""
from __future__ import annotations

import os
import subprocess
from pathlib import Path


def commit_repo(root: Path, *, email: str, name: str, filename: str) -> None:
    """Create a one-commit git repo at `root` authored and committed by `email`.

    Identity comes from the GIT_AUTHOR_* / GIT_COMMITTER_* environment, which
    git prefers over `user.*` config, so no config calls are needed.
    """
    root.mkdir(parents=True)
    env = {
        **os.environ,
        "GIT_AUTHOR_NAME": name,
        "GIT_AUTHOR_EMAIL": email,
        "GIT_COMMITTER_NAME": name,
        "GIT_COMMITTER_EMAIL": email,
    }
    subprocess.run(
        ["git", "init"], cwd=root, check=True, stdout=subprocess.DEVNULL
    )
    (root / filename).write_text("x\n")
    subprocess.run(["git", "add", "-A"], cwd=root, env=env, check=True)
    subprocess.run(
        ["git", "commit", "-m", f"add {filename}"],
        cwd=root,
        env=env,
        check=True,
        stdout=subprocess.DEVNULL,
    )
