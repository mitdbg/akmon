"""A throwaway git repository for PACT v5 tests: a `dev` branch, an `origin`
remote pointing at itself, and a feature branch checked out."""

from __future__ import annotations

import subprocess
from pathlib import Path

import yaml


def git(root: Path, *args: str) -> str:
    out = subprocess.run(["git", *args], cwd=root, capture_output=True, text=True, check=True)
    return out.stdout.strip()


def make_repo(tmp_path: Path) -> Path:
    root = tmp_path / "repo"
    root.mkdir()
    git(root, "init", "-q", "-b", "dev")
    git(root, "config", "user.email", "t@example.com")
    git(root, "config", "user.name", "t")
    (root / "README.md").write_text("# demo\n", encoding="utf-8")
    git(root, "add", "-A")
    git(root, "commit", "-q", "-m", "init")
    # A second commit on dev so the merge base is not the root commit; a
    # receipt on the root commit then lies before the base.
    (root / "CHANGELOG.md").write_text("# changes\n", encoding="utf-8")
    git(root, "add", "-A")
    git(root, "commit", "-q", "-m", "dev")
    git(root, "remote", "add", "origin", str(root))
    git(root, "fetch", "-q", "origin", "dev")
    git(root, "checkout", "-q", "-b", "feature")
    return root


def write_manifest(root: Path, data: dict, *, archived: bool = False) -> Path:
    branch = "archive" if archived else "changes"
    path = root / "pact" / branch / data["id"] / "pact.yaml"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(yaml.safe_dump(data, sort_keys=False), encoding="utf-8")
    return path


def commit_all(root: Path, message: str, *trailers: str) -> str:
    git(root, "add", "-A")
    args = ["commit", "-q", "-m", message]
    for trailer in trailers:
        args += ["-m", trailer]
    git(root, *args)
    return git(root, "rev-parse", "HEAD")


MANIFEST = {
    "pact_version": 5,
    "id": "demo",
    "title": "Demo",
    "tier": 1,
    "why": "Because.",
    "affected_code": ["src/"],
    "requirements": [{"id": "req.demo", "statement": "It works."}],
    "tasks": [{"id": "task.demo", "requirement": "req.demo", "verification": "pytest passes."}],
}
