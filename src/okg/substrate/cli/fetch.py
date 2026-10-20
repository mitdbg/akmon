"""`okg fetch-deployments` — clone/update the okg-deployments
repo into the XDG cache.

Per `separate-framework-and-deployments` Phase 3 (2026-05-22):
operators run this to populate `${XDG_CACHE_HOME:-$HOME/.cache}/
okg/deployments/<source>/` with reference deployments. After
the cache is populated, `okg init --profile <name>` can
resolve any cached profile as if it were built-in.

The single profile resolver `discover_profile` (in
`okg.substrate.deployment_bootstrap.profile_init`) consults this
cache as the last cascade step (per `deployment-fetch-resolver`).
When the cache is empty it fails loudly with `profile_fetch_failed`
naming the unconfigured-external-repo condition rather than
silently auto-fetching; the operator runs this verb explicitly.
"""
from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import subprocess
from pathlib import Path
from typing import Optional


from okg.substrate.deployments import _cache_dir


#: This copy names no reference-deployments repository: the command needs
#: --repo or OKG_DEPLOYMENTS_REPO and refuses without one.
DEFAULT_REPO_URL: str | None = None
REPO_REQUIRED_HINT = (
    "no reference-deployments repository given: pass --repo <git URL> "
    "or set OKG_DEPLOYMENTS_REPO"
)
DEFAULT_BRANCH = "main"


def _emit(payload: dict, *, as_json: bool) -> None:
    if as_json:
        print(json.dumps(payload, indent=2))
    else:
        for k, v in payload.items():
            print(f"{k}: {v}")


def _slugify_repo_url(url: str) -> str:
    """Derive a stable cache subdir name from a repo URL.

    Examples:
      git@github.com:example-org/okg-deployments.git
        → example-org-okg-deployments
      https://github.com/example-org/okg-deployments.git
        → example-org-okg-deployments
      file:///srv/repos/okg-deployments
        → repos-okg-deployments  (or similar)

    Designed so the SAME repo via SSH and HTTPS resolves to
    the same cache subdir.
    """
    # Strip `.git` suffix.
    base = url.rstrip("/")
    if base.endswith(".git"):
        base = base[: -len(".git")]
    # Pull out the org/repo pair if it's GitHub-shaped.
    m = re.search(r"github\.com[:/]([^/]+)/([^/]+)$", base)
    if m:
        return f"{m.group(1)}-{m.group(2)}"
    # File URL — use the last 2 path components.
    if base.startswith("file://"):
        parts = base[len("file://"):].strip("/").split("/")
        return "-".join(parts[-2:]) or "local-deployments"
    # Fallback: alnum-only collapse.
    return re.sub(r"[^A-Za-z0-9-]+", "-", base).strip("-").lower()


def cache_dir_for(repo_url: str) -> Path:
    """The cache directory for `repo_url`'s contents.

    `_cache_dir() / "deployments" / <slug>`.
    """
    return _cache_dir() / "deployments" / _slugify_repo_url(repo_url)


def fetch_or_update(
    repo_url: str,
    *,
    branch: str = DEFAULT_BRANCH,
    target: Optional[Path] = None,
) -> Path:
    """Clone `repo_url` into `target` (or its cache slot)
    if absent, else `git fetch + reset --hard origin/<branch>`.

    Returns the resolved local path. Raises `subprocess.
    CalledProcessError` on git failure (caller handles).
    """
    target = target or cache_dir_for(repo_url)
    if (target / ".git").is_dir():
        # Update.
        subprocess.run(
            ["git", "-C", str(target), "fetch", "--quiet",
             "origin", branch],
            check=True,
        )
        subprocess.run(
            ["git", "-C", str(target), "reset", "--quiet",
             "--hard", f"origin/{branch}"],
            check=True,
        )
        return target
    # Clone.
    target.parent.mkdir(parents=True, exist_ok=True)
    if target.exists():
        # Stale non-git dir; nuke and reclone.
        shutil.rmtree(target)
    subprocess.run(
        ["git", "clone", "--quiet", "--branch", branch,
         repo_url, str(target)],
        check=True,
    )
    return target


def list_profiles(cache_root: Path) -> list[str]:
    """Return top-level subdirs of `cache_root` that contain a
    `profile.yaml` — those are the profiles operators can
    `okg init --profile <name>` against.
    """
    if not cache_root.is_dir():
        return []
    out: list[str] = []
    for entry in sorted(cache_root.iterdir()):
        if not entry.is_dir():
            continue
        if entry.name.startswith("."):
            continue
        if (entry / "profile.yaml").is_file():
            from okg.substrate.deployments.admission import archived_deployment

            if archived_deployment(entry) is None:
                out.append(entry.name)
    return out


def cmd_fetch_deployments(
    args: argparse.Namespace, *, as_json: bool,
) -> int:
    """Argparse dispatch for `okg fetch-deployments`."""
    repo_url = getattr(args, "repo", None) or os.environ.get(
        "OKG_DEPLOYMENTS_REPO", DEFAULT_REPO_URL,
    )
    if not repo_url and not (
        getattr(args, "list", False) and getattr(args, "source_name", None)
    ):
        _emit(
            {"status": "repo_required", "hint": REPO_REQUIRED_HINT},
            as_json=as_json,
        )
        return 2
    branch = getattr(args, "branch", None) or DEFAULT_BRANCH
    source_name = getattr(args, "source_name", None)
    if source_name:
        target = _cache_dir() / "deployments" / source_name
    else:
        target = cache_dir_for(repo_url)

    if getattr(args, "list", False):
        profiles = list_profiles(target)
        _emit(
            {
                "status": "ok",
                "cache_dir": str(target),
                "profiles": profiles,
            },
            as_json=as_json,
        )
        return 0

    try:
        resolved = fetch_or_update(
            repo_url, branch=branch, target=target,
        )
    except subprocess.CalledProcessError as e:
        _emit(
            {
                "status": "fetch_failed",
                "repo": repo_url,
                "branch": branch,
                "returncode": e.returncode,
                "hint": (
                    "Check the repository URL and that you can read it."
                ),
            },
            as_json=as_json,
        )
        return e.returncode

    profiles = list_profiles(resolved)
    _emit(
        {
            "status": "ok",
            "repo": repo_url,
            "branch": branch,
            "cache_dir": str(resolved),
            "profiles": profiles,
        },
        as_json=as_json,
    )
    return 0
