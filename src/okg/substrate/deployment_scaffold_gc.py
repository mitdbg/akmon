"""Garbage-collect stale operator-state deployment scaffolds.

`okg install` / `okg deployment new` scaffold deployments under the user-state
deployments root (``user_state_deployments_root()``). Over time that root
accumulates throwaway scaffolds — incomplete directories with no
``deployment.yaml`` (an abandoned `new`/`install` run) and numbered
experiment scaffolds (e.g. ``foo-1``, ``foo-2``) whose primary
(``foo``) still exists. Nothing reaps these, so the root grows unbounded.

This mirrors the list/gc + dry-run-unless-confirm shape of
``deployment_isolation`` (DB clones) and ``cli.deployment_worktree.gc_forks``
(experiment forks): list first, drop only with an explicit confirm. It NEVER
removes a directory that loads as a valid deployment (one with a
``deployment.yaml``) unless it is a numbered sibling of an existing primary
scaffold, and it never silently falls back to a destructive default.
"""
from __future__ import annotations

import re
import shutil
from pathlib import Path
from typing import Any


# `<name>-<digits>` — the numbered-throwaway shape `okg deployment new`
# produces when disambiguating a scaffold whose name is already taken.
_NUMBERED_SUFFIX_RE = re.compile(r"^(?P<base>.+)-(?P<n>\d+)$")


def _scaffold_status(
    entry: Path,
    *,
    primary_names: set[str],
) -> tuple[str, str | None]:
    """Classify a directory under the user-state deployments root.

    Returns ``(status, reason)`` where status is one of:
      - ``"deployment"``: a valid deployment (has deployment.yaml) that is
        NOT a numbered throwaway → keep.
      - ``"incomplete_scaffold"``: directory with no deployment.yaml → stale.
      - ``"numbered_throwaway"``: ``<base>-<n>`` whose ``<base>`` is also a
        valid deployment here → stale.
    """
    has_manifest = (entry / "deployment.yaml").is_file()
    match = _NUMBERED_SUFFIX_RE.match(entry.name)
    if match and match.group("base") in primary_names:
        return (
            "numbered_throwaway",
            f"numbered sibling of primary scaffold {match.group('base')!r}",
        )
    if not has_manifest:
        return (
            "incomplete_scaffold",
            "no deployment.yaml (abandoned new/install run)",
        )
    return ("deployment", None)


def list_scaffolds(*, root: Path | None = None) -> dict[str, Any]:
    """List operator-state scaffolds under the user-state deployments root,
    flagging the stale ones (incomplete or numbered-throwaway)."""
    from okg.substrate.deployments import user_state_deployments_root

    scaffolds_root = Path(root) if root is not None else user_state_deployments_root()
    entries: list[dict[str, Any]] = []
    if scaffolds_root.is_dir():
        dirs = sorted(p for p in scaffolds_root.iterdir() if p.is_dir())
        primary_names = {
            p.name
            for p in dirs
            if (p / "deployment.yaml").is_file()
            and not _NUMBERED_SUFFIX_RE.match(p.name)
        }
        for entry in dirs:
            status, reason = _scaffold_status(entry, primary_names=primary_names)
            entries.append({
                "name": entry.name,
                "path": str(entry),
                "status": status,
                "stale": status != "deployment",
                "reason": reason,
            })
    stale = [e for e in entries if e["stale"]]
    return {
        "ok": True,
        "schema": "okg.deployment.scaffold_gc.v1",
        "deployments_root": str(scaffolds_root),
        "root_exists": scaffolds_root.is_dir(),
        "scaffolds": entries,
        "count": len(entries),
        "stale": len(stale),
        "stale_names": [e["name"] for e in stale],
    }


def gc_scaffolds(
    *,
    yes: bool = False,
    root: Path | None = None,
) -> dict[str, Any]:
    """List stale scaffolds and, with ``yes=True``, remove them.

    Dry-run unless ``yes`` is set: returns the stale set and a hint."""
    report = list_scaffolds(root=root)
    stale = [e for e in report["scaffolds"] if e["stale"]]
    report["mode"] = "remove" if yes else "dry_run"
    if not yes:
        report["would_remove"] = [e["name"] for e in stale]
        report["note"] = (
            f"{len(stale)} stale scaffold(s); rerun with --yes to remove"
        )
        return report
    removed: list[str] = []
    errors: list[dict[str, str]] = []
    for entry in stale:
        path = Path(entry["path"])
        try:
            shutil.rmtree(path)
            removed.append(entry["name"])
        except Exception as exc:  # noqa: BLE001 - per-entry report
            errors.append({"name": entry["name"], "error": str(exc)[:160]})
    report["removed"] = removed
    report["errors"] = errors
    report["ok"] = not errors
    return report
