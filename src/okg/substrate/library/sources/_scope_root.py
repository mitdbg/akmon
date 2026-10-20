"""Checks that a source's input directory can really be read (issue #2892).

A completed-scope run that returns an empty record set tells the runner to
retract every record the source published before. That is right only when
the source actually looked at its input and found nothing. A directory that
is missing, is not a directory, or cannot be listed looks exactly like an
empty one to a walk that skips errors, so these helpers make that case an
error instead.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Mapping

from okg.substrate.errors import SubstrateError


def require_listable_directory(
    path: Path,
    *,
    source: str,
    role: str,
    reason_code: str,
) -> None:
    """Raise unless ``path`` is a directory whose entries can be listed."""
    if not path.is_dir():
        state = "missing" if not path.exists() else "not a directory"
        raise SubstrateError(
            f"{source}: {role} {str(path)!r} is {state}; refusing to treat it "
            "as an empty scope, which would retract everything this source "
            "published",
            data={
                "source": source,
                "path": str(path),
                "state": state,
                "reason_code": reason_code,
            },
            fix_hint=(
                f"restore {role} or correct the path in source_registry.yaml, "
                "then re-run the ingest"
            ),
        )
    try:
        with os.scandir(path) as entries:
            next(entries, None)
    except OSError as exc:
        raise SubstrateError(
            f"{source}: {role} {str(path)!r} cannot be read ({exc}); "
            "refusing to treat it as an empty scope",
            data={
                "source": source,
                "path": str(path),
                "state": "unreadable",
                "reason_code": reason_code,
            },
            fix_hint=f"make {role} readable by the ingest user, then re-run",
        ) from exc


def walk_files_or_raise(root: Path) -> list[Path]:
    """Every regular file under ``root``; an unreadable directory raises.

    Matches ``Path.rglob('*')`` filtered by ``is_file()`` on Python 3.12:
    symbolic links to directories are not followed, symbolic links to files
    are kept. Unlike ``rglob``, a directory that cannot be listed is an
    error, not an empty directory.
    """

    def _raise(exc: OSError) -> None:
        raise exc

    files: list[Path] = []
    for dirpath, _dirnames, filenames in os.walk(
        root,
        onerror=_raise,
        followlinks=False,
    ):
        for name in filenames:
            candidate = Path(dirpath) / name
            if candidate.is_file():
                files.append(candidate)
    return sorted(files)


def prior_record_count(cursor: Any) -> int | None:
    """How many records the prior record set holds; None when unknown.

    No record-set authority means the runner has nothing it could retract,
    which counts as zero. An inline record set is counted directly; an
    external one is known only through the runner's summary.
    """
    if not isinstance(cursor, Mapping):
        return 0
    inline = cursor.get("record_set")
    if isinstance(inline, Mapping):
        records = inline.get("records", inline)
        return len(records) if isinstance(records, Mapping) else None
    if inline is not None:
        return None
    if cursor.get("record_set_ref") is None:
        return 0
    summary = cursor.get("record_set_summary")
    if isinstance(summary, Mapping):
        count = summary.get("record_count")
        if isinstance(count, int) and not isinstance(count, bool):
            return count
    return None
