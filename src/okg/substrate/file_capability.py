"""Shared local-file capability boundary.

Graph attributes and source records can carry local file paths (`local_pdf_path`,
`source_pdf_path`, …). Serving or parsing such a path must first confirm it
resolves — with symlinks followed — INSIDE an explicitly-configured approved
root, so a graph-controlled absolute path (`/etc/passwd`), a `..` traversal, or
a symlink escaping the corpus cannot read arbitrary local files. This is the
shared primitive the graph-controlled read-side / enrichment path checks reuse
(per `constrain-graph-file-and-parser-ingestion`): the viz pub_pdf read side, the
PDF structural enricher, the viz reader local-file body
(`_reader_document_body_from_local_file`), and the viz profile-artifact server
(`system_profile_artifact`).

Not all local-file containment goes through here: `object_store._is_under` is a
WRITE-path containment (it bounds where the file object store may put/fetch an
object, including not-yet-existing paths) with different semantics, so it is
intentionally separate.
"""
from __future__ import annotations

import os
from collections.abc import Iterable
from pathlib import Path


def resolve_within_approved_root(
    candidate: str | os.PathLike[str] | None,
    roots: Iterable[str | os.PathLike[str]],
    *,
    must_be_file: bool = True,
    max_bytes: int | None = None,
) -> Path | None:
    """Resolve `candidate` and return it iff it is contained within one of the
    approved `roots` (realpath, symlinks followed). Returns None (denied) for:
    a missing/blank candidate, an empty `roots` set (fail closed), a path that
    resolves outside every root, a `..` escape, a symlink whose target escapes
    the root, a non-file when `must_be_file`, or a file larger than `max_bytes`.

    Returning None rather than raising lets callers map a denial to their own
    surface (HTTP 403/404, an extraction_error Observation) without catching.
    """
    if candidate is None or str(candidate).strip() == "":
        return None
    root_paths: list[Path] = []
    for root in roots:
        if root is None or str(root).strip() == "":
            continue
        try:
            root_paths.append(Path(root).expanduser().resolve())
        except (OSError, RuntimeError, ValueError):
            continue
    if not root_paths:
        return None  # no approved root configured -> fail closed
    try:
        resolved = Path(candidate).expanduser().resolve()
    except (OSError, RuntimeError, ValueError):
        return None
    inside = False
    for root in root_paths:
        if resolved == root:
            inside = True
            break
        try:
            resolved.relative_to(root)
            inside = True
            break
        except ValueError:
            continue
    if not inside:
        return None
    if must_be_file and not resolved.is_file():
        return None
    if max_bytes is not None:
        try:
            if resolved.stat().st_size > max_bytes:
                return None
        except OSError:
            return None
    return resolved
