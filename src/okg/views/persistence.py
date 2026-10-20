"""Manifest persistence — to/from JSON via POSIX-atomic rename.

The umbrella declares the on-disk manifest as the **byte-exact
provenance record** for a view run. Writes use a temp-file +
``os.replace`` pattern so a crash mid-write leaves the prior
manifest intact (POSIX-atomic rename on the same filesystem).

The format is plain JSON with a ``manifest_version`` field that
lets future schema migrations dispatch on it. A view directory
holds two files written together:

* ``view.md`` — the assembled markdown (the human-readable view).
* ``manifest.json`` — this module's serialization of the
  :class:`okg.views.types.View`.

Both are written via temp+rename. If either rename succeeds and
the other fails, the worse outcome is a divergent pair: the
manifest claims a view body that disagrees with ``view.md``.
Recovery: re-run refresh; the framework will recompute and
overwrite both. The disk manifest is replayable, so divergence is
recoverable.
"""
from __future__ import annotations

import json
import os
import tempfile
from pathlib import Path
from typing import Any

from okg.views.types import Fragment, View

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

MANIFEST_FILENAME = "manifest.json"
ASSEMBLED_FILENAME = "view.md"
# Per `add-portal-fragment-annotations` (2026-04-27):
# parallel artifacts the materializer writes when an
# assembler returns AssembledWithMarkers. Read-only for
# portal renderers; never mirrored into graph node attrs.
MARKERED_FILENAME = "view.markered.md"
FRAGMENT_OFFSETS_FILENAME = "fragment_offsets.json"

# Bumped only when the on-disk schema changes. Keep in sync with
# View.manifest_version's default.
SUPPORTED_MANIFEST_VERSION = 1


# ---------------------------------------------------------------------------
# Serialization
# ---------------------------------------------------------------------------


def _fragment_to_dict(f: Fragment) -> dict[str, Any]:
    return {
        "id": f.id,
        "kind": f.kind,
        "sources": list(f.sources),  # JSON has no tuple; list round-trips
        "source_hashes": dict(f.source_hashes),
        "content": f.content,
        "display_label": f.display_label,
        "prompt_hash": f.prompt_hash,
    }


def _fragment_from_dict(d: dict[str, Any]) -> Fragment:
    return Fragment(
        id=d["id"],
        kind=d["kind"],
        sources=tuple(d["sources"]),
        source_hashes=dict(d["source_hashes"]),
        content=d["content"],
        display_label=d.get("display_label", ""),
        # Absent in manifests written before #693; default "" so a
        # legacy generated fragment refreshes on the next prompt-bearing
        # run (empty != any real prompt hash) rather than being trusted.
        prompt_hash=d.get("prompt_hash", ""),
    )


def view_to_dict(view: View) -> dict[str, Any]:
    """Serialize a :class:`View` to a JSON-ready dict.

    The dict's ``manifest_version`` is the View's own; if a future
    code change wants to bump the schema, it bumps both the type
    default and ``SUPPORTED_MANIFEST_VERSION`` and writes a
    migration in :func:`view_from_dict`.
    """
    return {
        "manifest_version": view.manifest_version,
        "view_id": view.view_id,
        "view_def_hash": view.view_def_hash,
        "generation_id": view.generation_id,
        "fragments": [_fragment_to_dict(f) for f in view.fragments],
    }


def view_from_dict(data: dict[str, Any]) -> View:
    """Reconstruct a :class:`View` from a JSON dict.

    Refuses unknown manifest versions loudly. The caller — likely
    the refresh loop or an external diff tool — gets a clear error
    instead of a silently-misinterpreted manifest.
    """
    version = data.get("manifest_version", 1)
    if version != SUPPORTED_MANIFEST_VERSION:
        raise ValueError(
            f"manifest_version {version!r} is not supported "
            f"(this code supports {SUPPORTED_MANIFEST_VERSION!r}); "
            f"upgrade okg.views or migrate the manifest"
        )
    return View(
        view_id=data["view_id"],
        view_def_hash=data["view_def_hash"],
        manifest_version=version,
        generation_id=data.get("generation_id"),
        fragments=[_fragment_from_dict(f) for f in data["fragments"]],
    )


# ---------------------------------------------------------------------------
# Atomic writes
# ---------------------------------------------------------------------------


def _atomic_write_text(path: Path, text: str) -> None:
    """Write ``text`` to ``path`` via temp file + os.replace.

    POSIX-atomic on the same filesystem: either the rename succeeds
    and ``path`` is fully the new content, or it fails and ``path``
    is unchanged. There is no torn-write window.

    The temp file lives in the same directory as ``path`` so the
    rename stays on the same filesystem (cross-FS rename can fail
    or fall back to copy+unlink, which is not atomic).
    """
    path.parent.mkdir(parents=True, exist_ok=True)

    # delete=False because we explicitly rename the file ourselves;
    # NamedTemporaryFile would unlink it on close otherwise.
    fd, tmp_path = tempfile.mkstemp(
        prefix=f".{path.name}.",
        suffix=".tmp",
        dir=path.parent,
    )
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            f.write(text)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp_path, path)
    except BaseException:
        # Best-effort cleanup if the rename failed (or the write
        # itself raised). The temp file would otherwise be orphaned.
        try:
            os.unlink(tmp_path)
        except FileNotFoundError:
            # The temp file may already have been moved or cleaned up.
            pass
        raise


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def save(
    view: View,
    view_dir: Path | str,
    *,
    assembled: str,
    markered: str | None = None,
    fragment_offsets: list[dict[str, Any]] | None = None,
) -> None:
    """Save ``view`` and its assembled markdown to ``view_dir``.

    Writes ``manifest.json`` and ``view.md`` atomically via
    temp+rename. The writes are NOT a transactional pair (no
    POSIX primitive crosses two files atomically); see module
    docstring for the divergence-recovery story.

    When ``markered`` and ``fragment_offsets`` are provided
    (per `add-portal-fragment-annotations`), also writes
    ``view.markered.md`` and ``fragment_offsets.json``. When
    one is provided without the other this raises ``ValueError``
    — they're a paired artifact (a markered variant without
    offsets has no consumer; offsets without a markered variant
    has no source-of-truth for the wrap positions).

    Stale artifacts: when ``markered`` is None but a previous
    cycle wrote ``view.markered.md`` to this dir, the stale
    file is deleted so the on-disk state matches the
    assembler's intent (a view that switched from a
    markered-aware assembler to a plain ``str`` one cleanly
    drops the artifact).
    """
    out = Path(view_dir)
    payload = json.dumps(view_to_dict(view), indent=2, sort_keys=True)
    _atomic_write_text(out / MANIFEST_FILENAME, payload)
    _atomic_write_text(out / ASSEMBLED_FILENAME, assembled)

    has_markered = markered is not None
    has_offsets = fragment_offsets is not None
    if has_markered != has_offsets:
        raise ValueError(
            "save: markered and fragment_offsets must be "
            "passed together; got "
            f"markered={'set' if has_markered else 'None'}, "
            f"fragment_offsets={'set' if has_offsets else 'None'}"
        )
    markered_path = out / MARKERED_FILENAME
    offsets_path = out / FRAGMENT_OFFSETS_FILENAME
    if has_markered:
        if markered is None or fragment_offsets is None:
            raise ValueError("markered and fragment_offsets must be set together")
        _atomic_write_text(markered_path, markered)
        _atomic_write_text(
            offsets_path,
            json.dumps(fragment_offsets, indent=2, sort_keys=True),
        )
    else:
        # Drop stale markered files so a cycle that returned
        # plain `str` doesn't leave a contradicting on-disk
        # markered variant behind.
        for path in (markered_path, offsets_path):
            try:
                path.unlink()
            except FileNotFoundError:
                # Stale companion files are already absent.
                pass


def load(view_dir: Path | str) -> View:
    """Load a :class:`View` from ``view_dir/manifest.json``.

    Caller is responsible for re-assembling markdown from the
    fragments via the framework's ``assemble`` if it needs the
    rendered view body; ``view.md`` on disk is the cached assembled
    output, but the manifest is what the framework treats as
    authoritative.
    """
    manifest_path = Path(view_dir) / MANIFEST_FILENAME
    data = json.loads(manifest_path.read_text(encoding="utf-8"))
    return view_from_dict(data)
