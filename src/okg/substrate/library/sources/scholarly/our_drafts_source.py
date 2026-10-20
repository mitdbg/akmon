"""OurDraftsSource — substrate adapter for operator-authored drafts.

Reads every `*.tex` / `*.md` file matching the deployment-
configured `draft_paths` glob patterns. Emits one `Paper`
NodeFact per draft with `attrs.kind = our-draft`. Identity is
derived from a canonicalized path + content hash so re-runs
are idempotent even when the file is edited (the hash changes;
the node_id reflects the new revision while preserving the
path-slug component for human readability).

Identity convention:

  `paper:<content_hash_first_16_chars>-<path_slug>`

Per umbrella D11 + S-A1-R5: paths are deployment-configured;
the substrate hardcodes none.

After emission, the OurDraftsSource pairs with the
`LatexStructuralEnricher`. On `.md` files the enricher catches
a `LatexParseError` (no LaTeX environments) and emits an
`extraction_error` Observation — clean failure surface; the
md-handling path will be added in a follow-up child once the
operator markdown convention is settled.
"""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterator, Optional

from okg.substrate.library.sources.base import (
    NodeFact,
    RecordEmission,
    RecordNode,
    RecordSet,
    SourceRun,
)

__all__ = ["OurDraftsSource", "compute_draft_id"]

_SLUG_RE = re.compile(r"[^a-z0-9]+")


def _slugify(value: str) -> str:
    slug = _SLUG_RE.sub("-", value.lower()).strip("-")
    return slug[:50] or "untitled"


def compute_draft_id(draft_path: Path) -> tuple[str, str]:
    """Return (paper_id, content_hash) for a draft `.tex` or `.md`.

    Content-hash-stable. The id format is
    `paper:<hash16>-<path_slug>` where path_slug is derived from
    the file's stem.
    """
    h = hashlib.sha256()
    with draft_path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    content_hash = h.hexdigest()
    slug = _slugify(draft_path.stem)
    return f"paper:{content_hash[:16]}-{slug}", content_hash


@dataclass
class OurDraftsSource:
    """Substrate source for operator-authored draft `.tex` / `.md` files.

    Constructor:
      `draft_paths: list[str]` — list of glob patterns. The
      source expands each glob and discovers matching `.tex` and
      `.md` files. No default; the deployment supplies values.
    """

    draft_paths: list[str]
    kind: str = "our-draft"
    extra_attrs: Optional[dict[str, Any]] = None

    name: str = "our_drafts_source"
    profile: str = "discovery_crawl"

    # Incremental-source contract (issue #323): the scope is a set of local
    # operator-authored `.tex` / `.md` files on disk, with no git head and no
    # mutable upstream API — so the probe is the O(content) content-hash floor.
    # The probe re-runs `_discover()` (the SAME enumeration `run()` uses) every
    # cycle and hashes each file's bytes, so an edit (sha shifts), an add (new
    # `(key, sha)` entry + count bump), and a delete (dropped entry + count
    # drop) all bust the token, while a worktree-checkout mtime bump does not.
    # `draft_paths` / `kind` / `extra_attrs` fold into `config` (they change the
    # emitted attrs / scope under unchanged file bytes). No `now()`-derived value
    # reaches an emitted attr — `title` is the file stem, `content_hash` is the
    # file bytes — so the content floor is sound. A vanished file reads as
    # `absent` inside the probe rather than raising, so a delete is detected as a
    # membership change, not swallowed.
    change_probe_kind = "content_hash"

    def __post_init__(self) -> None:
        if not isinstance(self.draft_paths, (list, tuple)) or not self.draft_paths:
            raise ValueError(
                "OurDraftsSource: `draft_paths` must be a non-empty list"
            )
        self._patterns = [str(p) for p in self.draft_paths]
        self.change_probe = self._build_change_probe()

    def _build_change_probe(self) -> Any:
        import sys

        from okg.substrate.library.sources.content_hash_probe import (
            ContentHashProbe,
        )

        def _items() -> list[tuple[str, Path]]:
            # Reuse the source's OWN enumeration so the probe scope is byte-for-
            # byte the set `run()` reads. A file removed since last cycle is
            # simply absent here, which the membership digest reads as a delete.
            return [(str(path), path) for path in self._discover()]

        return ContentHashProbe(
            content_items=_items,
            config={
                "draft_paths": self._patterns,
                "kind": self.kind,
                "extra_attrs": self.extra_attrs,
            },
            emit_targets=[sys.modules[__name__]],
        )

    def _discover(self) -> list[Path]:
        seen: set[Path] = set()
        result: list[Path] = []
        for pattern in self._patterns:
            expanded = str(Path(pattern).expanduser())
            # If the pattern is a directory, walk it for .tex / .md.
            p = Path(expanded)
            if p.is_dir():
                for path in sorted(p.rglob("*")):
                    if path.is_file() and path.suffix.lower() in {
                        ".tex", ".md",
                    }:
                        if path not in seen:
                            seen.add(path)
                            result.append(path)
                continue
            # Otherwise, treat as glob.
            base = Path(expanded)
            # Find an anchor we can call glob() on.
            anchor = base
            while anchor and (anchor.name and ("*" in anchor.name
                                                or "?" in anchor.name)):
                anchor = anchor.parent
            relative = str(base.relative_to(anchor)) if anchor != base else expanded
            if anchor.is_dir():
                for path in sorted(anchor.glob(relative)):
                    if path.is_file() and path.suffix.lower() in {
                        ".tex", ".md",
                    } and path not in seen:
                        seen.add(path)
                        result.append(path)
            elif base.is_file() and base.suffix.lower() in {".tex", ".md"}:
                if base not in seen:
                    seen.add(base)
                    result.append(base)
        return result

    def run(
        self,
        run_id: str,
        *,
        mode: str = "cursor",
        cursor: Optional[dict[str, Any]] = None,
        sync_scope: Optional[dict[str, Any]] = None,
    ) -> SourceRun:
        revision = {"run_id": run_id}
        discovered: list[tuple[Path, str, str]] = []
        for draft_path in self._discover():
            node_id, content_hash = compute_draft_id(draft_path)
            discovered.append((draft_path, node_id, content_hash))

        next_cursor = {
            "last_run_id": run_id,
            "n_papers_seen": len(discovered),
        }
        completed_scope = mode in {"reconcile", "scope_complete"}
        record_set = (
            self._record_set(discovered, revision=revision)
            if completed_scope else None
        )

        def _facts() -> Iterator[NodeFact]:
            for draft_path, node_id, content_hash in discovered:
                source_attr_key = (
                    "source_tex_path" if draft_path.suffix.lower() == ".tex"
                    else "source_md_path"
                )
                attrs: dict[str, Any] = {
                    "paper_id": node_id,
                    "kind": self.kind,
                    "title": draft_path.stem,
                    source_attr_key: str(draft_path),
                    "content_hash": content_hash,
                }
                if self.extra_attrs:
                    attrs.update(self.extra_attrs)
                yield NodeFact(
                    node_id=node_id,
                    subtype="paper",
                    attrs=attrs,
                    source_record_id={
                        "draft_path": str(draft_path),
                        "content_hash": content_hash,
                    },
                    source_revision=revision,
                    op="I",
                )

        return SourceRun(
            facts=_facts(),
            completed_scope=completed_scope,
            next_cursor=next_cursor,
            run_mode=mode,
            record_set=record_set,
        )

    def _record_set(
        self,
        discovered: list[tuple[Path, str, str]],
        *,
        revision: dict[str, Any],
    ) -> RecordSet:
        records: dict[str, RecordEmission] = {}
        for draft_path, node_id, content_hash in discovered:
            records[node_id] = RecordEmission(
                primary=RecordNode(
                    node_id=node_id, subtype="paper",
                    source_record_id={
                        "draft_path": str(draft_path),
                        "content_hash": content_hash,
                    },
                    source_revision=revision,
                ),
                descendants=[],
                edges=[],
            )
        return RecordSet(records=records)
