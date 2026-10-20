"""notes_dir source — reads markdown notes from a directory.

Per `refine-academic-pi-with-comms-and-calendar` Tier A:
emits `CalendarEvent` + `ActionItem` from markdown meeting
notes with `okg-meeting:` frontmatter. The directory path comes
from the source-registry entry (`notes_dir:` param); deployments
that adopt the academic-researcher profile default to `~/.memex/notes/`,
but the substrate source itself is path-agnostic.

Frontmatter shape::

    ---
    okg-meeting:
      kind: one_on_one
      participants:
        - Alice Chen
        - user@example.com
      actions:
        - owner: Alice Chen
          due: 2026-05-30
          description: send draft of paper
      concept_refs:
        - bitemporal-data
      paper_refs:
        - paper:lamport-1978
    ---

    # 1:1 with Alice — 2026-05-15

    Discussed ...
"""
from __future__ import annotations

import hashlib
import re
from pathlib import Path
from typing import Any, Iterable, Optional

import yaml
from okg.substrate import fastyaml

from okg.substrate.library.sources.base import (
    EdgeFact,
    NodeFact,
    ProgressMarker,
    SourceRun,
)


_FILENAME_RE = re.compile(r"^(\d{4}-\d{2}-\d{2})-(.+)\.md$")


class NotesDirSource:
    """Reads operator-authored meeting notes."""

    name: str = "notes_dir"
    profile: str = "discovery_crawl"

    def __init__(
        self,
        *,
        notes_dir: str = "~/.local/share/okg/notes",
        ownership_id: str = "notes_dir",
        **_ignored: Any,
    ) -> None:
        self.notes_dir = Path(notes_dir).expanduser()
        self.ownership_id = ownership_id

    def run(
        self, run_id: str, *, mode: str = "cursor", **_ignored: Any,
    ) -> SourceRun:
        if not self.notes_dir.is_dir():
            return SourceRun(facts=iter([]), completed_scope=True)
        return SourceRun(
            facts=self._stream(),
            completed_scope=True,
        )

    def _stream(self) -> Iterable[Any]:
        for path in sorted(self.notes_dir.glob("*.md")):
            m = _FILENAME_RE.match(path.name)
            if not m:
                continue
            date_str, slug = m.groups()
            text = path.read_text(encoding="utf-8")
            front = _parse_frontmatter(text)
            if front is None:
                continue
            meeting_block = front.get("okg-meeting")
            if not isinstance(meeting_block, dict):
                continue

            content_hash = hashlib.sha256(text.encode("utf-8")).hexdigest()
            event_id = f"event:notes:{date_str}-{slug}"
            yield NodeFact(
                node_id=event_id,
                subtype="calendar_event",
                attrs={
                    "event_id": event_id,
                    "title": _title_from_text(text, slug),
                    "kind": meeting_block.get("kind", "other"),
                    "start_dt": f"{date_str}T00:00:00",
                    "external_ref": str(path),
                    "location": meeting_block.get("location", ""),
                },
                source_record_id={"path": str(path)},
                source_revision={"content_sha": content_hash},
            )

            for idx, action in enumerate(meeting_block.get("actions") or []):
                if not isinstance(action, dict):
                    continue
                action_id = f"action:notes:{date_str}-{slug}-{idx}"
                yield NodeFact(
                    node_id=action_id,
                    subtype="action_item",
                    attrs={
                        "action_id": action_id,
                        "description": action.get("description", ""),
                        "owner_ref": action.get("owner", ""),
                        "due": action.get("due"),
                        "status": action.get("status", "open"),
                        "parent_meeting_ref": event_id,
                        "source": "meeting",
                    },
                    source_record_id={"path": str(path), "action_idx": idx},
                    source_revision={"content_sha": content_hash},
                )
                # Meeting → ActionItem edge (produces)
                yield EdgeFact(
                    src=event_id,
                    dst=action_id,
                    edge_type="produces",
                    source_record_id={"path": str(path), "action_idx": idx},
                )

            yield ProgressMarker(record_id=str(path), fingerprint=content_hash)


def _parse_frontmatter(text: str) -> Optional[dict]:
    if not text.startswith("---\n"):
        return None
    end = text.find("\n---", 4)
    if end == -1:
        return None
    fm_text = text[4:end]
    try:
        return fastyaml.safe_load(fm_text) or {}
    except yaml.YAMLError:
        return None


def _title_from_text(text: str, slug: str) -> str:
    for line in text.splitlines():
        if line.startswith("# "):
            return line[2:].strip()
    return slug
