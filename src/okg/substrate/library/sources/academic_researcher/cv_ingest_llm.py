"""cv_ingest_llm source — LLM-driven CV section classifier.

Per `refine-academic-pi-with-comms-and-calendar` Tier B:
reads a TeX or PDF CV, runs a section-classifier LLM prompt
over each section heading, emits TalkEngagement + Committee +
Course + RecommendationLetter candidates.

This is a scaffold. The class shape, config params, and
emission contract are committed. Full LLM integration requires
the substrate's LLM client + prompt YAML loading + per-section
parsing; that integration is the implementation work that
follows the schema commitment in this proposal.
"""
from __future__ import annotations

import hashlib
import re
from pathlib import Path
from typing import Any, Iterable, Optional

from okg.substrate.library.sources.base import (
    NodeFact,
    ProgressMarker,
    SourceRun,
)


class CvIngestLlmSource:
    """LLM-driven CV ingester. Scaffold."""

    name: str = "cv_ingest_llm"
    profile: str = "discovery_crawl"

    def __init__(
        self,
        *,
        cv_path: Optional[str] = None,
        confidence_floor: float = 0.70,
        prompt_path: Optional[str] = None,
        ownership_id: str = "cv_ingest_llm",
        **_ignored: Any,
    ) -> None:
        self.cv_path = cv_path
        self.confidence_floor = confidence_floor
        self.prompt_path = prompt_path or str(
            Path(__file__).parent / "prompts"
            / "cv_section_classifier.yaml"
        )
        self.ownership_id = ownership_id

    def run(
        self, run_id: str, *, mode: str = "cursor", **_ignored: Any,
    ) -> SourceRun:
        if not self.cv_path or not Path(self.cv_path).is_file():
            return SourceRun(facts=iter([]), completed_scope=True)

        # Heading-based fast path (no LLM): catches the common
        # CV structure. LLM integration is a follow-up.
        return SourceRun(
            facts=self._stream(),
            completed_scope=True,
        )

    def _stream(self) -> Iterable[Any]:
        text = self._read_cv()
        content_hash = hashlib.sha256(text.encode("utf-8")).hexdigest()
        for kind_name, slug, entry_text in _split_sections(text):
            if kind_name == "talks":
                yield from self._emit_talks(entry_text, content_hash)
            elif kind_name == "committees":
                yield from self._emit_committees(entry_text, content_hash)
            elif kind_name == "courses":
                yield from self._emit_courses(entry_text, content_hash)
            elif kind_name == "letters":
                yield from self._emit_letters(entry_text, content_hash)
            yield ProgressMarker(
                record_id=f"cv:{slug}", fingerprint=content_hash,
            )

    def _read_cv(self) -> str:
        path = Path(self.cv_path)
        if path.suffix.lower() == ".tex":
            return path.read_text(encoding="utf-8", errors="replace")
        if path.suffix.lower() == ".pdf":
            # PDF extraction requires the substrate's PDF
            # parser. Scaffolded: return empty for now.
            return ""
        return path.read_text(encoding="utf-8", errors="replace")

    def _emit_talks(self, text: str, content_hash: str) -> Iterable[NodeFact]:
        for idx, item in enumerate(_split_entries(text)):
            talk_id = f"talk:cv:{content_hash[:12]}:{idx}"
            yield NodeFact(
                node_id=talk_id,
                subtype="talk_engagement",
                attrs={
                    "talk_id": talk_id,
                    "title": item[:200],
                    "kind": "invited_talk",
                    "status": "given",
                    "_provenance": "extracted_model",
                    "_confidence": 0.75,
                },
                source_record_id={"cv_hash": content_hash, "idx": idx},
                source_revision={"content_sha": content_hash},
            )

    def _emit_committees(self, text: str, content_hash: str) -> Iterable[NodeFact]:
        for idx, item in enumerate(_split_entries(text)):
            cid = f"committee:cv:{content_hash[:12]}:{idx}"
            yield NodeFact(
                node_id=cid,
                subtype="committee",
                attrs={
                    "committee_id": cid,
                    "name": item[:200],
                    "kind": "pc",
                    "role": "member",
                    "status": "active",
                    "_provenance": "extracted_model",
                    "_confidence": 0.75,
                },
                source_record_id={"cv_hash": content_hash, "idx": idx},
                source_revision={"content_sha": content_hash},
            )

    def _emit_courses(self, text: str, content_hash: str) -> Iterable[NodeFact]:
        for idx, item in enumerate(_split_entries(text)):
            cid = f"course:cv:{content_hash[:12]}:{idx}"
            yield NodeFact(
                node_id=cid,
                subtype="course",
                attrs={
                    "course_id": cid,
                    "code": "",
                    "title": item[:200],
                    "term": "",
                    "role": "instructor",
                    "_provenance": "extracted_model",
                    "_confidence": 0.75,
                },
                source_record_id={"cv_hash": content_hash, "idx": idx},
                source_revision={"content_sha": content_hash},
            )

    def _emit_letters(self, text: str, content_hash: str) -> Iterable[NodeFact]:
        for idx, item in enumerate(_split_entries(text)):
            lid = f"letter:cv:{content_hash[:12]}:{idx}"
            yield NodeFact(
                node_id=lid,
                subtype="recommendation_letter",
                attrs={
                    "letter_id": lid,
                    "recipient": item[:200],
                    "status": "sent",
                    "_provenance": "extracted_model",
                    "_confidence": 0.70,
                },
                source_record_id={"cv_hash": content_hash, "idx": idx},
                source_revision={"content_sha": content_hash},
            )


_SECTION_HEADINGS = {
    "talks": re.compile(
        r"\\section\*?\{[^}]*(invited\s+talks?|keynotes?|colloquia)[^}]*\}",
        re.I,
    ),
    "committees": re.compile(
        r"\\section\*?\{[^}]*(service|program\s+committee|"
        r"editorial)[^}]*\}",
        re.I,
    ),
    "courses": re.compile(
        r"\\section\*?\{[^}]*(teaching|courses)[^}]*\}",
        re.I,
    ),
    "letters": re.compile(
        r"\\section\*?\{[^}]*(letters?\s+of\s+recommendation|"
        r"recommendations)[^}]*\}",
        re.I,
    ),
}


def _split_sections(text: str) -> list[tuple[str, str, str]]:
    """Return [(kind_name, slug, text)] per identified section."""
    out = []
    for kind, pat in _SECTION_HEADINGS.items():
        m = pat.search(text)
        if not m:
            continue
        start = m.end()
        # Bound by next \section{}.
        end_m = re.search(r"\\section\*?\{", text[start:])
        end = start + (end_m.start() if end_m else len(text) - start)
        out.append((kind, kind, text[start:end].strip()))
    return out


def _split_entries(section_text: str) -> list[str]:
    """Naive entry-splitter: LaTeX itemize / bullet / dash lines."""
    entries: list[str] = []
    for line in section_text.splitlines():
        s = line.strip()
        if s.startswith(r"\item"):
            entries.append(s[len(r"\item"):].strip())
        elif s.startswith("- ") or s.startswith("* "):
            entries.append(s[2:].strip())
    return [e for e in entries if e]
