"""Scoped unique-match span localization over markdown-style documents.

Claims carry no span provenance (design §2), so the injector localizes a
claim's value inside the document region its heading-path scope names and
requires exactly one occurrence — ambiguity or absence is a recorded
rejection, never a guess (design §5).

Scope strings are `` > ``-joined heading titles (``"Systems > Alpha"``);
the empty scope names the whole document. A heading's region runs from the
end of its heading line to the next heading of the same or higher level.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

from okg.bench.injection.types import Span

SCOPE_SEPARATOR = " > "

_HEADING_RE = re.compile(r"^(#{1,6})\s+(.+?)\s*$", re.MULTILINE)


@dataclass(frozen=True)
class Located:
    span: Span


@dataclass(frozen=True)
class NotFound:
    detail: str


@dataclass(frozen=True)
class Ambiguous:
    detail: str
    occurrences: int


LocateResult = Located | NotFound | Ambiguous


@dataclass(frozen=True)
class _Section:
    path: str
    start: int
    end: int


class MarkdownScopeIndex:
    """Heading-path → text-region index for one decoded document."""

    def __init__(self, text: str) -> None:
        self._text = text
        self._sections = self._build(text)

    @staticmethod
    def _build(text: str) -> tuple[_Section, ...]:
        headings = [
            (match.start(), match.end(), len(match.group(1)), match.group(2))
            for match in _HEADING_RE.finditer(text)
        ]
        sections: list[_Section] = []
        stack: list[tuple[int, str]] = []
        for index, (start, line_end, level, title) in enumerate(headings):
            while stack and stack[-1][0] >= level:
                stack.pop()
            stack.append((level, title))
            path = SCOPE_SEPARATOR.join(part for _, part in stack)
            end = len(text)
            for later_start, _, later_level, _ in headings[index + 1 :]:
                if later_level <= level:
                    end = later_start
                    break
            sections.append(_Section(path=path, start=line_end, end=end))
        return tuple(sections)

    def section_path_at(self, offset: int) -> str:
        """Heading path of the deepest section containing ``offset`` ("" = none)."""
        best_path = ""
        best_start = -1
        for section in self._sections:
            if section.start <= offset < section.end and section.start > best_start:
                best_path = section.path
                best_start = section.start
        return best_path

    def region(self, scope: str) -> _Section | None:
        """The unique region named by ``scope`` (empty scope = whole document)."""
        if scope == "":
            return _Section(path="", start=0, end=len(self._text))
        matches = [section for section in self._sections if section.path == scope]
        if len(matches) != 1:
            return None
        return matches[0]

    def locate_unique(self, value: str, scope: str) -> LocateResult:
        """Locate exactly one occurrence of ``value`` inside ``scope``'s region."""
        if not value:
            return NotFound(detail="empty value is not localizable")
        section = self.region(scope)
        if section is None:
            count = sum(1 for candidate in self._sections if candidate.path == scope)
            if count > 1:
                return Ambiguous(detail=f"scope {scope!r} names {count} regions", occurrences=count)
            return NotFound(detail=f"scope {scope!r} not present in document")
        occurrences: list[int] = []
        cursor = section.start
        while True:
            found = self._text.find(value, cursor, section.end)
            if found < 0:
                break
            occurrences.append(found)
            cursor = found + 1
        if not occurrences:
            return NotFound(detail=f"value not present under scope {scope!r}")
        if len(occurrences) > 1:
            return Ambiguous(
                detail=f"value occurs {len(occurrences)} times under scope {scope!r}",
                occurrences=len(occurrences),
            )
        start = occurrences[0]
        return Located(span=Span(start=start, end=start + len(value)))
