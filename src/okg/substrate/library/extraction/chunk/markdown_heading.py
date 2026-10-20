"""Heading-aware markdown chunker with size normalization.

Walks the `markdown-it-py` AST. Each heading (`#`/`##`/`###`)
opens a section that runs until the next heading of any depth.
Section bodies get size-normalized:

  - target: 300-800 tokens (~1200-3200 chars)
  - min:    100 tokens (~400 chars) — sections smaller than this
            merge with the next sibling chunk in the linear
            sequence (when no next sibling, with the previous
            chunk).
  - max:    1500 tokens (~6000 chars) — sections larger than
            this split at paragraph boundaries; each split
            inherits the section's `heading_path`, with
            `chunk_index_within_section` /
            `chunk_total_within_section` in `extras` to
            disambiguate.

Token estimate: `len(text) // 4` is the convention (English
markdown approximation; not exact for code blocks). Size
decisions measure a chunk without its surrounding whitespace.

The chunks tile the document: each chunk is an exact slice of
the input, carries its `span`, and keeps the blank lines that
follow it, so the chunks in order concatenate to the input
byte for byte (issue #2997).

Contrast with `MarkdownStructuralChunker`: that one emits
paragraph-bounded chunks for fine-grained content-hash dedup,
which produces a long-tailed distribution dominated by very
small chunks (single bullets, one-line code references). This
chunker emits section-bounded, size-normalized chunks for
agent-friendly retrieval. Pick by use case in the deployment's
`extractors.yaml`.

Wraps `markdown-it-py` only — wrap invariant holds.

Per `add-heading-aware-chunker` (2026-04-26).
"""

from __future__ import annotations

import re
from typing import Any, Optional

from okg.substrate.library.extraction._protocols import ParsedChunk


# Token estimate convention: len(text) // 4
_CHARS_PER_TOKEN = 4

# Defaults match the proposal's documented bands.
DEFAULT_MIN_TOKENS = 100
DEFAULT_MAX_TOKENS = 1500

# Line breaks as markdown-it counts them when it numbers lines
# (it normalizes `\r\n` and a lone `\r` to `\n` first).
_LINE_BREAK = re.compile(r"\r\n?|\n")

# One or more blank lines between paragraphs.
_PARAGRAPH_BREAK = re.compile(r"\n\s*\n")


def _content_len(text: str, start: int, end: int) -> int:
    """Length of `text[start:end]` without surrounding whitespace."""
    return len(text[start:end].strip())


class MarkdownHeadingChunker:
    """Heading-aware markdown chunker with size normalization.

    Set `prefers_raw_text = True` so `DocCorpusSource._chunks_from`
    bypasses parser-emitted paragraph chunks and runs this
    chunker on the document's raw text. Without that flag the
    chunker would never see the markdown structure (the parser
    has already pre-split it into paragraphs).
    """

    # _chunks_from in doc_corpus.py honors this — when True, the
    # raw text path runs even if `parsed.chunks` is non-empty.
    prefers_raw_text: bool = True

    def __init__(
        self,
        *,
        min_tokens: int = DEFAULT_MIN_TOKENS,
        max_tokens: int = DEFAULT_MAX_TOKENS,
    ) -> None:
        if min_tokens < 1:
            raise ValueError(
                f"min_tokens must be >= 1; got {min_tokens}"
            )
        if max_tokens <= min_tokens:
            raise ValueError(
                f"max_tokens ({max_tokens}) must exceed "
                f"min_tokens ({min_tokens})"
            )
        self.min_chars = min_tokens * _CHARS_PER_TOKEN
        self.max_chars = max_tokens * _CHARS_PER_TOKEN

    # ------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------

    def chunk(self, text: str) -> list[ParsedChunk]:
        if not text or not text.strip():
            return []

        from markdown_it import MarkdownIt
        md = MarkdownIt("commonmark")
        tokens = md.parse(text)

        sections = self._collect_sections(text, tokens)

        # Pass 1: split oversized sections at paragraph
        # boundaries. Each split is tagged with its index inside
        # the original section so the downstream node can carry
        # `chunk_index_within_section`.
        expanded: list[dict[str, Any]] = []
        for sec in sections:
            if _content_len(text, sec["start"], sec["end"]) > self.max_chars:
                parts = self._split_at_paragraphs(
                    text, sec["start"], sec["end"], self.max_chars,
                )
                total = len(parts)
                for j, (start, end) in enumerate(parts):
                    expanded.append({
                        "heading_path": list(sec["heading_path"]),
                        "start": start,
                        "end": end,
                        "extras": {
                            "chunk_index_within_section": j,
                            "chunk_total_within_section": total,
                        },
                    })
            else:
                expanded.append({
                    "heading_path": list(sec["heading_path"]),
                    "start": sec["start"],
                    "end": sec["end"],
                    "extras": {},
                })

        # Pass 2: merge undersized chunks forward (with the next
        # chunk in the sequence). When the very last chunk is
        # undersized and has no successor, merge it backward
        # into the prior chunk instead. This is order-preserving
        # and stable.
        merged = self._merge_undersized(text, expanded)

        # Materialize as ParsedChunks: exact slices with their
        # spans. The ranges are contiguous from 0 to len(text).
        out: list[ParsedChunk] = []
        for sec in merged:
            out.append(ParsedChunk(
                text=text[sec["start"]:sec["end"]],
                heading_path=list(sec["heading_path"]),
                span=(sec["start"], sec["end"]),
                extras=dict(sec.get("extras") or {}),
            ))
        return out

    # ------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------

    @staticmethod
    def _collect_sections(
        text: str, tokens: list,
    ) -> list[dict[str, Any]]:
        """Walk tokens; gather sections as `[start, end)` ranges.

        A section runs from one heading line (inclusive) up to
        the line of the next heading, so it keeps the blank lines
        before that heading. The preamble (text before the first
        heading) is its own section with `heading_path = []`; a
        blank preamble joins the first heading's section instead.
        Documents with no headings produce a single
        whole-document section. The ranges are contiguous and
        cover `text` from 0 to `len(text)`.

        Section text retains the heading line itself so an
        agent reading the chunk knows which section it's in
        without consulting `heading_path`. (Both are useful;
        heading_path is structured for filtering, the inline
        heading is what the LLM reads.)
        """
        # Character offset of each line start, counted the way
        # markdown-it numbers the lines in `tok.map`.
        line_starts = [0] + [m.end() for m in _LINE_BREAK.finditer(text)]

        def offset_of(line: int) -> int:
            return line_starts[line] if line < len(line_starts) else len(text)

        # Find heading tokens: (level, heading_text, start offset)
        headings: list[tuple[int, str, int]] = []
        i = 0
        while i < len(tokens):
            tok = tokens[i]
            if tok.type == "heading_open":
                level = int(tok.tag[1])  # h1 -> 1, h2 -> 2, ...
                inline = (
                    tokens[i + 1] if i + 1 < len(tokens) else None
                )
                heading_text = (
                    inline.content if inline is not None else ""
                ).strip()
                line_start = (
                    tok.map[0] if tok.map is not None else 0
                )
                headings.append(
                    (level, heading_text, offset_of(line_start)),
                )
                # heading_open / inline / heading_close = 3 tokens
                i += 3
                continue
            i += 1

        if not headings:
            return [{"heading_path": [], "start": 0, "end": len(text)}]

        sections: list[dict[str, Any]] = []

        # Preamble — content before the first heading.
        first_start = headings[0][2]
        section_floor = 0
        if first_start > 0 and text[:first_start].strip():
            sections.append({
                "heading_path": [],
                "start": 0,
                "end": first_start,
            })
            section_floor = first_start

        # One section per heading. Body runs from this heading's
        # line to the NEXT heading's line (any level), exclusive.
        # heading_path is the active heading stack after pushing
        # this heading.
        heading_stack: list[tuple[int, str]] = []
        for idx, (level, hd_text, start) in enumerate(headings):
            while heading_stack and heading_stack[-1][0] >= level:
                heading_stack.pop()
            heading_stack.append((level, hd_text))
            path = [h[1] for h in heading_stack]

            end = (
                headings[idx + 1][2]
                if idx + 1 < len(headings)
                else len(text)
            )
            sections.append({
                "heading_path": list(path),
                "start": section_floor if idx == 0 else start,
                "end": end,
            })

        return sections

    @staticmethod
    def _split_at_paragraphs(
        text: str, start: int, end: int, max_chars: int,
    ) -> list[tuple[int, int]]:
        """Greedily group the paragraphs of `text[start:end]`
        into ranges whose content is <= max_chars.

        Paragraph = stretch separated by one-or-more blank
        lines. Each paragraph keeps the blank lines after it, so
        the returned ranges tile `[start, end)`. Single
        paragraphs that exceed `max_chars` are emitted as their
        own oversize chunk; we don't split prose mid-sentence
        (the proposal explicitly scopes splitting to paragraph
        boundaries). Mid-sentence splitting can land in a
        follow-up if oversize singles prove a real problem.
        """
        cuts = [m.end() for m in _PARAGRAPH_BREAK.finditer(text, start, end)]
        paragraphs: list[tuple[int, int]] = []
        prev = start
        for cut in [*cuts, end]:
            if cut > prev:
                paragraphs.append((prev, cut))
                prev = cut

        chunks: list[tuple[int, int]] = []
        group_start: Optional[int] = None
        group_end = start
        for p_start, p_end in paragraphs:
            if group_start is None:
                group_start, group_end = p_start, p_end
                continue
            if _content_len(text, group_start, p_end) > max_chars:
                chunks.append((group_start, group_end))
                group_start = p_start
            group_end = p_end
        if group_start is not None:
            chunks.append((group_start, group_end))

        return chunks or [(start, end)]

    def _merge_undersized(
        self, text: str, sections: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        """Merge any chunk whose content is below `min_chars`
        into the next chunk in the sequence; the merged chunk
        takes the SUCCESSOR's `heading_path` (because the agent
        reads the deeper context from there). Merged chunks lose
        their `chunk_index_within_section` since the merge
        crosses section identity. The ranges are adjacent, so a
        merge is the union of the two ranges.

        If the final chunk is still undersized and has no
        successor, fold it BACKWARD into the prior chunk
        instead — preserving the prior chunk's `heading_path`.
        Documents with a single undersized chunk emit it as-is.
        """
        if not sections:
            return []

        out: list[dict[str, Any]] = []
        i = 0
        n = len(sections)
        while i < n:
            cur = sections[i]
            if (
                _content_len(text, cur["start"], cur["end"]) < self.min_chars
                and i + 1 < n
            ):
                nxt = sections[i + 1]
                cur = {
                    "heading_path": list(nxt["heading_path"]),
                    "start": cur["start"],
                    "end": nxt["end"],
                    # The merged chunk no longer corresponds to
                    # a single original section, so drop the
                    # split index. (If `nxt` itself was a split,
                    # the index loses meaning post-merge.)
                    "extras": {},
                }
                # Replace nxt in-place so subsequent iterations
                # keep merging if cur is STILL undersized.
                sections[i + 1] = cur
                i += 1
                continue
            out.append(cur)
            i += 1

        # Tail-fold: if the last chunk is still undersized and
        # we have a predecessor, merge backward.
        if (
            len(out) >= 2
            and _content_len(text, out[-1]["start"], out[-1]["end"])
            < self.min_chars
        ):
            tail = out.pop()
            out[-1]["end"] = tail["end"]
            # Predecessor keeps its heading_path; if it had a
            # split index, leave it (the predecessor's identity
            # still reflects its origin section).
        return out


__all__ = ["MarkdownHeadingChunker"]
