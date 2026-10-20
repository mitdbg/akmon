# SPDX-License-Identifier: MIT
"""Bounded structural chunking for raw LaTeX source.

Adapted from archi-physics/cooper ``chunker.py``, MIT licensed:
Git blob 35d1440b9eb250f84b4958235bc2a017fde1b193. The retained license
notice is in ``NOTICE.archi-physics-cooper``.

The scanner never compiles TeX or reads input/include paths. Table chunks use
bounded local macro expansion and validated grid lowering; their authoritative
raw source remains identified by ``span`` and ``source_content_sha256`` while
their chunk text is the canonical searchable table rendering. Other chunks are
exact slices of the supplied Python string.
"""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass, field
from typing import Iterator

from okg.substrate.library.extraction._protocols import ParsedChunk
from okg.substrate.library.extraction.latex_macros import (
    MacroDefinition,
    extract_balanced_group,
    scan_macro_definitions,
)
from okg.substrate.library.extraction.latex_table_grid import (
    LatexTableGridError,
    parse_latex_table_grid,
)
from okg.substrate.library.extraction.table_grid import (
    TableExtractionResult,
    latex_table_locator,
)


_BEGIN_RE = re.compile(r"\\begin\s*\{(?P<name>[A-Za-z@*]+)\}")
_HEADING_HEAD_RE = re.compile(
    r"\\(?P<level>part|section|subsection|subsubsection)\*?(?![A-Za-z@])"
)
_ITEM_TOKEN_RE = re.compile(
    r"\\(?:(?P<boundary>begin|end)\s*\{(?P<name>itemize|enumerate|description)\}|(?P<item>item)\b)"
)
_BLANK_LINE_RE = re.compile(r"\n\s*\n")
_LATEX_COMMAND_RE = re.compile(r"\\[A-Za-z]+\*?(?:\[[^\]]*\])?")

_MATH_ENVS = frozenset(
    {
        "equation",
        "align",
        "gather",
        "eqnarray",
        "multline",
        "displaymath",
    }
)
_FIGURE_ENVS = frozenset({"figure"})
INTERNAL_FIGURE_ENVIRONMENT_SPAN = "_okg_latex_figure_environment_span"
_TABLE_ENVS = frozenset({"table", "tabular", "longtable", "array"})
_LIST_ENVS = frozenset({"itemize", "enumerate", "description"})
_CODE_ENVS = frozenset({"verbatim", "lstlisting", "minted"})
_HEADING_RANK = {"part": 0, "section": 1, "subsection": 2, "subsubsection": 3}


@dataclass(frozen=True)
class _Span:
    start: int
    end: int
    name: str


@dataclass(frozen=True)
class _Heading:
    start: int
    end: int
    level: str
    title: str


def _bare_env(name: str) -> str:
    return name[:-1] if name.endswith("*") else name


def _is_escaped(text: str, position: int) -> bool:
    slashes = 0
    index = position - 1
    while index >= 0 and text[index] == "\\":
        slashes += 1
        index -= 1
    return slashes % 2 == 1


def _search_unescaped(
    pattern: re.Pattern[str],
    text: str,
    position: int = 0,
    end: int | None = None,
) -> re.Match[str] | None:
    """Return the next match whose command slash is not itself escaped."""
    limit = len(text) if end is None else end
    match = pattern.search(text, position, limit)
    while match is not None and _is_escaped(text, match.start()):
        match = pattern.search(text, match.end(), limit)
    return match


def _mask_comments(text: str) -> str:
    chars = list(text)
    index = 0
    while index < len(chars):
        if chars[index] == "%" and not _is_escaped(text, index):
            while index < len(chars) and chars[index] != "\n":
                chars[index] = " "
                index += 1
            continue
        index += 1
    return "".join(chars)


def _mask_ranges(text: str, ranges: list[tuple[int, int]]) -> str:
    chars = list(text)
    for start, end in ranges:
        chars[max(0, start) : min(len(chars), end)] = " " * max(
            0, min(len(chars), end) - max(0, start)
        )
    return "".join(chars)


def _find_env_end(text: str, name: str, search_from: int) -> int | None:
    begin_re = re.compile(r"\\begin\s*\{" + re.escape(name) + r"\}")
    end_re = re.compile(r"\\end\s*\{" + re.escape(name) + r"\}")
    depth = 1
    position = search_from
    while depth:
        begin = _search_unescaped(begin_re, text, position)
        end = _search_unescaped(end_re, text, position)
        if end is None:
            return None
        if begin is not None and begin.start() < end.start():
            depth += 1
            position = begin.end()
        else:
            depth -= 1
            position = end.end()
    return position


def _invalid_end(text: str, start: int) -> int:
    blank = _BLANK_LINE_RE.search(text, start)
    return len(text) if blank is None else blank.start()


def _merge_ranges(ranges: list[tuple[int, int]]) -> list[tuple[int, int]]:
    merged: list[list[int]] = []
    for start, end in sorted((s, e) for s, e in ranges if e > s):
        if not merged or start > merged[-1][1]:
            merged.append([start, end])
        else:
            merged[-1][1] = max(merged[-1][1], end)
    return [(start, end) for start, end in merged]


def _overlaps(start: int, end: int, ranges: list[tuple[int, int]]) -> bool:
    return any(
        start < other_end and other_start < end for other_start, other_end in ranges
    )


def _scan_code_environments(
    scan: str,
) -> tuple[list[_Span], list[str], list[tuple[int, int]]]:
    spans: list[_Span] = []
    warnings: list[str] = []
    invalid: list[tuple[int, int]] = []
    position = 0
    while True:
        match = _search_unescaped(_BEGIN_RE, scan, position)
        if match is None:
            break
        raw_name = match.group("name")
        if _bare_env(raw_name) not in _CODE_ENVS:
            position = match.end()
            continue
        end = _find_env_end(scan, raw_name, match.end())
        if end is None:
            warnings.append(f"latex_unbalanced_environment:{match.start()}")
            invalid.append((match.start(), _invalid_end(scan, match.end())))
            position = match.end()
            continue
        spans.append(_Span(match.start(), end, raw_name))
        position = end
    return spans, warnings, invalid


def _scan_environments(
    scan: str,
) -> tuple[list[_Span], list[str], list[tuple[int, int]]]:
    relevant = _MATH_ENVS | _FIGURE_ENVS | _TABLE_ENVS | _LIST_ENVS
    spans: list[_Span] = []
    warnings: list[str] = []
    invalid: list[tuple[int, int]] = []
    position = 0
    while True:
        match = _search_unescaped(_BEGIN_RE, scan, position)
        if match is None:
            break
        raw_name = match.group("name")
        if _bare_env(raw_name) not in relevant:
            # Do not skip an irrelevant wrapper such as document: relevant
            # structures may be nested inside it.
            position = match.end()
            continue
        end = _find_env_end(scan, raw_name, match.end())
        if end is None:
            warnings.append(f"latex_unbalanced_environment:{match.start()}")
            invalid.append((match.start(), _invalid_end(scan, match.end())))
            position = match.end()
            continue
        spans.append(_Span(match.start(), end, raw_name))
        position = end
    return spans, warnings, invalid


def _scan_display_delimiters(
    scan: str,
    occupied: list[tuple[int, int]],
) -> tuple[list[_Span], list[str], list[tuple[int, int]]]:
    spans: list[_Span] = []
    warnings: list[str] = []
    invalid: list[tuple[int, int]] = []
    position = 0
    while True:
        start = scan.find(r"\[", position)
        if start < 0:
            break
        if _is_escaped(scan, start):
            position = start + 2
            continue
        close_match = _search_unescaped(re.compile(r"\\\]"), scan, start + 2)
        if close_match is None:
            warnings.append(f"latex_unbalanced_display_math:{start}")
            invalid.append((start, _invalid_end(scan, start + 2)))
            break
        end = close_match.end()
        if not _overlaps(start, end, occupied):
            spans.append(_Span(start, end, "display-bracket"))
        position = end

    dollars = [
        match.start()
        for match in re.finditer(r"\$\$", scan)
        if not _is_escaped(scan, match.start())
        and not _overlaps(match.start(), match.end(), occupied)
    ]
    for index in range(0, len(dollars) - 1, 2):
        spans.append(_Span(dollars[index], dollars[index + 1] + 2, "display-dollar"))
    if len(dollars) % 2:
        start = dollars[-1]
        warnings.append(f"latex_unbalanced_display_math:{start}")
        invalid.append((start, _invalid_end(scan, start + 2)))
    return spans, warnings, invalid


def _scan_headings(
    scan: str, raw: str
) -> tuple[list[_Heading], list[str], list[tuple[int, int]]]:
    headings: list[_Heading] = []
    warnings: list[str] = []
    invalid: list[tuple[int, int]] = []
    for match in _HEADING_HEAD_RE.finditer(scan):
        if _is_escaped(scan, match.start()):
            continue
        position = match.end()
        while position < len(scan) and scan[position].isspace():
            position += 1
        if position < len(scan) and scan[position] == "[":
            depth = 1
            position += 1
            while position < len(scan) and depth:
                if scan[position] == "[" and not _is_escaped(scan, position):
                    depth += 1
                elif scan[position] == "]" and not _is_escaped(scan, position):
                    depth -= 1
                position += 1
            if depth:
                warnings.append(f"latex_unbalanced_heading:{match.start()}")
                invalid.append((match.start(), _invalid_end(scan, match.end())))
                continue
            while position < len(scan) and scan[position].isspace():
                position += 1
        if position >= len(scan) or scan[position] != "{":
            continue
        balanced = extract_balanced_group(scan, position)
        if balanced is None:
            warnings.append(f"latex_unbalanced_heading:{match.start()}")
            invalid.append((match.start(), _invalid_end(scan, match.end())))
            continue
        _, end = balanced
        title = raw[position + 1 : end - 1].strip()
        headings.append(_Heading(match.start(), end, match.group("level"), title))
    return headings, warnings, invalid


def _caption_span(scan: str, env: _Span) -> tuple[int, int] | None | bool:
    caption_re = re.compile(r"\\caption\s*\{")
    match = _search_unescaped(caption_re, scan, env.start, env.end)
    if match is None:
        return None
    absolute_start = match.start()
    open_brace = match.end() - 1
    balanced = extract_balanced_group(scan, open_brace)
    if balanced is None or balanced[1] > env.end:
        return False
    return absolute_start, balanced[1]


def _list_item_chunks(raw: str, scan: str, env: _Span) -> list[ParsedChunk]:
    open_end = scan.find("}", env.start, env.end)
    body_start = env.start if open_end < 0 else open_end + 1
    end_match = _search_unescaped(
        re.compile(r"\\end\s*\{" + re.escape(env.name) + r"\}"),
        scan,
        body_start,
        env.end,
    )
    body_end = env.end if end_match is None else end_match.start()
    depth = 0
    item_starts: list[int] = []
    for token in _ITEM_TOKEN_RE.finditer(scan, body_start, body_end):
        if _is_escaped(scan, token.start()):
            continue
        if token.group("boundary") == "begin":
            depth += 1
        elif token.group("boundary") == "end":
            depth = max(0, depth - 1)
        elif token.group("item") and depth == 0:
            item_starts.append(token.start())
    chunks: list[ParsedChunk] = []
    for index, start in enumerate(item_starts):
        end = item_starts[index + 1] if index + 1 < len(item_starts) else body_end
        while end > start and raw[end - 1].isspace():
            end -= 1
        if end > start:
            chunks.append(
                ParsedChunk(
                    text=raw[start:end],
                    span=(start, end),
                    chunk_kind="list_item",
                )
            )
    return chunks


def _environment_chunks(
    raw: str,
    scan: str,
    env: _Span,
    warnings: list[str],
    macro_definitions: list[MacroDefinition],
) -> list[ParsedChunk]:
    bare = _bare_env(env.name)
    if bare in _MATH_ENVS:
        return [
            ParsedChunk(
                raw[env.start : env.end],
                span=(env.start, env.end),
                chunk_kind="equation",
            )
        ]
    if bare in _FIGURE_ENVS:
        caption = _caption_span(scan, env)
        if caption is False:
            warnings.append(f"latex_unbalanced_caption:{env.start}")
            return []
        start, end = (env.start, env.end) if caption is None else caption
        return [
            ParsedChunk(
                raw[start:end],
                span=(start, end),
                extras={INTERNAL_FIGURE_ENVIRONMENT_SPAN: (env.start, env.end)},
                chunk_kind="figure_caption",
            )
        ]
    if bare in _TABLE_ENVS:
        raw_table = raw[env.start : env.end]
        locator = latex_table_locator(env.start, env.end)
        source_hash = hashlib.sha256(raw_table.encode("utf-8")).hexdigest()
        try:
            grid = parse_latex_table_grid(
                raw_table,
                macro_definitions=macro_definitions,
                table_start=env.start,
            )
        except LatexTableGridError as exc:
            warnings.append(f"latex_table_{exc.code}:{env.start}")
            result = TableExtractionResult(
                lane="latex",
                status=exc.status,
                error_code=exc.code,
                source_locator=locator,
                source_content_sha256=source_hash,
            )
            text = raw_table
        else:
            result = TableExtractionResult(
                lane="latex",
                status="parsed",
                grid=grid,
                source_locator=locator,
                source_content_sha256=source_hash,
            )
            text = grid.searchable_text()
        return [
            ParsedChunk(
                text,
                span=(env.start, env.end),
                extras=result.to_chunk_extras(),
                chunk_kind="table",
            )
        ]
    if bare in _LIST_ENVS:
        return _list_item_chunks(raw, scan, env)
    return []


def _has_visible_content(masked: str) -> bool:
    without_commands = _LATEX_COMMAND_RE.sub("", masked)
    return any(char.isalnum() for char in without_commands)


def _paragraph_chunks(
    raw: str,
    visible_scan: str,
    exclusions: list[tuple[int, int]],
) -> Iterator[ParsedChunk]:
    cursor = 0
    for excluded_start, excluded_end in _merge_ranges(exclusions):
        if excluded_start > cursor:
            yield from _split_paragraph_range(raw, visible_scan, cursor, excluded_start)
        cursor = max(cursor, excluded_end)
    if cursor < len(raw):
        yield from _split_paragraph_range(raw, visible_scan, cursor, len(raw))


def _split_paragraph_range(
    raw: str,
    visible_scan: str,
    start: int,
    end: int,
) -> Iterator[ParsedChunk]:
    position = start
    while position < end:
        while position < end and raw[position].isspace():
            position += 1
        if position >= end:
            return
        blank = _BLANK_LINE_RE.search(raw, position, end)
        stop = end if blank is None else blank.start()
        trimmed = stop
        while trimmed > position and raw[trimmed - 1].isspace():
            trimmed -= 1
        if trimmed > position and _has_visible_content(visible_scan[position:trimmed]):
            yield ParsedChunk(
                raw[position:trimmed],
                span=(position, trimmed),
                chunk_kind="paragraph",
            )
        position = end if blank is None else blank.end()


def _annotate_headings(
    chunks: list[ParsedChunk], headings: list[_Heading]
) -> list[ParsedChunk]:
    ordered_headings = sorted(headings, key=lambda item: item.start)
    active: dict[int, str] = {}
    heading_index = 0
    output: list[ParsedChunk] = []
    for chunk in sorted(chunks, key=lambda item: item.span or (0, 0)):
        start = (chunk.span or (0, 0))[0]
        while (
            heading_index < len(ordered_headings)
            and ordered_headings[heading_index].end <= start
        ):
            heading = ordered_headings[heading_index]
            rank = _HEADING_RANK[heading.level]
            active = {key: value for key, value in active.items() if key < rank}
            if heading.title:
                active[rank] = heading.title
            heading_index += 1
        output.append(
            ParsedChunk(
                text=chunk.text,
                heading_path=[active[key] for key in sorted(active)],
                span=chunk.span,
                extras=dict(chunk.extras),
                chunk_kind=chunk.chunk_kind,
            )
        )
    return output


@dataclass
class LatexStructuralChunker:
    """Extract deterministic, non-overlapping structural chunks from LaTeX."""

    prefers_raw_text: bool = True
    last_warnings: list[str] = field(default_factory=list, init=False)
    last_macro_definitions: list[MacroDefinition] = field(
        default_factory=list, init=False
    )

    def chunk(self, text: str) -> list[ParsedChunk]:
        if not isinstance(text, str):
            raise TypeError(
                f"LatexStructuralChunker.chunk expected str; got {type(text).__name__}"
            )
        self.last_warnings = []
        self.last_macro_definitions = []
        if not text:
            return []

        comment_scan = _mask_comments(text)
        code_spans, code_warnings, code_invalid = _scan_code_environments(comment_scan)
        self.last_warnings.extend(code_warnings)
        scan = _mask_ranges(
            comment_scan,
            [(span.start, span.end) for span in code_spans] + code_invalid,
        )

        macros, macro_warnings, macro_invalid = scan_macro_definitions(
            text, scan_text=scan
        )
        self.last_macro_definitions = macros
        self.last_warnings.extend(macro_warnings)
        macro_ranges = [definition.span for definition in macros] + macro_invalid
        scan = _mask_ranges(scan, macro_ranges)

        environments, env_warnings, env_invalid = _scan_environments(scan)
        self.last_warnings.extend(env_warnings)
        occupied = [(span.start, span.end) for span in environments + code_spans]
        displays, display_warnings, display_invalid = _scan_display_delimiters(
            scan, occupied
        )
        self.last_warnings.extend(display_warnings)
        structural_ranges = occupied + [(span.start, span.end) for span in displays]

        heading_scan = _mask_ranges(
            scan, structural_ranges + env_invalid + display_invalid
        )
        headings, heading_warnings, heading_invalid = _scan_headings(heading_scan, text)
        self.last_warnings.extend(heading_warnings)

        exclusions = (
            structural_ranges
            + macro_ranges
            + code_invalid
            + env_invalid
            + display_invalid
            + heading_invalid
            + [(heading.start, heading.end) for heading in headings]
        )

        document_begin_re = re.compile(r"\\begin\s*\{document\}")
        document_end_re = re.compile(r"\\end\s*\{document\}")
        document_begin = _search_unescaped(document_begin_re, scan)
        if document_begin is not None:
            document_end = _search_unescaped(
                document_end_re, scan, document_begin.end()
            )
            exclusions.append((0, document_begin.end()))
            if document_end is None:
                self.last_warnings.append(
                    f"latex_unbalanced_environment:{document_begin.start()}"
                )
            else:
                exclusions.append((document_end.start(), len(text)))

        atomic: list[ParsedChunk] = [
            ParsedChunk(
                text[span.start : span.end],
                span=(span.start, span.end),
                chunk_kind="code_block",
            )
            for span in code_spans
        ]
        for environment in environments:
            atomic.extend(
                _environment_chunks(
                    text,
                    scan,
                    environment,
                    self.last_warnings,
                    macros,
                )
            )
        atomic.extend(
            ParsedChunk(
                text[span.start : span.end],
                span=(span.start, span.end),
                chunk_kind="equation",
            )
            for span in displays
        )
        atomic.extend(_paragraph_chunks(text, comment_scan, exclusions))

        self.last_warnings = list(dict.fromkeys(self.last_warnings))
        result = _annotate_headings(atomic, headings)
        for chunk in result:
            assert chunk.span is not None
            if not (
                chunk.chunk_kind == "table"
                and chunk.extras.get("table_grid_status") == "parsed"
            ):
                assert chunk.text == text[chunk.span[0] : chunk.span[1]]
        for left, right in zip(result, result[1:]):
            assert left.span is not None and right.span is not None
            assert left.span[1] <= right.span[0]
        return result


__all__ = ["LatexStructuralChunker"]
