"""MarkdownStructuralEnricher — deterministic Markdown heading anatomy.

Reads `source_file` nodes whose path ends with `.md`/`.markdown`
and emits `MarkdownHeading` nodes + containment edges (source_file
contains heading; heading contains nested heading).

Heading recognition (intentionally conservative):
- ATX-style headings only: `# H1`, `## H2`, …, up to `###### H6`.
- Setext-style (underlined with `=` or `-`) is NOT recognized
  because it conflicts with `---` YAML frontmatter delimiters.
- Code-fenced regions (``` … ```) are skipped so `#` inside code
  blocks doesn't get treated as a heading.

Nesting rule: a heading at depth D is contained by the most-recent
heading at depth < D (if any). Top-level headings (depth 1, or
depth N with no parent) are contained by the source_file.

Pure-function `parse(text, *, repo, path)` is tested standalone.
"""
from __future__ import annotations

import re

from okg.substrate.library.enrichers import DerivedSupportPolicy
from okg.substrate.library.enrichers.code_graph._structural import (
    NonPythonStructuralEnricher,
    ParsedEdge,
    ParsedFile,
    ParsedNode,
    fact_id,
    kebab_slug,
)

EXTRACTOR_NAME = "markdown_structural"
EXTRACTOR_VERSION = "1.0.0"

_ATX_HEADING = re.compile(r"^(#{1,6})\s+(.+?)(?:\s+#+)?\s*$")
_FENCE_OPEN_CLOSE = re.compile(r"^\s*(```|~~~)")


def parse_markdown(
    text: str, *, repo: str, path: str,
) -> ParsedFile:
    """Parse a Markdown source. Returns deterministic ParsedFile."""
    headings: list[tuple[int, str, int, int]] = []
    # ^ (depth, raw_text, line_start_offset, end_of_line_offset)

    in_fence = False
    fence_marker: str | None = None
    offset = 0
    for line in text.splitlines(keepends=True):
        line_len = len(line)
        stripped = line.rstrip("\n")
        # Fence handling: open on a matching marker, close on the
        # SAME marker. Inside a fence, ignore everything (no
        # heading detection).
        m_fence = _FENCE_OPEN_CLOSE.match(stripped)
        if m_fence:
            marker = m_fence.group(1)
            if not in_fence:
                in_fence = True
                fence_marker = marker
            elif marker == fence_marker:
                in_fence = False
                fence_marker = None
            offset += line_len
            continue
        if in_fence:
            offset += line_len
            continue
        m = _ATX_HEADING.match(stripped)
        if m:
            depth = len(m.group(1))
            heading_text = m.group(2).strip()
            headings.append(
                (depth, heading_text, offset, offset + line_len),
            )
        offset += line_len

    if not headings:
        return ParsedFile(nodes=[], edges=[])

    # Build node ids + nesting parents.
    nodes: list[ParsedNode] = []
    edges: list[ParsedEdge] = []
    # Stack of (depth, node_id) for nesting resolution.
    stack: list[tuple[int, str]] = []
    seen_slugs: dict[str, int] = {}

    file_node_id = f"file:{repo}:{path}"

    for depth, raw_text, start, end in headings:
        # Slug; disambiguate duplicates with a numeric suffix.
        base_slug = kebab_slug(raw_text)
        count = seen_slugs.get(base_slug, 0)
        seen_slugs[base_slug] = count + 1
        slug = base_slug if count == 0 else f"{base_slug}-{count}"
        heading_id = (
            f"mdheading:"
            f"{fact_id([repo, path, slug, str(depth)])}"
        )
        nodes.append(ParsedNode(
            node_id=heading_id,
            subtype="markdown_heading",
            attrs={
                "heading_id": heading_id,
                "repo": repo,
                "path": path,
                "depth": depth,
                "text": raw_text,
                "slug": slug,
                "span_start": start,
                "span_end": end,
            },
        ))
        # Resolve parent: pop the stack while top's depth >= this.
        while stack and stack[-1][0] >= depth:
            stack.pop()
        if stack:
            parent_id = stack[-1][1]
            edges.append(ParsedEdge(
                src=parent_id,
                dst=heading_id,
                edge_type="contains",
                attrs={"depth": depth},
            ))
        else:
            edges.append(ParsedEdge(
                src=file_node_id,
                dst=heading_id,
                edge_type="contains",
                attrs={"depth": depth},
            ))
        stack.append((depth, heading_id))

    return ParsedFile(nodes=nodes, edges=edges)


class MarkdownStructuralEnricher(NonPythonStructuralEnricher):
    name = EXTRACTOR_NAME
    # Every parsed node comes from exactly one source file.
    support_policy: DerivedSupportPolicy = "all_members"
    target_subtypes = ("markdown_heading",)
    suffix = ".md"
    extractor_version = EXTRACTOR_VERSION

    def _matches_suffix(self, path: str) -> bool:
        # Override the SQL LIKE filter to also accept .markdown.
        return path.endswith(".md") or path.endswith(".markdown")

    def parse(
        self, text: str, *, repo: str, path: str,
    ) -> ParsedFile:
        return parse_markdown(text, repo=repo, path=path)
