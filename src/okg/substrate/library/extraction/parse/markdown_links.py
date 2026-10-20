"""Inline-link extractor for markdown text.

Walks the `markdown-it-py` AST for `link_open` tokens; returns
the (href, link_text) pairs as `LinkRef` records. Path
resolution and known-doc filtering live in the doc-corpus
source — this module only does the AST walk so it stays
context-free and unit-testable on synthetic input.

External URLs (`http://`, `https://`, `mailto:`),
anchor-only hrefs (`#section`), and empty hrefs are returned
as-is; the caller decides what to drop. Reference-style links
(`[text][ref]`) and image links (`![alt](src)`) are NOT
emitted — only inline anchor links.

Companion helper `resolve_link_target(href, source_rel_path)`
canonicalizes an href against the source doc's repo-relative
path, strips the anchor + query, and refuses external schemes
— returning `(resolved_path, anchor)` or `(None, None)` for
unresolvable hrefs.

Per `add-markdown-link-extraction` (2026-04-27).

Wraps `markdown-it-py` only — wrap invariant holds.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class LinkRef:
    """One inline link in a markdown body.

    `href` is the raw URL/path as written in the source. The
    consumer resolves it against the source doc's path. `text`
    is the anchor's visible label; useful as an edge attr for
    "what this chunk says about the target".
    """

    href: str
    text: str


def extract_links(text: str) -> list[LinkRef]:
    """Return every inline link in `text` as a LinkRef.

    Empty input returns []. Order matches document order.
    Duplicate (href, text) pairs are returned multiple times —
    deduplication is the caller's job (the doc-corpus source
    de-dups by (chunk_id, target_doc_id) before edge emission).
    """
    if not text:
        return []
    from markdown_it import MarkdownIt
    md = MarkdownIt("commonmark")
    tokens = md.parse(text)

    out: list[LinkRef] = []
    for tok in tokens:
        # Inline tokens carry the actual link_open / link_close
        # children — top-level tokens are heading_open,
        # paragraph_open, etc. The inline token's `children`
        # list holds the inline-level AST.
        if tok.type != "inline" or not tok.children:
            continue
        i = 0
        children = tok.children
        while i < len(children):
            child = children[i]
            if child.type != "link_open":
                i += 1
                continue
            href = ""
            for attr_name, attr_val in (child.attrs or {}).items():
                if attr_name == "href":
                    href = attr_val
                    break
            # Walk forward until matching link_close, collecting
            # text content. Nested formatting (em, strong) emits
            # `text` children we concatenate; nested code emits
            # a `code_inline` whose `content` is the verbatim
            # code.
            j = i + 1
            text_parts: list[str] = []
            while j < len(children) and children[j].type != "link_close":
                ch = children[j]
                if ch.type == "text":
                    text_parts.append(ch.content)
                elif ch.type == "code_inline":
                    text_parts.append(ch.content)
                # Other inline tokens (em_open/_close, etc.)
                # contribute via their text children.
                j += 1
            link_text = "".join(text_parts).strip()
            out.append(LinkRef(href=href, text=link_text))
            i = j + 1  # skip past the matched link_close
    return out


def resolve_link_target(
    href: str, source_rel_path: str,
) -> tuple[Optional[str], Optional[str]]:
    """Resolve an inline link's `href` to a repo-relative
    target path.

    Inputs:
      - `href` is the raw URL/path as written in markdown.
      - `source_rel_path` is the source doc's repo-relative
        path (e.g. `docs/runbooks/troubleshooting/X.md`).

    Returns `(resolved_rel_path, anchor)` where:
      - `resolved_rel_path` is the canonical repo-relative
        target path (e.g. `docs/reference/glossary.md`), or `None` if
        the href is external / anchor-only / empty.
      - `anchor` is the `#fragment` portion (without the `#`),
        or `None` if there is no fragment.

    External schemes (`http://`, `https://`, `mailto:`,
    `ftp://`, `data:`) and anchor-only hrefs return
    `(None, None)`. Query strings are stripped. `..` and `.`
    segments are normalized. Hrefs starting with `/` are
    treated as repo-root-relative.

    The caller decides whether the resolved path corresponds
    to a known doc — this function only canonicalizes.
    """
    if not href:
        return None, None
    h = href.strip()
    if not h:
        return None, None

    # External schemes — refused at the resolver. Adding new
    # schemes is a follow-up (e.g. `entity_mention` for URLs).
    lower = h.lower()
    for scheme in (
        "http://", "https://", "mailto:", "ftp://", "data:", "tel:",
        "javascript:",
    ):
        if lower.startswith(scheme):
            return None, None

    # Standard URL grammar: `path?query#fragment`. Split off
    # the fragment FIRST (it's the trailing component), then
    # strip the query from what's left of the path.
    anchor: Optional[str] = None
    if "#" in h:
        h, _, frag = h.partition("#")
        anchor = frag or None

    if "?" in h:
        h = h.split("?", 1)[0]

    if not h:
        # Anchor-only — `[text](#section)` — same-doc nav.
        return None, None

    # Repo-root-relative href (`/docs/X.md`) — strip leading
    # slash and treat as repo-relative.
    if h.startswith("/"):
        resolved = h.lstrip("/")
    else:
        # Relative to the source doc's directory.
        source_dir = os.path.dirname(source_rel_path)
        resolved = os.path.normpath(os.path.join(source_dir, h))

    # Normalize separators to forward-slash for cross-OS
    # consistency (the repo's source-of-truth path style).
    resolved = resolved.replace(os.sep, "/")

    # `os.path.normpath` of `../..` may produce `..` if it
    # walks above the repo root. That's not a valid target.
    if resolved.startswith("../") or resolved == "..":
        return None, None

    return resolved, anchor


__all__ = ["LinkRef", "extract_links", "resolve_link_target"]
