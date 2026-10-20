"""Text normalization — Unicode + markup-strip helpers.

Plain functions, not a Protocol. ftfy + unicodedata cover Unicode
fixing; lxml + markdown_it strip HTML / Markdown to body text.
"""

from __future__ import annotations

import unicodedata
from typing import Literal


def unicode_clean(text: str, *, form: Literal["NFC", "NFKC"] = "NFC") -> str:
    """Repair mojibake via ftfy and apply Unicode normalization.

    ftfy.fix_text undoes the common UTF-8/CP1252 round-trip
    breakage; unicodedata.normalize then canonicalizes
    composed/decomposed forms.
    """
    if not isinstance(text, str):
        raise TypeError(f"unicode_clean expected str; got {type(text).__name__}")
    if not text:
        return ""
    import ftfy
    fixed = ftfy.fix_text(text)
    return unicodedata.normalize(form, fixed)


def strip_html(html: str) -> str:
    """Strip HTML tags + script/style content; return body text.

    Preserves whitespace structure imperfectly — collapses runs
    of whitespace, drops leading/trailing.
    """
    if not isinstance(html, str):
        raise TypeError(f"strip_html expected str; got {type(html).__name__}")
    if not html:
        return ""
    from lxml import html as _html
    tree = _html.fromstring(html)
    for tag in ("script", "style", "noscript"):
        for el in tree.iter(tag):
            el.drop_tree()
    text = tree.text_content() or ""
    import re as _re
    return _re.sub(r"\s+", " ", text).strip()


def strip_markdown(md_text: str) -> str:
    """Render Markdown to plain text by parsing + concatenating
    inline tokens — strips heading markers, list markers, and
    code-fence delimiters.
    """
    if not isinstance(md_text, str):
        raise TypeError(
            f"strip_markdown expected str; got {type(md_text).__name__}"
        )
    if not md_text:
        return ""
    from markdown_it import MarkdownIt
    md = MarkdownIt("commonmark")
    tokens = md.parse(md_text)
    parts: list[str] = []
    for tok in tokens:
        if tok.type == "inline":
            parts.append(tok.content)
        elif tok.type == "code_block" or tok.type == "fence":
            parts.append(tok.content)
    return "\n".join(parts).strip()


def casefold_for_match(text: str) -> str:
    """Aggressive case-fold for case-insensitive matching."""
    return text.casefold() if isinstance(text, str) else ""


__all__ = [
    "casefold_for_match",
    "strip_html",
    "strip_markdown",
    "unicode_clean",
]
