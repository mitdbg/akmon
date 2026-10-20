"""Format-keyed parser dispatch.

`parser_for(format)` returns the registered parser class for the
named format. Raises `ParserNotFound` on unknown format. The
caller is responsible for knowing the format — `sniff_format` is
a separate helper that is NOT auto-called by `parser_for`.

This is deliberately not a `CompositeParser` that tries-each-
parser-until-one-succeeds. Silent fallback (e.g. `pypdf` not
crashing on a `.docx` and returning garbage text) is a P0 in
this codebase.
"""

from __future__ import annotations

from typing import Callable, Optional

from okg.substrate.errors import SubstrateError


class ParserNotFound(SubstrateError):
    code = "parser_not_found"


# Format string → factory returning a Parser-conforming instance.
# Lazy: factories instantiate on demand so importing the dispatch
# module doesn't pull every parser library.
_REGISTRY: dict[str, Callable[[], object]] = {}


def register_parser(format: str, factory: Callable[[], object]) -> None:
    """Register a factory for `format`. Idempotent: re-registering
    the same format replaces the prior factory."""
    _REGISTRY[format] = factory


def parser_for(format: str):
    """Return a parser instance for the named format. Raise
    `ParserNotFound` on unknown format."""
    factory = _REGISTRY.get(format)
    if factory is None:
        raise ParserNotFound(
            f"no parser registered for format {format!r}",
            data={
                "format": format,
                "available": sorted(_REGISTRY.keys()),
            },
            fix_hint=(
                "register a Parser via "
                "`okg.substrate.library.extraction.parse.register_parser` "
                "or use one of the available formats"
            ),
        )
    return factory()


# Magic-byte sniffer. Separate from parser_for so the caller's
# decision to sniff the format is explicit in their code.
def sniff_format(content: bytes) -> Optional[str]:
    """Return a best-guess format string from magic bytes, or
    None if no signature matches.

    Caller must opt in to using this by calling it explicitly
    and passing the result to `parser_for`.
    """
    if not isinstance(content, (bytes, bytearray)):
        return None
    # Take 16 bytes to fit common signatures (e.g. `<!DOCTYPE html`).
    head = bytes(content[:16])
    if head.startswith(b"%PDF-"):
        return "pdf"
    if head.startswith(b"PK\x03\x04"):
        # ZIP-based: docx, xlsx, .ipynb (sometimes), .epub, etc.
        # Caller can disambiguate further via filename or by
        # peeking at the [Content_Types].xml inside.
        return "zip"
    head_lower = head.lower()
    if head_lower.startswith(b"<!doctype") or head_lower.startswith(b"<html"):
        return "html"
    if head.startswith(b"{\n") or head.startswith(b"{\r") or head.startswith(b"{\""):
        # heuristic only; may also be a JSON file
        return "json"
    return None


# Bootstrap registrations. Each module's import has the side
# effect of calling `register_parser`; we collect them here so the
# dispatch table is populated by the time any caller imports
# `parse.parser_for`. Each module wraps ≤1 third-party top-level
# package so the wrap-invariant lint passes structurally on the
# shipped tree.
def _bootstrap() -> None:
    from okg.substrate.library.extraction.parse import (  # noqa: F401
        code_python,
        csv as csv_mod,
        docx,
        email as email_mod,
        frontmatter,
        html_fast,
        html_lxml,
        json_struct,
        latex,
        markdown,
        notebook,
        pdf_tables,
        pdf_text,
        text,
        toml_struct,
        xlsx,
        yaml_struct,
    )


_bootstrap()
