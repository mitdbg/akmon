"""Format-specific document parsers + format-keyed dispatch.

Each backend wraps exactly one third-party library and exports
exactly one class implementing the `Parser` Protocol. The
wrap-invariant is enforced by `okg doctor --check
extraction-wrap-invariant`.

Use `parser_for(format)` for explicit dispatch. There is NO
`CompositeParser` that tries each parser until one "succeeds" —
silent fallback on `pypdf.parse` returning degraded text from a
`.docx` is a P0 anti-pattern this proposal explicitly avoids.
"""

from okg.substrate.library.extraction.parse.dispatch import (
    ParserNotFound,
    parser_for,
    register_parser,
    sniff_format,
)


__all__ = [
    "ParserNotFound",
    "parser_for",
    "register_parser",
    "sniff_format",
]
