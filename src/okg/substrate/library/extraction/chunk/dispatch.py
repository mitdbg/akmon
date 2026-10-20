"""Name-keyed chunker dispatch.

`chunker_for(name)` returns a registered Chunker FACTORY-
returning instance — same shape as `parse.dispatch.parser_for`.
Bootstrap pre-registers the chunkers shipped in this
package: `window`, `semantic`, `structural_md`,
`structural_html`, `composite`.

The YAML `extractors.yaml` registry continues to load
chunkers by class path; this dispatch is the
runtime-by-name path. No silent CompositeChunker fallback —
unknown names raise structured errors.

Per `add-doc-corpus-extraction-adapter` (2026-04-25).
"""

from __future__ import annotations

from typing import Any, Callable

from okg.substrate.errors import SubstrateError


__all__ = [
    "ChunkerNotFound",
    "available_chunkers",
    "chunker_for",
    "register_chunker",
]


class ChunkerNotFound(SubstrateError):
    code = "chunker_not_found"


# Name → factory returning a Chunker instance. Lazy:
# factories instantiate on demand so importing this module
# doesn't pull every chunker library.
_REGISTRY: dict[str, Callable[..., object]] = {}


def register_chunker(
    name: str, factory: Callable[..., object],
) -> None:
    """Register a factory for `name`. Idempotent: re-
    registering the same name replaces the prior factory."""
    _REGISTRY[name] = factory


def chunker_for(name: str, **kwargs: Any):
    """Return a chunker instance for the named backend.

    Raises `ChunkerNotFound` on unknown name. Any kwargs are
    forwarded to the factory.
    """
    factory = _REGISTRY.get(name)
    if factory is None:
        raise ChunkerNotFound(
            f"no chunker registered for name {name!r}",
            data={"name": name, "available": sorted(_REGISTRY.keys())},
            fix_hint=(
                "register a Chunker via "
                "`okg.substrate.library.extraction.chunk.dispatch."
                "register_chunker` or use one of the available names"
            ),
        )
    return factory(**kwargs)


def available_chunkers() -> list[str]:
    """Return the list of currently-registered chunker names."""
    return sorted(_REGISTRY.keys())


def _bootstrap() -> None:
    """Register the chunkers shipped in this package."""
    from okg.substrate.library.extraction.chunk.composite import (
        CompositeChunker,
    )
    from okg.substrate.library.extraction.chunk.markdown_heading import (
        MarkdownHeadingChunker,
    )
    from okg.substrate.library.extraction.chunk.latex_structural import (
        LatexStructuralChunker,
    )
    from okg.substrate.library.extraction.chunk.semantic import (
        SemanticChunker,
    )
    from okg.substrate.library.extraction.chunk.structural_html import (
        HtmlStructuralChunker,
    )
    from okg.substrate.library.extraction.chunk.structural_markdown import (
        MarkdownStructuralChunker,
    )
    from okg.substrate.library.extraction.chunk.window import (
        WindowChunker,
    )

    # `window` factory needs default args; provide ones that
    # match the proposal's documented chunker chain.
    register_chunker(
        "window",
        lambda max_chars=1500, overlap=200: WindowChunker(
            max_chars=max_chars, overlap=overlap,
        ),
    )
    register_chunker("semantic", SemanticChunker)
    register_chunker(
        "structural_md", MarkdownStructuralChunker,
    )
    # Per `add-heading-aware-chunker` (2026-04-26): the heading-
    # aware chunker normalizes size at the section level. Its
    # default token band (100/1500) lives on the class; deployments
    # can override via the `min_tokens` / `max_tokens` config keys
    # in extractors.yaml.
    register_chunker(
        "markdown_heading",
        lambda min_tokens=100, max_tokens=1500: MarkdownHeadingChunker(
            min_tokens=min_tokens, max_tokens=max_tokens,
        ),
    )
    register_chunker(
        "structural_html", HtmlStructuralChunker,
    )
    register_chunker("latex_structural", LatexStructuralChunker)
    register_chunker("composite", CompositeChunker)


_bootstrap()
