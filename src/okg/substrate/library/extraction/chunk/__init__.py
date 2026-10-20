"""Document chunkers + composite-with-empty-fallthrough.

Backends (each module wraps ≤1 third-party package — wrap invariant):
  - WindowChunker (stdlib only) — character-window with overlap.
  - SemanticChunker (semantic-text-splitter) — sentence-aware.
  - MarkdownStructuralChunker (markdown-it-py) — paragraph-bounded
    under each heading; preserves heading_path. Optimized for
    fine-grained content-hash dedup.
  - MarkdownHeadingChunker (markdown-it-py) — section-bounded with
    size normalization (merge undersized, split oversized at
    paragraph boundaries). Optimized for agent-friendly retrieval.
    Per `add-heading-aware-chunker` (2026-04-26).
  - HtmlStructuralChunker (lxml) — by HTML <h*>.

Composition: CompositeChunker tries each in order, returns the
first non-empty result. Empty IS the fallthrough trigger
(well-defined, unlike "first that succeeds").

Units: characters (Python `str`, Unicode code points). Bytes are
not supported.
"""

from okg.substrate.library.extraction.chunk.composite import (
    CompositeChunker,
)
from okg.substrate.library.extraction.chunk.markdown_heading import (
    MarkdownHeadingChunker,
)
from okg.substrate.library.extraction.chunk.latex_structural import (
    LatexStructuralChunker,
)
from okg.substrate.library.extraction.chunk.semantic import SemanticChunker
from okg.substrate.library.extraction.chunk.structural_html import (
    HtmlStructuralChunker,
)
from okg.substrate.library.extraction.chunk.structural_markdown import (
    MarkdownStructuralChunker,
)
from okg.substrate.library.extraction.chunk.window import WindowChunker


__all__ = [
    "CompositeChunker",
    "HtmlStructuralChunker",
    "LatexStructuralChunker",
    "MarkdownHeadingChunker",
    "MarkdownStructuralChunker",
    "SemanticChunker",
    "WindowChunker",
]
