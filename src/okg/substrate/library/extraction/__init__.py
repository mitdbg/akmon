"""Substrate-side extraction library.

Houses Python-only operations the substrate's SQL primitives
can't perform: NER, document parsing, sentence-aware chunking,
identifier checksum validation, text normalization, off-substrate
fuzzy matching.

Three Protocols (each with ≥ 2 backends):
  - Parser   (13 backends + format-keyed dispatch — one third-party
              package per backend module; wrap invariant holds
              structurally)
  - Chunker  (4 backends + composite)
  - EntityExtractor (3 backends + composite; Protocol at okg.extraction)

Single-backend operations ship as plain functions:
  - normalize.unicode_clean / strip_html / strip_markdown
  - validators.{doi,orcid,isbn} (shipped in M6.7.5)
  - linking.sql_fuzzy_resolve / local_fuzzy_resolve

Per-deployment configuration via `extractors.yaml` →
`registry.load_extractors`.
"""

from okg.substrate.library.extraction._protocols import (
    BibEntry,
    Chunker,
    EntityExtractor,
    ExtractedEntity,
    InlineCitation,
    Parser,
    ParsedChunk,
    ParsedDocument,
)
from okg.substrate.library.extraction.chunk import (
    CompositeChunker,
    HtmlStructuralChunker,
    MarkdownStructuralChunker,
    SemanticChunker,
    WindowChunker,
)
from okg.substrate.library.extraction.ner import (
    CompositeNER,
    GLiNERBackend,
    RegexEntityExtractor,
    SpacyNER,
)
from okg.substrate.library.extraction.chunk.dispatch import (
    ChunkerNotFound,
    available_chunkers,
    chunker_for,
    register_chunker,
)
from okg.substrate.library.extraction.ner.dispatch import (
    NerBackendNotFound,
    available_ner_backends,
    ner_for,
    register_ner,
)
from okg.substrate.library.extraction.parse import (
    ParserNotFound,
    parser_for,
    register_parser,
    sniff_format,
)
from okg.substrate.library.extraction.parse.json_struct import (
    ParseFailed,
)
from okg.substrate.library.extraction.registry import (
    ExtractorAxisMissing,
    ExtractorBundle,
    ExtractorChainMissing,
    load_extractors,
)
from okg.substrate.library.extraction.table_grid import (
    TABLE_GRID_SCHEMA_VERSION,
    TABLE_GRID_STATUSES,
    TableCellV1,
    TableExtractionResult,
    TableGridError,
    TableGridV1,
    TableRowV1,
    canonical_pdf_bbox,
    latex_table_locator,
    normalize_table_text,
    pdf_table_locator,
    table_occurrence_id,
    validate_table_grid,
)
from okg.substrate.library.extraction.validators import (
    ValidatorNotFound,
    available_validators,
    register_validator,
    validator_for,
)


__all__ = [
    # Protocols + dataclasses
    "BibEntry",
    "Chunker",
    "EntityExtractor",
    "ExtractedEntity",
    "InlineCitation",
    "Parser",
    "ParsedChunk",
    "ParsedDocument",
    "TABLE_GRID_SCHEMA_VERSION",
    "TABLE_GRID_STATUSES",
    "TableCellV1",
    "TableExtractionResult",
    "TableGridError",
    "TableGridV1",
    "TableRowV1",
    # Chunkers
    "CompositeChunker",
    "HtmlStructuralChunker",
    "MarkdownStructuralChunker",
    "SemanticChunker",
    "WindowChunker",
    # NER
    "CompositeNER",
    "GLiNERBackend",
    "RegexEntityExtractor",
    "SpacyNER",
    # Parsing
    "ParseFailed",
    "ParserNotFound",
    "parser_for",
    "register_parser",
    "sniff_format",
    # Chunker / NER / Validator dispatch (Stride A)
    "ChunkerNotFound",
    "NerBackendNotFound",
    "ValidatorNotFound",
    "available_chunkers",
    "available_ner_backends",
    "available_validators",
    "chunker_for",
    "ner_for",
    "register_chunker",
    "register_ner",
    "register_validator",
    "validator_for",
    # Registry
    "ExtractorAxisMissing",
    "ExtractorBundle",
    "ExtractorChainMissing",
    "load_extractors",
    # Structured table contract
    "canonical_pdf_bbox",
    "latex_table_locator",
    "normalize_table_text",
    "pdf_table_locator",
    "table_occurrence_id",
    "validate_table_grid",
]
