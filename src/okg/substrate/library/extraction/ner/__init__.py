"""NER backends + CompositeNER (span-overlap dedupe).

Three backends:
  - SpacyNER       (wraps spaCy)
  - GLiNERBackend  (wraps GLiNER, zero-shot)
  - RegexEntityExtractor (re-export of okg.substrate.projection.extractor)

CompositeNER fans out + dedupes via span-overlap + canonical
type. See docstring on `CompositeNER` for the rule.
"""

from okg.substrate.library.extraction.ner.composite import CompositeNER
from okg.substrate.library.extraction.ner.gliner_backend import GLiNERBackend
from okg.substrate.library.extraction.ner.regex_backend import (
    RegexEntityExtractor,
)
from okg.substrate.library.extraction.ner.spacy_backend import (
    ModelNotInstalled,
    SpacyNER,
)


__all__ = [
    "CompositeNER",
    "GLiNERBackend",
    "ModelNotInstalled",
    "RegexEntityExtractor",
    "SpacyNER",
]
