"""Re-export of the substrate's existing RegexEntityExtractor.

The canonical home stays at
`okg.substrate.projection.extractor.RegexEntityExtractor`; this
module re-exports it under the extraction-library namespace so
deployment code can pick its NER backends from one tree without
chasing imports.
"""

from okg.substrate.projection.extractor import (
    RegexEntityExtractor as RegexEntityExtractor,  # noqa: F401
)


__all__ = ["RegexEntityExtractor"]
