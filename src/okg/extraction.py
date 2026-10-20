"""Entity extraction protocol and result types.

Core OKG defines only the protocol and data types. Deployment-specific
extractors (e.g., CMSEntityExtractor) live in their deployment directories.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol, runtime_checkable


@dataclass(frozen=True)
class ExtractedEntity:
    """A single entity extracted from text."""

    type: str  # e.g. "site", "jira_issue", "cmssw_release"
    value: str  # the matched text
    start: int  # character offset in original text
    end: int  # character offset end
    confidence: float = 0.9  # base confidence; sources may override


@runtime_checkable
class EntityExtractor(Protocol):
    """Protocol for deployment-specific entity extractors.

    Implementations extract structured entities from free text. The OKG
    pipeline uses the extractor during Phase 3 (Link) to create edges
    from text mentions.

    Deployments declare their extractor in ``deployment.yaml``::

        extractor:
          module: extraction
          class: MyEntityExtractor
    """

    def extract(self, text: str) -> list[ExtractedEntity]:
        """Extract entities from a text string."""
        raise NotImplementedError
