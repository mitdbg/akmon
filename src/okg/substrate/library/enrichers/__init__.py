"""Substrate-shipped enrichers (deployment-agnostic).

Today this package contains:

- ``concept_extractor.ConceptExtractor`` — emits
  ``<source_subtype> -mentions-> Concept`` edges using the
  ``okg.find_concept_mentions`` SRF (per
  ``add-substrate-concept-extraction``).

Substrate-shipped linkers continue to live under
``okg.substrate.library.linkers``.
"""

from okg.substrate.derived_node_liveness import DerivedSupportPolicy

#: Re-exported so a deployment-authored enricher declares its node liveness
#: policy without importing a substrate-internal module:
#:
#:     from okg.substrate.library.enrichers import DerivedSupportPolicy
#:
#:     class MyExtractor(LLMExtractor):
#:         support_policy: DerivedSupportPolicy = "all_members"
__all__ = ["DerivedSupportPolicy"]
