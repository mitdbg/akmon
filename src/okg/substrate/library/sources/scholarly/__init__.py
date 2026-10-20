"""Substrate-shipped scholarly source adapters.

Lifted from `deployments/memex/sources/` per
`lift-scholarly-sources-to-substrate-library` (Wave A child of
the `expand-memex-to-personal-okg` umbrella).

Adapters in this package:

  - `arxiv.ArxivSource`                  — arXiv API metadata
  - `dblp.DblpSource`                    — DBLP search API
  - `inspire_hep.InspireHepSource`       — INSPIRE-HEP REST API
  - `openalex.OpenAlexSource`            — OpenAlex Works API
  - `openreview.OpenReviewSource`        — OpenReview API v2
  - `semantic_scholar.SemanticScholarSource`
                                          — Semantic Scholar Graph
  - `scholarly_identity_aliases.IdentityAliasSource`
                                          — Cross-adapter identity
                                            alias emission

A4 additions (`add-substrate-bibliographic-sources`):

  - `bib_references.BibReferencesSource`  — read .bib files; emit
                                            Paper(kind=foundational),
                                            Person, Venue
  - `talks.TalksSource`                   — read talks YAML +
                                            YouTube transcripts;
                                            emit Paper(kind=talk),
                                            Person, DocumentChunk
  - `author_aliases.AuthorAliasesSource`  — operator-curated YAML
                                            alias entries → substrate
                                            alias table
  - `tier_promotion`                      — Tier 1/2/3 emission
                                            convention + promotion
                                            config loader + soft
                                            A1/A2 extraction hooks

All adapters are designed to compose into a deployment via its
`source_registry.yaml`. They emit `NodeFact` / `EdgeFact` records
per the substrate source protocol; downstream behavior (Phase A
projection, narrowing, publish-gate) is unchanged from pre-lift.

Helpers under this package:

  - `_common.slugify`                    — kebab-case slug
  - `network_cache`                      — `CachedNetworkPaperSource`
                                            base class +
                                            `SourceResponseCache`,
                                            `RateLimiter`,
                                            `NetworkRequest`

Pre-lift cache directories (`<deployment>/.cache/paper_sources/<provider>/`)
remain valid post-lift; the adapters read them via the registry-
supplied `cache_root` parameter.

Paper-anatomy sources added per
`extend-substrate-scholarly-with-paper-anatomy` (umbrella
`evolve-memex-to-ai-scientist-substrate` A1):

  - `paper_pdf_source.PaperPdfSource`     — inbox-walk → Paper
                                            (kind=research, tier=1)
                                            + invokes pdf_structural
  - `our_drafts_source.OurDraftsSource`   — `.tex`/`.md` globs → Paper
                                            (kind=our-draft)
                                            + invokes latex_structural

Both accept paths as parameters; the substrate hardcodes no
operator path.

Imports are lazy: PaperPdfSource and OurDraftsSource are loaded
via source_registry.yaml's `module: <path>` / `class: <name>`
strings (the substrate's source-loader does importlib.import_module
+ getattr). Eager imports here trigger a circular-import deadlock
with paper_pdf_source's own scholarly references (observed during
B1 implementation verification).
"""

__all__: list[str] = []
