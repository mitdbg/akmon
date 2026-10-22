---
audience: user, developer, operator
type: reference
canonical_id: built-in-capabilities
section: reference
visibility: primary
status: active
owner: documentation
---
# Built-in capabilities

Actual availability depends on the deployment, source authority, credentials,
graph model, and publication checks.

## Status summary

| Capability | Status | What that means |
| --- | --- | --- |
| Build a graph from your own data and query it from an agent | Available | The [Quickstart](../start/quickstart.md) path: install, add sources, connect an MCP agent, get a cited answer. |
| Run an operator-managed deployment | Available | Deployment configuration, import, publication, status, metrics, and recovery commands. |
| Share one read server with several agents over HTTP | Available | A token-protected HTTP MCP server; see [Connect an agent](../use/agents/index.md#share-one-server-over-http). |
| Give each person their own access | Requires configuration | Multi-user HTTP with per-person access rules, plus an identity provider or a sign-in proxy; see [Multi-user HTTP principals](mcp.md#multi-user-http-principals). |
| Ask a live system at read time | Requires configuration | [Live relations](../operate/live-relations.md) answer from a declared external system beside the graph, without importing it. |
| Import local files and structured records | Available | Built-in adapters cover common document, record, repository, and report formats. Each source still needs an explicit scope. |
| Import public scholarly metadata | Requires configuration | Adapters exist for sources including ORCID, DBLP, OpenAlex, Semantic Scholar, arXiv, OpenReview, and INSPIRE. Requires network access and source-specific identifiers. |
| Import private mail, calendar, chat, issue, or course data | Requires configuration | Adapters or host/MCP routes exist for selected services. A deployment must supply credentials, allowlists, and policy. No account is connected by default. |
| Build relationships and extracted structure | Requires configuration | Exact identifiers, aliases, document structure, code structure, and deployment rules can produce evidenced relationships. Model-assisted processing requires an explicit backend and review policy. |
| Publish checked graph versions | Available | Publication runs the configured builders and checks, then advances the reader-visible version atomically or leaves the previous version active. |
| Search and follow relationships | Available | Lexical search, inspection, traversal, filtering, mapping, aggregation, and bounded query recipes are available through the CLI and MCP read interface. |
| Semantic search and embeddings | Requires configuration | A deployment declares vector sets; a backfill or the worker fills them. |
| Incremental claim maintenance and conflict handling | Experimental | Runtime support is limited to an unchanged-state development fixture. |
| General incremental recomputation | Not provided | Normal deployments use the complete publication path. Incremental reuse is not wired across arbitrary builders, views, rollups, and embeddings. |
| Packaged release | Available | Each release attaches a Python wheel to its GitHub release and pushes container images and a Helm chart. The repository is public at github.com/mitdbg/akmon. |
| Hosted service | Not provided | You run AKMON yourself, against a PostgreSQL database you control. |

“Available” means a tested path exists in this repository. It does not mean
every profile enables it or that AKMON has authority to access a
particular account.

## Source families

AKMON source adapters translate external data into records, relationships, and
evidence. Implemented families:

| Family | Examples | Typical requirements |
| --- | --- | --- |
| Files and documents | Markdown, text, JSON and YAML documents, and PDF text (text layer only, one passage per page; no model call with the default settings), through `add-source`; CSV, JSON, JSON Lines and YAML as mapped records through `add-source --manifest`. HTML, TOML, XLSX, DOCX, notebooks, PDF tables, and LaTeX through a profile or a Python connector, not `add-source` | Approved paths and format-specific settings |
| Git and code | Commit history, tracked files, symbols, imports, package manifests, interfaces, data flow, SARIF, test and profiling reports | Repository path, branch or revision scope, optional analysis tools |
| Scholarly records | BibTeX, ORCID, DBLP, OpenAlex, Semantic Scholar, arXiv, OpenReview, INSPIRE | Public identifiers, API settings, local paper paths, optional credentials. Local paper drafts need a profile or a Python connector, not `add-source`; a folder of PDFs is read as documents by `add-source` (see the files row) |
| Research operations | Experiments, benchmark output, cluster jobs, grants, talks, courses, CV and lab-webpage records | Profile or deployment-specific configuration |
| Collaboration systems | GitHub issues and pull requests; Slack, Gmail, Outlook, Google Calendar, Apple Calendar, and Canvas through the `academic-researcher` profile | Credentials or a host/MCP connection route, plus explicit allowlists |
| AKMON development history | PACT changes, agent sessions, test reports, CI results, security analysis, branch dispositions | Repository or artifact paths; mainly used by Workspace Graph deployments |
| Relational and external systems | Selected database schemas, externally supplied records, signed graph packages | Connection authority, schema mapping, receiver policy |

Not every family can be added with one command. `okg deployment source-types`
lists the types `okg deployment add-source` adds: `local-folder` and `github`.
A profile sets up others, and a packaged connector is added with
`--registration`. Jira and Confluence readers exist only in the legacy
`devops` and `docs-corpus` templates (`okg init --template <name>`). Treat
them as sample-only: the templates wire them to bundled sample files, and no
profile or `add-source` route sets up a live instance.

A folder source added with `add-source` reads Markdown, text, JSON, YAML and
PDF text (plus images, when captioning is on). It reads a PDF's text layer
only, one passage per page; a scanned PDF has none, and the preview warns about
it. With the default `extractors.yaml` and embeddings off, reading PDFs makes no
model call; captioning, table transcription and embeddings are the opt-ins that do. A matched file of another type, such as
a Word file, becomes a record holding only its file name; the preview and the
apply result warn about it. If no matched file can be read, `add-source`
refuses.

Most adapters support narrow fixture or local-file modes for development.
Live service adapters require explicit account and scope configuration
instead of scanning an entire account by default.

Domain connectors live in companion repositories such as
[archi-okg](https://github.com/archi-physics/archi-okg) (CERN); see its
[quickstart](https://github.com/archi-physics/archi-okg/blob/main/docs/cern-team-quickstart.md)
for the readers it adds and which need an account.
The Jira reader above is the sample-only one; the archi-okg JIRA reader is
separate.

Use [Connect your data](../use/sources/index.md) for the source
workflow. Built-in profile defaults are in [Choose a profile](../use/profiles/index.md).

## Connection routes

A deployment may read a service directly through a source adapter or receive a
normalized batch from another trusted process.

| Route | Authentication owner |
| --- | --- |
| Built-in adapter | The AKMON deployment resolves a credential reference and calls the source. |
| Host connection | The application hosting AKMON owns sign-in and sends a bounded batch. |
| Provider or gateway MCP service | The selected MCP service owns the source connection and exposes approved data. |
| Local read-only tool | A pinned local service or command reads within an approved scope. |

Every route must identify what it may read, where credentials live, how access
is revoked, and what evidence accompanies imported records. A route name alone
is not a privacy or readiness guarantee.

## Extraction and relationships

Built-in processing includes:

- document parsing and structural chunking;
- DOI, ORCID, and ISBN validation;
- exact, path, alias, and configured cross-link rules;
- code, interface, test, data-flow, and security-evidence linking;
- Markdown, configuration, PDF, and LaTeX structural extraction;
- optional named-entity, claim, concept, hypothesis, and relation extraction;
- optional embeddings and semantic concept linking; and
- deployment-defined invariant checks and native read views.

A source-provided or derived relationship must use a relationship shape allowed
by the deployment and retain evidence for how it was produced. Similar text is
not automatically presented as a statement made by the source.

Developers can [add a source adapter](../develop/index.md#add-a-source-adapter),
[extend the graph model](../develop/extensions/ontologies.md), or configure
additional builders through the deployment extension points.

## Publication and reading

During publication, AKMON can:

- assemble a stable candidate from working state;
- run configured relationship builders and enrichers;
- enforce the graph catalog and deployment invariants;
- create generation-scoped views and artifacts;
- publish a new immutable graph version; and
- record counts, failed checks, and generation metadata.

The read path supports bounded inspection, lexical search, relationship
expansion, filtering, mapping, aggregation, and query recipes. Reads resolve
authority and one graph generation before returning records. Continuation
state remains tied to that generation.

See [Query and trace the graph](../use/search/index.md), the [CLI reference](cli.md),
and the [MCP reference](mcp.md).

## Experimental incremental claim maintenance

The consistency library can represent claims, ordered corrections,
dependencies, conflict findings, and contested or policy-selected results. It
also holds logic for deciding whether prior work has enough evidence to be
reused.

The only runtime path is the opt-in `consistency-fixture`. It runs after
publication, evaluates one unchanged-state claim-resolution case, and does not
cover arbitrary relationship builders, rollups, views, or embeddings. Its
failure does not invalidate the graph version that already published.

Normal deployments construct and check the complete candidate. See
[Incremental maintenance and claim consistency](../develop/architecture/consistency.md)
for the developer contract.

## Implementation map

| Area | Code |
| --- | --- |
| Source adapters | `src/okg/substrate/library/sources/` |
| Parsing and extraction | `src/okg/substrate/library/extraction/` |
| Relationship builders and enrichers | `src/okg/substrate/library/linkers/`, `src/okg/substrate/library/enrichers/`, `src/okg/substrate/enrichers/` |
| Embeddings | `src/okg/substrate/embeddings/` |
| Claim consistency | `src/okg/substrate/consistency/` |
| Publication checks and views | `src/okg/substrate/publisher/`, `src/okg/substrate/catalog/` |
| MCP operators | `src/okg/substrate/mcp/` |

The [code architecture](../develop/architecture/pipeline.md) explains how these
packages participate in one import and publication cycle.
