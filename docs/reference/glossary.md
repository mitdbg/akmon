---
audience: user, data owner, operator, developer, researcher
type: reference
canonical_id: glossary
section: reference
visibility: primary
status: active
owner: documentation
---
# Glossary

Terms used in AKMON guides, commands, configuration, and code. The first
section covers adding sources and record types and running a deployment;
[Internals](#internals) covers code and advanced features.

## Words you will meet

### Admission

The decision process for a proposed change. Admission can accept it,
reject it, or hold it for review before it enters the working graph. The CLI
and stored status use `quarantine` for a held change.

### AKMON

The application that imports information, manages the OKG knowledge graph,
publishes checked graph versions, and provides search and graph operations.

### Archetype

The basic kind a record type belongs to, such as `artifact`, `actor`,
`process`, or `event`. Record types are written as classes in LinkML, a YAML
schema language; a class becomes a record type only when it declares one, as
`annotations: {archetype: artifact}`.

### Bridge

A deployment's own file of allowed relationships between record types, kept in
`<deployment>/schemas/bridges/*.yaml`. Use it when the rule belongs to one
deployment; a library module uses [cross-links](#cross-links) instead.

### Catalog

The rules that define the record types, relationship types, attributes, and
allowed relationship shapes for a deployment. A published version records the
catalog version used to check it.

### Change probe

A cheap check, run before reading a source, of whether its input changed since
the last import. Its kind, such as `git_scope`, `content_hash`, or
`mutable_api`, names what it compares. `okg deployment lint` rejects most
sources without one; `registry_only` and `live_overlay` sources are exempt.

### Connection route

The way authentication and source data reach AKMON. A route may use credentials
held by AKMON, a signed-in host application, a provider MCP service, a local
gateway, or a read-only command. The route does not define graph-writing rules.

### Connector

A source reader packaged as its own Python package, with a
[registration](#registration). Copy its `schemas/` and `schemas/bridges/` files
into the deployment by hand, then run
`okg deployment add-source <deployment> <name> --registration connector.json`.

### Cross-links

Files in a library module's `cross_links/` directory that allow relationships
between that module's record types and another module's. A rule applies only
when both modules are in the deployment.

### Data source

A file, database, API, service, or other external system that holds information
AKMON may read. A source is not the adapter code that reads it.

### Deployment

One configured AKMON installation, including its database, sources, graph
catalog, publication checks, views, and runtime settings.

### DLQ (dead-letter queue)

Work that stopped after exhausting its allowed retries. DLQ entries require
operator diagnosis; they are not successful or silently discarded tasks.

### Edge archetype

The relationship type named in an allowed-relationship rule, such as
`contains` or `cites`. `contains`-like types define parent and child when the
graph is drawn as a hierarchy.

### Generation

The CLI, API, and database term for an immutable graph checkpoint.
`generation_id` identifies a [published version](#published-version) or a
candidate being checked.

### Import

Reading a connected source and proposing new, changed, or removed records and
relationships. A successful import changes the working graph; it does not make
those changes searchable until publication succeeds.

### Invariant

A health check in a deployment's `invariants.yaml`: an SQL query that returns
the records breaking a rule. Rows from an `error` invariant block the publish.

### MCP

Model Context Protocol. AKMON exposes bounded graph-reading operations through
an MCP server for compatible applications and agents.

### Module

A reusable piece of [ontology](#ontology) in
`src/okg/substrate/library/ontologies/<module>/`: `module.yaml`,
`schema.yaml`, and optional `cross_links/`. A deployment lists the modules it
uses under `modules:`.

### Narrowing

A catalog rule that permits a specific relationship type between specific
record subtypes. A relationship outside the allowed combination is rejected.

### Nomos

AKMON's access-control and policy subsystem. It evaluates an identity, action,
resource, and deployment policy and returns an auditable allow or deny
decision.

### OKG

The knowledge graph managed by AKMON: typed records, relationships, source
evidence, history, graph rules, working branches, and published versions. OKG
is not a separate product.

### Ontology

The record types, attributes, and allowed relationships a deployment uses:
library [modules](#module) plus the deployment's own `schemas/*.yaml` and
[bridges](#bridge), compiled into the [catalog](#catalog).

### Policy bundle

A versioned set of Nomos rules that defines who may perform which actions on
which resources for a deployment.

### Profile

Usually a deployment profile: reusable settings for a domain, such as
`academic-researcher`, chosen with `okg install --profile`. Other commands use
the word differently: `--profile dev|prod` on `okg up` picks a database,
`okg deployment ready --profile` a check set, `okg search --profile` record
types to rank, and `okg run --profile` performance capture.

### Publish cycle

One attempt to turn the working graph into a published version: derive required
relationships, create a fixed candidate, run required checks, and advance the
published-version pointer only if they pass.

### Published version

A stable graph checkpoint that passed its configured publication checks and is
available to ordinary search. The CLI and API identify it with
`generation_id`.

### Reader

Two meanings. In search and publishing guides, a person or service that
searches a published version and does not see working-graph changes. In source
guides and `docs/develop/`, the code that reads one source: a
[connector](#connector) or source adapter, such as `reader.py`.

### Record

A typed item stored in the graph, such as a document, dataset, person, project,
service, issue, or decision. Keyword search reads its `text` attribute, which
`okg_searchable_text_fields` can fill from other fields.

### Registration

A connector's declaration: its type name, settings, credentials, and the record
types it emits. Python code defines it as `REGISTRATION`; `connector.json` is
its saved form.

### Relationship

A typed, meaningful link between two records. A source adapter can import a
relationship from its source, or a relationship builder can derive one from
stored records.

### Source connection

The saved configuration that tells AKMON which source and scope it may read,
which source adapter to use, and how to resolve credentials. Creating a connection
does not import data.

### Subtype

A more specific record or relationship type in the graph catalog. Subtypes
determine allowed attributes and relationship shapes.

### Working graph

Accepted records and relationships that are stored but not yet part of a
published version. Operators can inspect working state; ordinary search uses a
published checkpoint.

## Internals

### Affected frontier

The set of stored derived results that may depend on changed source evidence.
Maintenance recomputes it and its dependents; results outside it are reused
only while their reuse evidence is valid.

### Bitemporal

A model that records both when something was true in its source and when AKMON
stored or changed it, supporting questions about source history and graph
history separately.

### Chronos

AKMON's graph storage model for mutable branches and immutable checkpoints.
The current implementation uses PostgreSQL.

### Complete computation

A recomputation that evaluates the full configured scope instead of reusing a
previous derived result. AKMON uses it when dependency scope, policy, schema,
or reuse evidence is missing or changed; extra work is safer than stale reuse.

### Conflict finding

A derived record that preserves incompatible or competing source assertions,
their evidence, and the policy-relevant comparison. It makes disagreement
queryable; it does not by itself choose which claim is true.

### Contested result

A result for which relevant source assertions remain in disagreement under the
deployment's policy. AKMON keeps the competing evidence; a preferred view may
be selected or publication blocked, but recency or rank alone does not create
certainty.

### DBOS

The workflow runtime used for AKMON's background work. It stores
workflow state so scheduled or queued work can resume after a process restart.

### Derived result

A record or relationship computed from accepted source observations and
recorded rules or evidence. It is working state until a complete candidate
containing it passes publication checks.

### Embedder

Turns text into vectors for semantic search; each vector set names one.

### Enricher

A configured component that derives records or relationships after import.
Some enrichers perform the narrower job of building relationships.

### Fact

AKMON's internal representation of one proposed graph change: insert, update,
or retract a record or relationship.

### FactBatch

A bounded set of facts submitted together with stable identity, source
evidence, and retry information. The import service validates and applies the
batch as one recorded operation.

### Federation contract

The agreed identity, signing, scope, schema, and policy requirements for graph
exchange between two deployments.

### Formal assurance

Evidence from a formal model or checker about a precisely stated property.
Formal assurance applies only to the modeled property and assumptions.

### Incorporation

Applying an accepted exchange package to the receiver's working graph through
the normal import path. Incorporation still requires a later publication.

### Logistike

An internal name for AKMON's DBOS runtime. Guides call it **background work**:
imports, publication, schedules, queues, and maintenance that survive retries
and restarts.

### Mailbox

The receiver-side queue of graph-exchange packages awaiting validation,
admission, or incorporation.

### Maintained knowledge

Knowledge AKMON keeps current as sources change: it recomputes affected derived
results with safe reuse or complete computation, preserves disagreement, and
shows ordinary readers only a checked published version.

### Message board

An exchange status surface that lets deployments report delivery, review, or
incorporation outcomes without changing graph data directly.

### Peer registry

The receiver-side list of other deployments approved for graph exchange,
including their allowed scope and trusted signing keys.

### Poros

AKMON's optional protocol for exchanging signed graph packages between
separately operated deployments. The receiver applies its own access, admission,
and publication rules.

### Redaction

Withholding a field that the receiving deployment is not permitted to ingest
while recording that something was withheld and why.

### Relationship builder

A component that derives relationships from records already in a fixed graph
view. Each derived relationship keeps the rule and evidence that produced it.

### Reuse certificate

Recorded evidence that a previous derived result is still safe to carry
forward for the current source, policy, schema, and computation scope. Missing
or mismatched evidence triggers recomputation instead of silent reuse.

### Source adapter (`SourceAdapter`)

The Python integration that reads one kind of source. It accepts source
settings and a prior position and returns a `SourceRun` containing proposed
records, relationships, evidence, and the next position.

### Source position

The adapter-specific cursor, revision, timestamp, or other value that records
how far an import has progressed. Some code and database fields call this a
*watermark*.

### SourceRunner

The import component that invokes a `SourceAdapter`, records source progress,
and sends its output through the shared validation and graph-write path.

### Write envelope

A producer's declared limit on which record types, relationship shapes, and
values it may emit. Admission rejects well-formed output outside that limit
rather than expanding the producer's authority.
