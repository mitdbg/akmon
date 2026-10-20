---
audience: user, data owner, operator, developer
type: explanation
canonical_id: akmon-model
section: get-started
visibility: primary
status: active
owner: documentation
---
# The AKMON model

AKMON builds a searchable knowledge graph from data you already have.
The data can be a Git repository, a folder of documents, or a project's
issues, and every result records where it came from. It is installed as the Python package `okg`, and
its command is also `okg`. This page explains the few ideas behind every
command.

To see them working before you read further, follow
[Evaluate AKMON from source](evaluate-from-source.md). It builds a throwaway
graph from two sample records, publishes it, and searches it. You need Git,
`uv`, and Docker.

## The flow in one picture

```text
source            the data a deployment may read
  → import        read it; propose new, changed, or removed records
  → admission     accept, reject with a reason, or hold for review
  → working graph accepted changes, not yet visible to search
  → publish cycle build a complete candidate and run its checks
  → published version (generation), what every search reads
```

The rest of this page defines each step. The
[glossary](../reference/glossary.md) has every other term.

## Deployment

A **deployment** is one configured graph. It has its own PostgreSQL database
and defines:

- which sources it may read;
- which kinds of records and relationships it accepts (its **catalog**);
- which checks a new version must pass before it is published; and
- who may read or change it (its **policy**).

Two deployments can run the same software over different data and different
rules. Most commands name the deployment they act on with `--deployment`.

## Source and source adapter

A **source** is the bounded data a deployment has been allowed to read: for
example, one folder, one repository, or one repository's issues and pull
requests.

**Connecting** a source saves its definition: the kind of source, its scope,
and how to find its credentials. **Importing** reads it. Keeping the two apart
lets you review the scope before any data enters the graph.
[Connect your data](../use/sources/index.md) previews a source first; with
`--apply` it connects, imports, and publishes in one step.

A **source adapter** is the code that reads one kind of source and turns its
data into records and relationships. An adapter can declare which record and
relationship types it may write. When it does, admission rejects anything
outside that declaration instead of widening what the adapter may do. A
source with no declaration is admitted with a `producer_signature_missing`
warning.

## Record, relationship, and attribute

A **record** is one item the deployment knows about, such as a paper, a
person, a repository, a file, or a dataset. A **relationship** connects two
records with a named meaning, such as `authored_by`, `cites`, `part_of`, or
`depends_on`. An **attribute** is a value on either one, such as a title, a
date, or an identifier. The catalog decides which of each are allowed.

Commands and JSON output call records **nodes** and relationships **edges**.
In the tutorial, two input records become record, finding, document, and
document-chunk nodes (a document chunk is one passage of a document), joined
by edges.

## Import, admission, and the working graph

An **import** reads a source within its scope and proposes new, changed, or
removed records and relationships. **Admission** decides each import's
proposed changes as a whole: it accepts them, rejects them with a reason, or
holds them for review. One record outside the rules rejects the whole import,
not just that record. Commands mark a held change with the admission mode
`review` or `quarantine`; `okg admission list --held` lists them.

Accepted changes go into the **working graph**. Nothing there is visible to an
ordinary search yet.

## Publish cycle and published version

A **publish cycle** turns the working graph into a new version. It assembles a
complete candidate, runs the configured processing that derives further
records and relationships, and runs the deployment's checks. If every
required check passes, the candidate becomes the new **published version**.
Commands and results call a version a **generation** and identify it by
`generation_id`.

A published version is never edited in place. If a check fails, the cycle is
blocked and the previous version keeps serving searches.

`okg ingest` runs an import and then a publish cycle. With `--no-publish` it
stops after the import, and the changes stay in the working graph.

## Evidence and provenance

**Provenance** is the record of where a result came from: the source, the
source's own identifier for it, and the import that produced it. With the
evidence AKMON keeps, you can answer:

- Which source supplied this record?
- Which import or source revision saw it?
- Which rule built this relationship?
- Which inputs supported a derived result?

Evidence explains why something is in the graph. It does not prove that the
source itself is correct.

## Derived results and disagreement

A **derived result** is produced from records already in the graph rather than
stated by one source: for example, a relationship built by matching a shared
identifier. It keeps the rule and the inputs that produced it.

When sources disagree, AKMON keeps each source's evidence, and a required
check can block the new version. Automatic conflict handling is experimental;
see [Built-in capabilities](../reference/capabilities.md) for what works
today.

## Search and version pinning

A search reads one published version. Without `--generation-id` it reads the
latest one and returns that version's `generation_id` with the results. Pass
the same identifier to later searches to **pin** them: every read then sees
the same version, even if a newer one is published in between. A pinned read
works for as long as the deployment keeps that version. Keep the identifier
with an analysis to make its graph input reproducible.
[Query and trace the graph](../use/search/index.md) shows the commands.

## Security and data boundary

A graph can store source records, extracted text, relationships, source
identifiers, evidence, and earlier versions. If a source is sensitive, treat
the deployment and its backups as sensitive too.

Before importing private data:

- limit each source to the smallest useful account, folder, repository,
  channel, table, or time range;
- store credential references in configuration, never passwords, tokens, or
  connection strings;
- name who can approve sources, rotate credentials, query the graph, and
  retire the deployment; and
- identify every provider, connection host, assistant host, database, log, and
  backup that can receive a copy, then decide its retention and deletion rules.

Stop if the scope is unclear, a credential would enter the repository, or the
deployment cannot enforce the access boundary you need. See
[Security and credentials](../reference/security.md) for the network,
credential, and role details.

## Where to go next

- [Evaluate AKMON from source](evaluate-from-source.md): run the whole flow on
  sample data.
- [How information becomes searchable](data-lifecycle.md): follow one change
  from import to search, and what happens when an update fails.
- [Install OKG](../operate/install.md): build a graph of one of your own Git
  repositories.
- [Plan a deployment](../operate/plan.md) before you connect private data.
