---
audience: user, data owner, operator, developer
type: explanation
canonical_id: data-lifecycle
section: get-started
visibility: primary
status: active
owner: documentation
---
# How information becomes searchable

AKMON answers every search from a published version of the graph.
A **published version** is a complete snapshot that passed the deployment's
checks. This page follows one
change, an edited file, from the moment you run a refresh until a search finds
it, and explains what a reader sees when the refresh fails.

The commands below need a deployment you can refresh by name, such as one
built by [Install OKG](../operate/install.md). The throwaway graph from
[Evaluate AKMON from source](evaluate-from-source.md) shows the same
publish-and-search result on sample data, but it cannot take a new source yet.
[The AKMON model](concepts.md) defines the terms used here.

```text
Source: an approved folder, repository, or service
                  |
          okg ingest (a refresh)
                  |
    import → admission → working graph
                  |
     publish cycle: build + check
                  |
     checks pass: new published version
                  |
       Search → record + evidence

Failed refresh: the previous published
                version keeps serving reads.
```

## One change, step by step

Say a deployment named `<name>` reads a folder of notes through a
`local-folder` source called `notes`, added as in
[Connect your data](../use/sources/index.md), and you edit one file in that folder.

**Nothing changes yet.** The graph changes only when a refresh runs. Until
then, a search for a word you added finds nothing, because searches keep
reading the current published version.

**Import.** Run the refresh for this source. Inside a clone of the repository,
prefix each command with `uv run`.

```bash
okg ingest --deployment <name> --include notes --no-predecessors
```

The import reads the folder within its configured scope and proposes the
edited file's changed records. **Admission** decides the import's proposed
changes as a whole: it accepts them, rejects them with a reason, or holds them
for review. Accepted changes go into the **working graph**, which searches do
not read.

**Publish.** The same command then runs a **publish cycle**. It assembles a
complete candidate from the working graph, derives the relationships the
deployment configures, and runs the deployment's checks. If every required
check passes, the candidate becomes a new published version with a new
`generation_id`. If nothing changed, no new version is made.

**Search.** Search for a word you added:

```bash
okg search --deployment <name> --query '<a word you added>' --json
```

The result names the `generation_id` it read, now the new version. The match
carries its source and the source's own identifier, here the source name and
the file's path, so you can trace it back to the file.

Importing and publishing can be separated. `okg ingest --no-publish` stops
after the import: the accepted changes stay in the working graph and are not
searchable until a later publish cycle, which
`okg run --once --deployment <name> --inline --apply --json` runs. A
deployment that holds changes for review publishes them only after someone
accepts them; the admission commands are in the
[CLI reference](../reference/cli.md#okg-admission).

## Know which version you are reading

Every search returns the `generation_id` it read. Pass it back with
`--generation-id` to keep related searches on the same version, as
[Query and trace the graph](../use/search/index.md#keep-related-reads-on-one-version)
shows. A newer publication never changes an older version, and a pinned read
works for as long as the deployment keeps that version and the reader is still
allowed to read it.

The [operator UI](../operate/operator-ui.md) holds the version it loaded until
you press **Refresh**, and a UI whose read API was started with
`--generation` stays on that version even then. A new command-line search reads the latest version, so the
UI and a command can show different versions. Compare their identifiers before
you combine their answers. In the UI, **Health** shows **Published graph**,
the version serving reads, next to **Latest recorded update**, the most recent
publish attempt. They differ when that attempt was blocked.

A new published version shows that a publish cycle finished. It does not show
that a particular record was accepted or that a particular reader may see it,
so search for a record you know changed. The age of a version does not show
how fresh a source is, and zero net record changes do not show that no work
happened.

## What survives a failure

A failed refresh never replaces the published version with a partial graph.
Searches keep reading the previous version. If the deployment has never
published, there is no previous version to read. A failure before a publish
attempt is recorded may leave no attempt identifier in Health, and missing
outcome, reason, or history means that evidence is unavailable, not that
nothing went wrong.

| What happened | What to check next |
| --- | --- |
| A source could not be read | Inspect the source error and its approved access; publication history alone cannot explain the source run. |
| A change was rejected or held | Read the admission reason before expecting that change in a published result. |
| Processing or a publication check failed | Keep the recorded failure and the identifier of the previous published version, if there is one. |
| An older blocked attempt remains after recovery | Compare it with the latest recorded outcome; history does not by itself mean the current update is blocked. |
| A read was denied | Keep the authorization error. Do not treat it as an empty graph or widen access to get a result. |
| A bounded search did not return a record | Check the selected version, query, filters, and limits as well as the source and admission evidence. |

## Where to go next

- [Operating AKMON](../operate/index.md): the daily tasks of updating a
  source, explaining a record, and diagnosing a failed update.
- [The missing-record procedure](../runbooks/troubleshooting/first-run.md#8-import-succeeded-but-one-record-is-missing),
  when an import succeeded but the record you expected is not searchable.
- [Publication keeps failing](../runbooks/troubleshooting/generation-blocked.md),
  when a check blocks every new version.
- [Connect your data](../use/sources/index.md) and
  [Query and trace the graph](../use/search/index.md) for the full commands.
