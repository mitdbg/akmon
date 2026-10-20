---
audience: data owner, operator, researcher
type: how-to
canonical_id: sources
section: sources
visibility: primary
status: active
owner: documentation
---
# Connect your data

Choose how to bring your data into a graph, then add, refresh or remove a source.
You need a deployment that already works,
for example one built by the [Quickstart](../../start/quickstart.md). Below,
`<name>` is the deployment's name. Inside a clone of the repository, prefix
each command with `uv run`.

## Choose a route

| Your data | Use | Code needed |
| --- | --- | --- |
| A folder of Markdown, text, JSON or YAML files | The built-in [`local-folder`](local-folder.md) type | None |
| One GitHub repository's issues and pull requests | The built-in [`github`](github.md) type | None |
| Records you can export as JSON, JSON Lines, CSV or YAML, or get from a JSON API with GET requests, with their own record types and links | A [source manifest](manifest.md) | None; one YAML file |
| A Git repository's files, code and history | The `workspace-graph` [profile](../profiles/index.md), chosen at install | None |
| A service that needs POST requests, OAuth sign-in, or HTML or XML parsing | A reader written in Python: [Add a source adapter](../../develop/index.md#add-a-source-adapter) | Python |

`okg deployment source-types` lists the built-in types and their settings. The [capabilities reference](../../reference/capabilities.md)
lists every reader, including ones only a profile sets up.

## Connect the source

Pick a short name for the source, such as `notes`, and preview it:

```bash
okg deployment add-source <name> notes --type local-folder \
  --setting root=/absolute/path/to/folder
```

The preview changes nothing. It prints the scope it would read, a few of the
records and relationships it would add, and how edits and deletions are
handled. A problem stops it here with a `Fix:` line.

Run the same command with `--apply` to add it. At a terminal it asks for
confirmation first; `--yes` skips the question. It saves the source in the deployment's
`source_registry.yaml`, imports it, checks the result, and publishes a new
version of the graph. The output starts with `Source 'notes': published` and
ends with one record you can search for:

```bash
okg search --deployment <name> --query '<a word from your data>'
```

**If a step fails**, the output names the step and its cause, and the source
stays disabled. Fix the cause and run the same `--apply` command again. It
continues from the failed step and never duplicates records; if an earlier
attempt already published, it reports `no_change` and enables the source.
Running the command after a success is also safe. If the output says the
record of the earlier attempt is missing, remove the source as below and add
it again.

## Keep it up to date

The graph changes only when a refresh runs. After your data changes, run:

```bash
okg ingest --deployment <name>
```

This reads every source, then publishes one new version if something
changed. An edited record updates the same graph record, a new one is added,
and one that is gone is removed. A read that stops early removes nothing. To
refresh one source, add `--include notes --no-predecessors`; without
`--no-predecessors`, `--include` also reads the sources this one depends on.
To refresh on a schedule, run the
[background worker](../../operate/deployments/runtime.md).

If one source fails, the others still publish. The failed source's records
stay as they were, and `okg ingest` exits with an error and names the source
under `failed_sources`.

**Check it:** `okg status --deployment <name> --json`. Under `publishing`,
`latest_published_generation` changes only when a new version was made. If
one record is missing, follow the
[missing-record procedure](../../runbooks/troubleshooting/first-run.md#8-import-succeeded-but-one-record-is-missing).

## Remove a source

Preview, then remove:

```bash
okg deployment remove-source <name> notes
okg deployment remove-source <name> notes --apply --yes
```

The preview shows how many records and relationships would be retracted and
the registry entry that would go. `--apply` disables the source, retracts its
records, publishes a version without them, removes its entry from
`source_registry.yaml`, and updates the deployment's ownership record and,
if it has one, its access policy. Earlier versions keep the records as history. When the deployment is
a Git checkout, the command commits only its own change to
`source_registry.yaml`, and refuses while that file has uncommitted edits of
yours.

A source that never reached the graph, such as one whose first import
failed, has nothing to retract: `--apply` removes only its registry entry.
If a step fails, run the same command again; it continues without
retracting or publishing twice. A refusal before anything changes is
`remove_source_refused`, and its `fix` line names what to do first:

- **Records were imported but nothing was ever published.** Publish once
  with `okg run --once --deployment <name> --inline --apply --json`, then
  remove.
- **Another enabled source names this one as its `predecessor_source`.** Remove that
  link, or that source, first.
- **The source has records again after an earlier removal was published.**
  Disable it instead: set `enabled_by_default: false` in
  `source_registry.yaml`.

## Credentials

A `local-folder` source needs no credential. A reader that needs a secret takes `--credential NAME`, where `NAME`
is an environment variable that holds it; the deployment saves only that
name, a credential reference. Do not store a token, password or database
connection string in a `--setting` or in `source_registry.yaml`;
`add-source` refuses a setting whose name looks like a secret.
