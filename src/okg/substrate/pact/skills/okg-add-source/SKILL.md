---
name: okg-add-source
description: Populate the graph — wire a data source into a deployment, then ingest it. Use when the user says "add a data source", "add git/code/papers/openspec/sessions", "wire up <source>", "ingest X into the okg", or asks "what can this graph ingest". A graph is useless until it's populated.
---

# Adding a data source

The user guide for this path is `docs/use/sources/index.md`; a custom
connector is covered in `docs/develop/index.md` ("Add a source adapter").
Terms such as connector, registration, and change probe are defined in
`docs/reference/glossary.md`.

## 1. See what can be added

```bash
uv run okg deployment source-types --json
```

It lists each supported type with its settings and credential needs
(`local-folder` and `github` today). A format or service that is not listed
needs a packaged connector (step 3) or a profile that sets it up (step 5).

## 2. Preview, then apply

```bash
uv run okg deployment add-source <deployment> <name> --type <type> \
  --setting <key>=<value> [--credential <ENV_NAME>]
```

Without `--apply` this changes nothing: it prints the scope, sample records and
relationships, and how edits and deletions are handled. Show the preview to the
user. Then run the same command with `--apply --yes`: it saves the source in
`source_registry.yaml`, imports, checks, publishes, and prints one searchable
record. `--credential` takes the name of an environment variable, never a
secret value. A failed step prints its cause and leaves the source disabled;
fix the cause and re-run the same command.

## 3. A custom connector

A connector is its own Python package with a registration (`REGISTRATION` in
code, `connector.json` on disk). Copy its schemas into the deployment's
`schemas/` and `schemas/bridges/` first; `add-source` does not install them.
Keyword search reads a record's `text` attribute, so a class whose content is
in other fields must list them, or its records publish but never match a
keyword search:

```yaml
    annotations:
      archetype: artifact
      okg_searchable_text_fields: summary,description
```

Then preview and apply with `--registration` instead of `--type`:

```bash
uv run okg deployment add-source <deployment> <name> \
  --registration <dir>/connector.json --setting <key>=<value>
```

Start a new connector from `examples/json-record-connector`, and run its
conformance kit before adding it.

## 4. Lint before every ingest

```bash
uv run okg deployment lint <deployment> --json
```

Do not proceed while lint reports `probe_missing`, `probe_runtime_missing`,
`probe_unsound`, or `poll_probe_unsound`. Refresh later with
`uv run okg ingest --deployment <deployment>`.

## Incremental-source admission contract

Every runnable source must be incremental by construction. This matters when
you author or review a reader:

- The adapter declares a class-level `change_probe_kind` and assigns the
  matching runtime `self.change_probe`. The probe covers every input that can
  change emitted facts, including effective config and emission code.
- Match the probe to the read model: `git_scope` for a HEAD/worktree scope,
  `git_refs` for commit history, `union` for composite scopes, `mutable_api`
  for a remote version token, and `content_hash` as the O(content) floor for
  untracked or generated files.
- A source with `sync.triggers: [poll, ...]` must set
  `sync.poll.probe: change_probe`, or use `always_changed` when no sound
  runtime short-circuit exists. Never use producer-owned `stat_tree`,
  `content_hash`, `ttl`, or `remote_etag` poll selectors: they can suppress a
  real change before the runtime probe runs.
- Prove it with edit, add, delete, mtime-preserved edit, config-change, and
  probe-error cases. A probe error must force a full read, never
  `changed=false`.

## 5. Other routes

- **Profile sources.** `okg add <source> --deployment <deployment>` belongs to
  the `academic-researcher` profile: a low-level recovery command for its own
  source short-names (`okg add --help`). It is not the general path.
- **Agent (MCP).** For an approval-gated conversation, use
  `discover_sources` → `propose_setup` → `commit_setup`; check what is wired
  with `list_sources` and `source_status`. Inspect the proposal's adapter and
  poll settings before `commit_setup`, and run lint afterwards. A source with
  no trusted connection route is marked `needs_connection`.
