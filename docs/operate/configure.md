---
audience: operator, developer
type: how-to
canonical_id: task-change-deployment-configuration
section: operations
visibility: primary
status: active
owner: documentation
---
# Configure a deployment

A deployment is a folder of YAML files that says what the graph reads, which
record types it holds, which database it lives in, and who may read it. This
page explains those files for a deployment built by
[Install OKG](install.md), what you can change in them, and what to run after
each change so it takes effect.

Run the commands from the `okg` directory with its environment active. Below,
`<name>` is the deployment name you installed with.

## The deployment folder

The install writes the folder to `~/.local/share/okg/deployments/<name>/`, or
to `$XDG_DATA_HOME/okg/deployments/<name>/` when that variable is set. Below,
that folder is `<dir>`.

| File | What it holds | Edit it? |
|---|---|---|
| `deployment.yaml` | The deployment's settings: record types, database, access policy, ports. | Yes, a few blocks. See below. |
| `source_registry.yaml` | One entry per source the graph reads, with the settings each reader takes. | Yes. This is where most changes happen. |
| `schemas/` | The deployment's own record-type additions, generated at install. | No. |
| `invariants.yaml` | Health checks run on every publish. | Rarely. |
| `dashboards/` | Dashboard definitions for the operator interface. | Rarely. |

Keep a copy of a file before you edit it.

## deployment.yaml

The install writes these blocks, in this order.

**`name`, `description`.** The deployment's identity. Do not change `name`:
the database, the access policy and every record are keyed by it.

**`profile`.** A record of how the install built this deployment: the profile,
your install answers, and where each module and source default came from. It
is most of the file, about 700 lines. Leave it alone; it documents the
install, and changing it does not change what the graph reads.

**`modules`.** The record types the graph can hold, one module per family:
`git_graph` for files and commits, `code_graph` for code symbols, `extraction`
for document text, `agent_sessions` for Codex sessions, and so on. A source
can only publish record types from a loaded module. Add a module when a new
source needs its types; `okg deployment add-source` does that for you.

**`source_registry`, `schema_dir`.** Paths to `source_registry.yaml` and
`schemas/`. Leave them.

**`embeddings`.** Vector sets for semantic search; see
[Embeddings](#embeddings). The workspace profile turns them off.

**`enrichers`.** Linkers that run after sources import and add relationships
between records from different sources, for example from a test to the code
symbol it covers. You can remove one; see the table below for what to run
after.

**`viz`.** Records the `--mcp-port` answer from the install. Nothing on this
install reads `viz.port`: `okg mcp-serve`
takes its port from `--port`, and chat from `chat.mcp.port`.

**`nomos`.** The access policy. `runtime_enforcement` is `enforce`,
`audit` or `off`. `deployment_posture` says who may read and write and which
kinds of data the deployment accepts. `source_policy_defaults` gives each
source class its data rules: how sensitive its data is, whether raw copies are
stored, and whether it may be exported. Leave these as installed: setting
`store_raw: false`, for example, stops every source unless
`OKG_PSEUDONYM_HMAC_KEY` is set. Even with the key, every attribute that
the source's `pii_classes` does not mark `non_pii` or `free_text` is stored
masked, so search over that source returns nothing.
`okg deployment lint` warns about both
(`deployment.nomos.pseudonym_key_missing`,
`deployment.nomos.store_raw_masks_all`).

**`postgres`.** `dsn` is the database address. It may name an environment
variable instead, as `${OKG_DSN}`.

**`release`.** `channel: dev` and `kind: standalone` let the deployment publish
from a folder that is not a Git checkout. Leave them, and do not run
`git init` in the folder: a deployment inside a Git repository stops being
standalone, and every publish is refused while an edit is uncommitted
(`repository provenance inputs are not committed`).

Other blocks are optional and absent after install, for example `chat` (see
[Set up chat](chat.md)) and `search` (named search scopes). A `publisher`
block is accepted but has no effect on this install.

## Embeddings

A vector set is a model, a document template per type, and an index; all
but `backend` and `types` below are defaults.

```yaml
embeddings:
  enabled: true
  search_set: main        # required with several sets
  sets:
    main:
      backend: openrouter:google/gemini-embedding-2@768
      storage: halfvec    # or vector
      subtypes: all
      max_chars: 24000
      document:
        default: {header: [subtype, title], body: text}
        types: {jira_issue: {header: [subtype, title, status], body: text}}
      index:
        kind: hnsw        # or none (exact, small scopes)
        build: {m: 16, ef_construction: 64}
        search: {ef_search: 100, max_scan_tuples: 20000}
```

A document is a `Label: value` line per header field, a blank line, then
`text`; `okg migrate` refuses ids, hashes, timestamps and masked fields
(`embedding_header_field_masked`). A set keeps its model's width
(`hash@<dims>` for tests) within pgvector's limits: 16000 stored, and
under `hnsw` 4000 for `halfvec`, 2000 for `vector`. A new model, width,
storage, `max_chars`, prefix or provider makes a new set; a template change re-embeds changed documents
only; new build settings need `okg migrate`, then `okg db embed-index
--apply`. `{enabled: true, backend: <spec>}` is one set `main`. The worker
fills `hash` and `sentence-transformers` sets after each publish; a paid set
needs `okg db embed-backfill`. Search uses a set from the publish after its
fill completes.

## source_registry.yaml

`sources:` has one entry per source. This is the `git_files` entry from a
workspace install, shortened:

```yaml
sources:
  git_files:
    module: okg.substrate.library.sources.git_graph_files
    class: GitFilesSource
    ownership_id: <name>.git_files
    source_class: discovery_crawl
    record_identity_kind: scoped_locator
    record_identity_fields: [repo, path]
    source_revision_kind: content_hash
    deletion_semantics: missing_from_completed_scope
    publication_mode: published_generation
    params:
      repo: <name>
      repo_root: /path/to/repo
      include_untracked: false
      exclude_globs:
      - .venv/**
      - node_modules/**
```

The fields fall into three groups.

**The reader: `module`, `class`, `params`.** `module` and `class` name the
code that reads the source. `params` are that reader's settings: which folder
or repository, which files to include or skip. Each reader takes its own
`params`; the entry the install wrote shows the ones it uses. This is the part
you change.

**Identity: `ownership_id`, `record_identity_kind`, `record_identity_fields`,
`source_revision_kind`.** How the graph recognises the same record across
runs, and how it tells that a record changed. Here a file is identified by its
repository and path, and changes when its content hash changes. Do not change
these on a source that has already published: they define every record's
identity, so the graph would no longer match its existing records.

**Update rules: `source_class`, `deletion_semantics`, `publication_mode`,
`predecessor_source`.** How a run updates the graph.

- `source_class` is the kind of source. `discovery_crawl` walks a folder or
  repository, and a record missing from a complete walk is removed.
  `reference_catalog` is a versioned catalog, and records are removed only
  when a newer version arrives. `mutable_api` is a service read by cursor, and
  only a full re-read removes records. `append_feed` only ever adds.
- `deletion_semantics` says when a missing record counts as deleted.
- `publication_mode: published_generation` means changes appear in the next
  published version.
- `predecessor_source` makes this source run after another one it depends on.

Leave the update rules as the install wrote them.

**On or off: `enabled_by_default`.** Set `enabled_by_default: false` on an entry
to stop reading that source without deleting its entry. Its records stay in
the graph; to remove them, see
[Stop reading a source and remove its records](#stop-reading-a-source-and-remove-its-records).

## What to run after a change

After most edits, the next publish is refused with `catalog_manifest_drifted`
until you record the edited files as the ones this database publishes:

```bash
okg catalog ownership claim --deployment <name>
```

Then run the command in the table. Each row was run on a fresh workspace
install.

| You changed | Then run | Check |
|---|---|---|
| A source's `params`, such as a folder path | Claim, then `okg ingest --deployment <name> --include <source>` | `okg status` counts change; `okg search` finds the new records |
| Which files a source skips (`exclude_globs`) | See [Stop indexing some files](#stop-indexing-some-files) | Fewer `source_file` nodes |
| `enabled_by_default: false` on a source | Claim; later `okg ingest` runs skip the source | The source is missing from the ingest output |
| `modules`: added a module | Claim, `okg catalog load --deployment <name> --apply`, then `okg run --once --deployment <name> --apply` | `okg doctor` reports `ok` |
| `enrichers`: removed one | Claim, then `okg provision --deployment <name>` | `okg doctor` reports `ok` |
| `postgres.dsn`: a new, empty database | `okg provision --deployment <name>` (no claim; it sets up the database and publishes) | `okg status` reads the new database |
| `profile`, `viz`, `publisher` | Nothing; they do not change what the graph reads | — |

Scheduled refresh needs the [background worker](deployments/runtime.md);
while it runs, restart it after any edit.
Some changes do not work on this install yet:

- **Removing a module** such as `ci`, `compute_env`, `openspec` or
  `agent_memory` is refused with `bridge_subtype_unknown`.

If a source fails while others succeed, the publish goes ahead and the new
version lists the failed source as not current. Fix the source and run it
again. If the failed source's own changes could not be undone, the publish is
blocked instead, and later edits are refused with `incomplete checkpoint
recovery scope or binding changed`. Clear that with the command the refusal
prints, `okg runtime supersede-checkpoint`.

## Check an edit

```bash
okg deployment lint <name> --json
okg doctor --check sources --deployment <name> --json
```

Both exit 1 when an entry is malformed. Neither checks a reader's `params`: a
misspelled or missing setting passes both and fails only when the source runs. On a new install
`lint` also reports ten `unroutable_reference_field` warnings; they are
expected.

`okg schema show deployment.yaml` prints the schema for the file. It is built
from the same key list the loader reads, so it accepts every top-level key the
loader accepts and names the type each one takes. Inside a block it refuses an
unknown key only where the loader does too. The schema, `lint` and every
command that loads the deployment refuse a misspelled key inside `postgres`
or `chronos`, such as `dns:` for `dsn:`; `okg deployment env` does not check
it. `lint` also warns
(`deployment.manifest.unread_key`) when you set a key that no code acts on.

## Make Markdown text searchable

The install does not read the text of Markdown files. Add a document reader
with one command. Without `--apply` it only previews what it would read:

```bash
okg deployment add-source <name> repo_docs --type local-folder \
  --setting root=<repo> \
  --setting 'include=["**/*.md"]' \
  --setting 'exclude=["**/.git/**", "**/.venv/**", "**/node_modules/**"]'
```

`<repo>` is the repository path you installed with. The reader reads every
matching file under it, including files Git ignores, so keep the `exclude`
list. Run the same command with `--apply` added. It saves the source, imports,
publishes and prints a sample result. Now
`okg search --deployment <name> --query '<a word from a Markdown file>'`
returns a `document_chunk`.

## Stop indexing some files

Five sources index source files and code. Add the same pattern, relative to
the repository root, to each of them:

- under `params.exclude_globs` of `git_files`, `code_structure`,
  `code_data_flow` and `interfaces`;
- under `params.source_file_exclude_globs` of `git_history` (add the list if
  it is not there).

For example, `- src/scratch/**`. Check, claim, and run:

```bash
okg catalog ownership claim --deployment <name>
okg ingest --deployment <name>
```

The changed sources reread the repository and remove the files the pattern
now matches. `okg status` then counts fewer `source_file` and `code_symbol`
nodes, and `okg search` no longer finds the files. Add the pattern to all five
sources: if only `git_files` excludes a file, the others still point at it and
the publish can be refused. Other sources that mention files, such as session
logs or OpenSpec proposals, are not filtered by these patterns.

## Limit a code repository to some paths

A large repository, such as CMSSW, can be limited to a few directories. Put
the patterns under the repository's own `params`, keyed by the block they
apply to. This `code_repos` block reads only `DataFormats/` and
`RecoTracker/` of CMSSW:

```yaml
code_repos:
  base: ${OKG_CODE_REPOS_ROOT}
  expand: [git-files]
  repos:
    - slug: cmssw
      url: https://github.com/cms-sw/cmssw.git
      params:
        git-files:
          include_globs: ["DataFormats/**", "RecoTracker/**"]
```

`code_repos.params` takes the same shape and applies to every repository; a
repository's own `params` win. A pattern put anywhere else, such as
`include_globs` directly on the repository entry or under `code_repos`, is
refused with `source_registry_unknown_key`, which names the key and where it
belongs. The registry never silently reads the whole repository because of a
misplaced key.

### What a pattern matches

Patterns are matched against the path relative to the repository root, such
as `DataFormats/TrackReco/interface/Track.h`. The three readers do not match
the same way:

| Reader (`expand` block) | Keys | How a pattern matches |
|---|---|---|
| `git-files` | `include_globs`, `exclude_globs` | Python `fnmatch`. `*` also matches `/`, so `DataFormats/*` and `DataFormats/**` both mean "anything under `DataFormats/`". `**/BuildFile.xml` needs at least one directory, so it does not match a `BuildFile.xml` at the root; list `BuildFile.xml` as well for that. |
| `code-structure` | `exclude_globs` only, plus `include_exts` and `max_files` | Python `fnmatch`, as for `git-files`. A directory that matches a `<dir>/**` pattern is skipped without reading it. |
| `doc-corpus` | `include_globs`, `exclude_globs` | Gitignore-like. `*` stays inside one directory, `**/` matches zero or more directories (so `**/README.md` matches the root `README.md` too), and `*`, `**` or `**/*` alone match everything. |

For every reader:

- A missing `include_globs` reads every file. An empty list differs:
  `git-files` reads every file, `doc-corpus` reads none.
- A path that matches an `exclude_globs` pattern is skipped even when it also
  matches `include_globs`.
- `exclude_globs` **replaces** the list the reader would otherwise use; it is
  not added to it. A `code_repos` repository starts with the codebase-index
  template's list (`__pycache__`, `.venv`, `node_modules`, `.git` and similar).
  If you set `exclude_globs`, copy the entries of that list you still want
  from `src/okg/substrate/library/templates/codebase-index/source_registry.yaml`.
- `git-files` in a `code_repos` repository also reads untracked files that Git
  does not ignore. To find them it walks directories, and it tests every
  directory at every level before entering it: a directory `<dir>` is skipped
  when `<dir>/.okg-prune-probe` fails `include_globs` or matches
  `exclude_globs`. `.git/` and `.claude/` are always skipped. Tracked files
  are not affected by this rule. As a result:
  - A directory pattern such as `DataFormats/**` passes for `DataFormats/`
    and everything below it.
  - A file-name pattern such as `*.h` reads every tracked `.h` file, but
    skips untracked `.h` files below the repository root.
  - A nested pattern such as `DataFormats/Foo/**` skips `DataFormats/`
    itself, so untracked files under `DataFormats/Foo/` are not read. To
    read them, also list `DataFormats/.okg-prune-probe`, which lets the walk
    enter `DataFormats/` and matches no real file.
  - An exclude pattern such as `*/test/*` skips every `test/` directory
    below the root.

### code-structure reads the whole checkout

`code-structure` walks the checkout itself. The `git-files` `include_globs`
do not narrow it, and it has no `include_globs` of its own. On a repository
the size of CMSSW it stops with `code graph source exceeded max_files=5000`
instead of reading more. To index code symbols in only some directories,
either leave `code-structure` out of `expand` (as above), or exclude every
other top-level directory. This prints that list for the example:

```bash
cd "$OKG_CODE_REPOS_ROOT/cmssw"
find . -mindepth 1 -maxdepth 1 -type d ! -name .git |
  sed 's|^\./||; s|$|/**|' |
  grep -v -x -F -e 'DataFormats/**' -e 'RecoTracker/**'
```

Put the output, plus the template entries you keep, under
`params: {code-structure: {exclude_globs: [...]}}`, and add `code-structure`
to `expand`. Files at the repository root that have a code extension are
still read.

### Count the files before you ingest

No `okg` command previews how many files a source will read. Count them with
Git in the checkout, using the same `fnmatch` rule `git-files` uses:

```bash
cd "$OKG_CODE_REPOS_ROOT/cmssw"
{ git ls-files -z; git ls-files -z --others --exclude-standard; } | python3 -c '
import fnmatch, sys
include = ["DataFormats/**", "RecoTracker/**"]
paths = [p for p in sys.stdin.read().split("\0") if p]
print(sum(any(fnmatch.fnmatch(p, g) for g in include) for p in paths))'
```

`-z` keeps paths with spaces or non-ASCII characters intact. The source
reads at most that many files; `exclude_globs` patterns remove more, and so
does the directory rule above for untracked files. After the first ingest, `okg status` counts the `source_file` nodes it
published.

## Store commit diffs

By default the history source stores each commit's message and the list of
files it changed, not what changed inside them. To answer "what was the old
value, and which commit changed it?", turn on diffs for one repository under
`params` of its `git_history` entry:

```yaml
    params:
      store_diffs: true
      diffs_since: "2025-01-01"      # optional: only newer commits get diffs
```

In a `code_repos` block whose `expand` lists `git-history`, put the same
settings on the one repository entry:
`- {slug: <slug>, url: <url>, params: {git-history: {store_diffs: true}}}`.

Each changed text file of each commit then becomes a `git_file_diff` record
holding its diff: `-` lines show the old text, `+` lines the new. Search for an
old value returns the diff; its id, `diff:<repo>:<commit>:<path>`, names the
commit. Merge commits, binary files, files larger than 1 MiB, and files whose
old text is missing from a partial clone get no diff.

Before you turn it on:

- **Everyone who can read the graph can read every stored diff**, including
  text that was later deleted. Secret-shaped values (passwords, tokens, keys,
  private-key blocks) are replaced with `<secret:redacted>`, and files such as
  `*.env`, `*.pem`, `*.key` and paths containing `secret` or `credential` are
  never diffed. Redaction matches known shapes only (`key: value` on one line,
  `Bearer` tokens, cloud and chat tokens); a secret on the line after its key,
  such as a YAML `password: >-` block, stays readable. Do not turn diffs on for
  a repository that has held secrets in other forms.
- **Size.** One file's diff is cut at `diff_max_file_bytes` (default 32 KiB)
  and one commit's diffs at `diff_max_commit_bytes` (default 256 KiB), smallest
  files first; the commit's `diff_omitted_file_count` counts files left out.
  Measured on this project's own repository: about 23 KB and 3.6 diffs a
  commit on average. The search index stores the text a second
  time, and a deployment that embeds node text embeds every diff. Use
  `diffs_since` on a long history.
- `diff_exclude_globs` replaces the default list of files never diffed
  (lockfiles, minified files, source maps, and the secret-bearing files above).

The next history run re-reads every commit once and adds the diffs; later runs
add diffs for new commits only. After you turn diffs off, lower a limit or
exclude more files, the diffs already stored stay until a full run removes
them:

```bash
okg ingest --deployment <name> --include git_history --mode scope_complete
```

A deployment whose sources declare an `output_signature`
must also list the `git_file_diff` node and its two edges there, or the first
run with diffs on is refused.

## Stop reading a source and remove its records

Deleting a source's entry by hand stops it from being read, but its records
stay. Remove the source with one command instead:

```bash
okg deployment remove-source <name> <source>
okg deployment remove-source <name> <source> --apply
```

The first command changes nothing and shows what would be retracted. The
second disables the source (which stops scheduled and default imports of
it), retracts its records, publishes, removes its
entry from `source_registry.yaml`, records the new ownership, and updates the
access policy; see [Remove a source](../use/sources/index.md#remove-a-source).

### Fallback: the manual steps

If `remove-source` refuses and its fix does not apply, the same removal can be
done step by step. Retire the source while its entry is still there. `<dsn>`
is the `postgres.dsn` value in `deployment.yaml`:

```bash
okg remediate retire-source --source <source> \
  --deployment-name <name> --dsn <dsn>
```

This changes nothing. It lists the records it would remove and ends with
`counts` and a `plan_digest`. Apply that plan; `--confirm-source` takes the
source's name or the ownership id the plan shows:

```bash
okg remediate retire-source --source <source> \
  --deployment-name <name> --dsn <dsn> \
  --expected-plan-digest <plan_digest> --confirm-source <source> --apply
```

Delete the entry from `source_registry.yaml`, check, claim, and publish:

```bash
okg catalog ownership claim --deployment <name>
okg run --once --deployment <name> --apply
okg provision --deployment <name>
```

`okg run --once` publishes the removal and prints how many records it
retracted. `okg provision` updates the access policy, without which
`okg doctor` keeps failing with `stage_needed:nomos_baseline`. It can exit 1 with
`publish did not add live node and edge rows`; the policy is updated anyway.
