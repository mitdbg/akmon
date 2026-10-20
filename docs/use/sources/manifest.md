---
audience: user, data owner
type: how-to
canonical_id: source-manifest
section: sources
visibility: primary
status: active
owner: documentation
---
# Bring your own data with a manifest

This page adds your own records to a graph, with their own record types and
links, without writing code. You describe the records in one YAML file, a
*source manifest*. AKMON previews what the file maps, creates the record
types, imports the records, and publishes a new graph version you can search.

A manifest reads local JSON, JSON Lines (one JSON record per line), CSV or
YAML files, or JSON APIs over HTTP. Every key it takes is listed in the
[Source manifest reference](../../reference/source-manifest.md).

You need a deployment that already works, for example one built by
[Install OKG](../../operate/install.md). Below, `<name>` is the deployment's
name. Inside a clone of the repository, prefix each command with `uv run`.

[Connect your data](index.md#choose-a-route) compares this with the other
routes.

## Write the manifest

Suppose a tracker exports tickets every night as `tickets.json`, and each
ticket names the person it is assigned to:

```json
{"issues": [
  {"key": "T-1",
   "fields": {"summary": "Pump alarm on site 3",
              "description": "The turbidity alarm fired twice overnight.",
              "status": {"name": "Open"}, "votes": 2,
              "created": "2026-09-01T08:00:00Z",
              "assignee": {"email": "ada@example.org", "displayName": "Ada"},
              "blocks": ["T-2"]}},
  {"key": "T-2",
   "fields": {"summary": "Replace the inlet sensor",
              "assignee": {"email": "ada@example.org", "displayName": "Ada"}}}
]}
```

Save this manifest as `tickets.yaml` next to the export (or anywhere, with
an absolute `files` glob):

```yaml
manifest: okg.source/v1
description: Tickets exported from our tracker every night.

read:
  files: exports/*.json   # a glob, relative to this file's folder
  records: issues         # where the list of records sits in each file

types:
  tracker_ticket:
    key: "{key}"
    title: fields.summary
    text: fields.description
    dates:
      created: fields.created
    fields:
      status: fields.status.name
      votes: { from: fields.votes, type: integer }
    links:
      assigned_to: { to: tracker_person, key: "{fields.assignee.email}" }
      blocks: { to: tracker_ticket, each: fields.blocks, key: "{.}" }

  tracker_person:
    from: fields.assignee   # people are nested inside each ticket
    key: "{email}"
    title: displayName
```

What each part does:

- **`types`** names the record types to create. Exactly one type reads the
  records themselves (here `tracker_ticket`); every other type gives a
  `from:` path to records nested inside them. One person named by several
  tickets becomes one record.
- **`key`** identifies a record. It is text with `{path}` placeholders. A
  path is a dotted path into one record, and `.` is the record itself. A
  record without a key value is skipped and counted (see
  [When a row is bad](#when-a-row-is-bad)).
- **`title`** and **`text`** are searchable. A text longer than 2,000
  characters is split into passages; a search still returns the record and
  shows the best passage.
- **`fields`** keeps other values. The short form keeps the value as text;
  the long form names a type: `string`, `integer`, `number`, `boolean`,
  `date`, `datetime` or `list`.
- **`links`** creates relationships between records of this manifest.
  `each:` makes one link per item of a list, and `{.}` is the item. A link is
  written only when its target record is in the same read; otherwise it is
  left out and counted.

Type names must be new. A name a built-in type already uses, such as
`ticket`, `person` or `document`, is refused, and so is a name another
source in the deployment declares. Use a prefix such as `tracker_`; the
preview suggests one.

## Preview, then add the source

Preview what the manifest maps. This changes nothing:

```bash
okg deployment add-source <name> tickets --manifest tickets.yaml
```

The preview lists:

- the files it matched and the records it found per type;
- three mapped sample records per type, and the types and links it will
  create;
- every problem, with the file, the record and a fix: a missing or repeated
  key, a value that does not fit its type, a refused type name, or a link
  whose target is not in the files;
- whether the graph would accept the records:
  `Admission would accept the records: yes`.

A problem that would stop the import stops the preview with a `Fix:` line,
for example two records with the same key in one file. Fix the manifest or
the export and preview again.

Then add the source:

```bash
okg deployment add-source <name> tickets --manifest tickets.yaml --apply
```

It saves a copy of the manifest in the deployment as
`sources/tickets.source.yaml`, writes the record types under the
deployment's `schemas/` folder, imports the records and publishes. The
output ends with `Source 'tickets': published`, the new graph version, the
counts added, and one record you can search for.

Check it: search for a word from a ticket.

```bash
okg search --deployment <name> --query 'turbidity'
```

The first result is `tickets:tracker_ticket:T-1`. Every record's identifier
is `<source>:<type>:<key>`.

## Draft a manifest from your data

`okg source draft` reads up to 200 sample records and prints a first
manifest for you to correct. It writes nothing and never adds the source:

```bash
okg source draft exports/tickets.json > tickets.yaml
```

The draft names the record list, each field's type, dates, nested records
and links, a key, a title and a text field. The `key:` and `deletion:` lines
end with `# GUESSED`: check that the key identifies one record and never
changes, and choose the deletion rule, before you preview.

## Read from a JSON API

A manifest can read a JSON API with GET requests instead of files. This
example reads [OpenAlex](https://openalex.org), a free catalog of research
papers that needs no sign-in: MIT's 2024 papers with "knowledge graph" in
the title, their authors, and MIT itself. Save it as `openalex.yaml`:

```yaml
manifest: okg.source/v1
description: MIT's 2024 papers about knowledge graphs, from OpenAlex.

connections:
  openalex:
    base_url: https://api.openalex.org/

types:
  openalex_institution:
    read:
      connection: openalex
      path: institutions?filter=ror:https://ror.org/042nb2s44
      records: results
    key: "{id}"
    title: display_name
    fields:
      country: country_code

  openalex_work:
    read:
      connection: openalex
      # per-page=2 keeps the pages small, so this small example shows paging
      path: works?filter=institutions.id:I63966007,publication_year:2024,title.search:knowledge graph&per-page=2&cursor=*
      records: results
      paging: { cursor: { param: cursor, next: meta.next_cursor } }
    key: "{id}"
    title: display_name
    dates:
      published: publication_date
    fields:
      cited_by: { from: cited_by_count, type: integer }
    links:
      authored_by: { to: openalex_author, each: authorships, key: "{author.id}" }

  openalex_author:
    from: authorships
    parent: openalex_work
    key: "{author.id}"
    title: author.display_name
    links:
      affiliated_with: { to: openalex_institution, each: institutions, key: "{id}" }
```

What is new compared with files:

- **Each top-level type has its own `read:`.** Here works and the
  institution come from two listings, and `affiliated_with` links records
  of one read to records of the other. A nested type names its `parent:`
  when a source has several top-level types.
- **`connections`** hold what several reads share: the `base_url` and the
  `auth`. A read joins its `path:` to the `base_url`, or names a whole
  `url:` instead.
- **`paging`** fetches every page. OpenAlex returns the next cursor in
  `meta.next_cursor`; the reader stops when it is empty. `offset`, `page`
  and `link_header` paging are also available. A read stops at 1,000 pages
  (`max_pages`) and is then incomplete.
- **`records: "*"`** reads an API that answers with one JSON object keyed
  by name, such as `{"T1_CH_CERN": {...}, "T2_US_MIT": {...}}`: each value
  is a record, and `{_key}` is its name.

Preview and add it exactly like files:

```bash
okg deployment add-source <name> openalex --manifest openalex.yaml
okg deployment add-source <name> openalex --manifest openalex.yaml --apply
```

The preview lists each read with its pages, records and whether it
completed, then the samples and problems as for files.

Draft a manifest from the API itself with
`okg source draft '<url>'`; quote the URL so the shell leaves `&` and `*`
alone.

### Sign in with environment variables

An API that needs a credential gets it from an environment variable. The
manifest names the variable, never its value:

```yaml
connections:
  tracker:
    base_url: https://tracker.example.org/api/
    auth: { bearer: { token: TRACKER_TOKEN } }
```

The modes are `bearer: { token: VAR }`, `header: { name: X-Api-Key, value:
VAR }`, `basic: { user: VAR, password: VAR }` and `client_certificate: {
cert: VAR, key: VAR, ca_dir: VAR }`, where each variable holds a file path
for a client certificate. A manifest with a value instead of a variable
name, or a URL with a password or a `token=` or `api_key=` parameter, is
refused. Messages never show a credential, and a redirect to another host
drops it. Set the variable wherever `okg ingest` runs.

### When an item has no key

A nested item with no value at its key is skipped and counted under
`records.skipped_keyless`, and deletion goes ahead. Some APIs return such
items routinely: OpenAlex lists an author without an `author.id` when it
could not identify the person. When more than 10% of one type's items have
no key, the key path is probably wrong, and the read is refused: nothing is
applied or removed.

### When the API fails

A 429 or 5xx answer is retried up to 3 times, honoring `Retry-After`. A read
that still fails, answers with something that is not JSON (a login page,
for example), or has no list at `records:` is incomplete: the records of
the other reads apply, and no record is removed until the read completes.
The reason is in `okg ingest --json`, under the source's
`source_health.reason`.

### When many records disappear at once

An API that answers with a short list, for example after a token expired,
must not empty your graph. `max_deleted_fraction` (default 0.5 when any
read is HTTP) is the share of the earlier records one run may remove. A
run that would remove more applies the records it read, removes nothing,
and says so:

For example, narrowing the OpenAlex filter above to `type:article` keeps 2
of the 5 papers and 9 of the 21 authors. Re-running `add-source --manifest
... --apply` with it ends with:

```text
Note: this read would remove 15 of 27 earlier records (56%), more than
max_deleted_fraction 0.5; the records read were applied and NOTHING was
removed. If the removal is intended, raise max_deleted_fraction in the
manifest for one run (re-run `okg deployment add-source --manifest` with
it, then lower it again); if every record is going, run `okg ingest
--allow-full-retraction openalex` ...
```

If the removal is intended, add `max_deleted_fraction: 0.9` to your
manifest, re-run `add-source --manifest ... --apply` (it reports
`Records removed: 15`), then remove the line and re-run once more. A read
that returns no records at all is left to the full-retraction guard, which
`okg ingest --allow-full-retraction openalex` overrides for one run.

## Keep it current

When a new export arrives, run:

```bash
okg ingest --deployment <name> --include tickets --no-predecessors
```

An edited record updates the same graph record, a new record is added, and
a record missing from a complete read is removed with its links and
passages. A record that did not change writes nothing. When neither the files
nor the manifest changed, the source is skipped without reading. An API is
fetched on every run, unless every page carries an `ETag` or
`Last-Modified` header and the server answers `304 Not Modified` for each.

For an API with a reliable change field, `changes_when: updated_date` on a
type makes that field the record's revision, so a record whose field did
not change writes nothing.

Check it: `okg status --deployment <name> --json` shows a new
`latest_published_generation` under `publishing` after a change, and the
same one when nothing changed.

To keep every record ever imported, even when it leaves the export, set
`deletion: never` in the manifest. A record that stays in the export still
loses links and passages its new content no longer has.

### When a row is bad

A row with no key value is skipped and counted under
`records.skipped_keyless`; it cannot be matched to an earlier record, so a
record that lost its key is removed like one that left the export. When
more than 10% of one type's rows have no key value, the read is refused
with `keyless_share_exceeded`: nothing is applied or removed, because the
key path is probably wrong.

A key with a line break or another control character, or a value that does
not fit its declared type, is rejected. An unreadable file is skipped. The
other records still apply, and the read removes nothing at all until the
problem is fixed, so a bad row never deletes a good record. A link to a
record that this read could not use is kept.

The message is in `okg ingest --json`, under the source's
`source_health.reason`, for example:

```text
1 problem(s): exports/tickets.json record 3 (tracker_ticket:T-4):
votes (fields.votes): 'lots' is not an integer; valid records were applied
and ALL deletion is withheld until these are fixed
```

Fix the rows and run `okg ingest` again. The next clean read removes what
left the export. Under `deletion: never`, these problems are warnings only.

Two records with the same key in one file stop the read: nothing is applied
and nothing is removed, and the error names the file and both records.

### Know what is not guarded

- **A truncated export file can remove records.** A file-only source has
  no `max_deleted_fraction` unless you set one, so a cut-off export file
  removes the records it lost. Only a read that would remove every record
  is refused, with the override `okg ingest --allow-full-retraction
  tickets`. Set `max_deleted_fraction: 0.5`, check an export before you
  ingest it, or use `deletion: never`.
- **`okg ingest --reset-cursor` refuses a source that keeps records.** On a
  `deletion: never` source, one whose last read withheld a deletion, or one
  with `max_deleted_fraction` (every JSON API source), the reset would lose
  the earlier record set the check needs, so the run stops and applies
  nothing. Run `okg ingest` without `--reset-cursor`.

## Change the manifest later

Edit your own `tickets.yaml` and run the same `add-source --manifest`
command, first without and then with `--apply`. A change to a field's path
updates every affected record on the next read.

A change may only add. You can add types, fields and links, and widen a
field from `integer` to `number` or to `string`. You can also change an HTTP
read's URL, path, auth and paging; `max_deleted_fraction` limits what a
narrower URL removes. The command refuses, and names the way out, when a
change would:

- remove or rename a type, field or link;
- change a type's `key:`, which re-identifies every record;
- narrow a field's type;
- change a files read's `files`, `format` or `records`, an HTTP read's
  `records`, or a type's `from:` or `parent:`;
- switch `deletion: never` back to `missing_from_full_read`.

To make such a change, remove the source and add it again with the new
manifest. That imports every record again.

Do not edit `sources/tickets.source.yaml` or the generated schema files in
the deployment. They carry a header that marks them as generated; a file without
that header, or one edited by hand, is never replaced, and the command
refuses until you restore or move it.

Check it: the preview's `install schema:` lines say `replace` for the files
the change rewrites.

## Remove the source

```bash
okg deployment remove-source <name> tickets
okg deployment remove-source <name> tickets --apply
```

The first command previews, the second retracts the records, publishes a
version without them, and removes the source's entry. See
[Remove a source](index.md#remove-a-source) for the details. The generated
files stay in the deployment, so adding the source again with any manifest
works.

Check it: `okg search --deployment <name> --query 'turbidity'` no longer
returns `tickets:` records.
