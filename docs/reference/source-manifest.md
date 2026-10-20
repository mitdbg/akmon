---
audience: user, data owner, developer
type: reference
canonical_id: source-manifest-reference
section: reference
visibility: primary
status: active
owner: documentation
---
# Source manifest reference

A source manifest is the YAML file `okg deployment add-source <name>
<source> --manifest <file>` reads. It describes where the records are --
local JSON, JSON Lines, CSV or YAML files, or JSON APIs read over HTTP -- and the
record types they become. This page lists every key of version
`okg.source/v1`. For worked examples, see
[Bring your own data with a manifest](../use/sources/manifest.md).

Print the JSON Schema the command validates against:

```bash
okg schema show source_manifest.yaml
```

In the paths below, `<type>`, `<field>` and `<link>` stand for names you
choose. A name is snake_case: lower-case letters, digits and underscores,
starting with a letter.

## Paths and templates

- A **path** is a dotted path into one record, such as `fields.summary`. A
  number selects a list item (`fields.labels.0`). `.` is the record itself.
  A path with no value gives no value; that is never an error, except for a
  key.
- A **template** is text with `{path}` placeholders, such as `"{key}"` or
  `"PROJ-{id}"`. It needs at least one placeholder. A rendered key must not
  be empty and must not contain a line break, another control character, or
  an invisible formatting character.

## Top-level keys

| Key | Type | Default | Meaning |
| --- | --- | --- | --- |
| `manifest` | text, required | none | The format version. Must be `okg.source/v1`. |
| `description` | text | none | What the source holds, in plain words. Shown as the source's description. |
| `connections` | mapping | none | Named connections that several HTTP reads share, keyed by a name you choose. |
| `connections.<connection>` | mapping | none | One connection. |
| `connections.<connection>.base_url` | URL, required | none | The start of every URL read through this connection. A read's `path` is joined to it. |
| `connections.<connection>.auth` | auth | `none` | How reads through this connection authenticate; the same keys as `read.auth`. A read's own `auth` replaces it. |
| `read` | read | none | Where the records of the one top-level type without its own `read` come from. Needed unless every top-level type has its own `read`. The read keys are below. |
| `types` | mapping, required | none | The record types, keyed by a new type name. |
| `deletion` | `missing_from_full_read` or `never` | `missing_from_full_read` | `missing_from_full_read`: a record absent from a complete read is removed. `never`: records are never removed. |
| `max_deleted_fraction` | number from 0 to 1 | `0.5` when any read is HTTP; no limit for file-only sources | The share of the source's earlier records one run may remove. A run that would remove more applies the records it read and removes nothing; its message names the count. Raise it for one run to let an intended removal through. A read that returns no records at all is left to `okg ingest --allow-full-retraction <source>`. |

## Read keys

A read is either local files (`files`) or a JSON API read with HTTP GET
(`connection` with `path`, or `url`). These keys apply to the top-level
`read` and to every `types.<type>.read`.

| Key | Type | Default | Meaning |
| --- | --- | --- | --- |
| `read.files` | text | none | A glob naming the input files. A relative glob is resolved against the manifest's folder and saved as an absolute path. A glob that matches no file is refused; files are read in sorted path order. |
| `read.format` | `json`, `jsonl`, `csv` or `yaml` | from each file's extension: `.json`, `.jsonl` or `.ndjson`, `.csv`, `.yaml` or `.yml` | Files only: the file format. A YAML file holds one document and reads as the same data in JSON would: an unquoted date becomes its ISO text. YAML 1.1 reads unquoted `yes`, `no`, `on` and `off` as true or false; quote them to keep the text. An alias (`*name`) makes the file unreadable. A source manifest that a `*.yaml` glob matches is not read as data. |
| `read.records` | path, or `"*"` | the file or response itself | JSON and YAML only: where the list of records sits in each file or response. `"*"` reads each value of a JSON object as one record and makes the value's name available as `{_key}`. Without it, a document holds a list of records or one record. Each JSON Lines line and each CSV row is one record. A response with no list at this path makes the read incomplete. |
| `read.connection` | connection name | none | Read through `connections.<connection>`. Needs `path`. |
| `read.path` | text | none | The rest of the URL after the connection's `base_url`, query included. |
| `read.url` | URL | none | The whole URL, when the read uses no connection. |
| `read.auth` | `none`, or one mode below | the connection's auth, or `none` | How the read authenticates. Every value names an environment variable; a value that is not an upper-case variable name is refused, and so is a URL with a user name, a password, or a query parameter such as `token` or `api_key`. Messages and the preview never show a value. A redirect to another host drops the credentials. |
| `read.auth.bearer` | mapping | none | Send `Authorization: Bearer <value>`. |
| `read.auth.bearer.token` | variable name, required | none | The variable that holds the token. |
| `read.auth.header` | mapping | none | Send the value in a header of your choice. |
| `read.auth.header.name` | header name, required | none | The header, for example `X-Api-Key`. |
| `read.auth.header.value` | variable name, required | none | The variable that holds the value. |
| `read.auth.basic` | mapping | none | HTTP basic authentication. |
| `read.auth.basic.user` | variable name, required | none | The variable that holds the user name. |
| `read.auth.basic.password` | variable name, required | none | The variable that holds the password. |
| `read.auth.client_certificate` | mapping | none | A TLS client certificate, such as an X.509 grid proxy. |
| `read.auth.client_certificate.cert` | variable name, required | none | The variable that holds the certificate file's path. |
| `read.auth.client_certificate.key` | variable name, required | none | The variable that holds the private key file's path; it may name the same variable as `cert` when one file holds both. |
| `read.auth.client_certificate.ca_dir` | variable name | the system's trusted authorities | The variable that holds a directory (or one file) of certificate authorities to trust. |
| `read.paging` | `none`, `link_header`, or one mode below | `none` | How to fetch every page. Paging stops on an empty page or a missing next value. `link_header` follows the `Link: <...>; rel="next"` response header. |
| `read.paging.offset` | mapping | none | Offset paging: the offset grows by `size` each page, starting at 0. |
| `read.paging.offset.offset_param` | text, required | none | The query parameter that holds the offset. |
| `read.paging.offset.limit_param` | text, required | none | The query parameter that holds the page size. |
| `read.paging.offset.size` | whole number, required | none | The page size to ask for. |
| `read.paging.page` | mapping | none | Page-number paging. |
| `read.paging.page.page_param` | text, required | none | The query parameter that holds the page number. |
| `read.paging.page.size_param` | text, required | none | The query parameter that holds the page size. |
| `read.paging.page.size` | whole number, required | none | The page size to ask for. |
| `read.paging.page.first` | whole number | `1` | The first page's number. |
| `read.paging.cursor` | mapping | none | Cursor paging: each response names the next cursor. The first request uses the URL as written, so include the API's first cursor value (for example `cursor=*`) when it needs one. |
| `read.paging.cursor.param` | text, required | none | The query parameter that carries the cursor. |
| `read.paging.cursor.next` | path, required | none | Where each response holds the next cursor, for example `meta.next_cursor`. |
| `read.max_pages` | whole number | `1000` | A read that reaches this many pages while the API still has more is incomplete. |

An HTTP read sends GET requests with a 30-second timeout. A 429 or 5xx
response, or a dropped connection, is retried up to 3 times, honoring
`Retry-After` up to 120 seconds. A read that still fails, or answers with
something that is not JSON, is incomplete: the records of the other reads
apply and no record is removed.

Every run fetches every page; records that did not change write nothing.
When every page of the last run carried an `ETag` or `Last-Modified`
header and the server answers a conditional request for each page with
`304 Not Modified`, the run is skipped.

## Type keys

| Key | Type | Default | Meaning |
| --- | --- | --- | --- |
| `types.<type>` | mapping | none | One record type. The name must be new: not a built-in type name, not a type another source declares, and it must survive conversion to a class name and back (`sla_2024` does not; use `sla2024`). |
| `types.<type>.from` | path | none: the type reads the records themselves | Read a nested object, or each item of a nested list, of every top-level record as records of this type. A type without `from` is a top-level type. |
| `types.<type>.read` | read | the top-level `read` | This top-level type's own read, with the read keys above. A source may have several top-level types, each with its own read. Exactly one top-level type may use the top-level `read`. |
| `types.<type>.parent` | type name | the only top-level type | For a nested type when the source has several top-level types: the top-level type whose records hold it. |
| `types.<type>.key` | template, required | none | The record's key. Two records of the same type with the same key in one file stop the read. The same key in two files keeps the record from the later file. The graph identifier is `<source>:<type>:<key>`. |
| `types.<type>.title` | path | none | The record's title. Searchable. |
| `types.<type>.text` | path | none | The record's text. Searchable. Text longer than 2,000 characters is split into passages linked to the record. |
| `types.<type>.dates` | mapping | none | Dates of the record. Each value must be an ISO 8601 date-time. |
| `types.<type>.dates.created` | path | none | When the record was created. |
| `types.<type>.dates.modified` | path | none | When the record last changed. |
| `types.<type>.dates.published` | path | none | When the record was published. |
| `types.<type>.dates.resolved` | path | none | When the record was resolved. |
| `types.<type>.dates.effective` | path | none | When the record takes effect. |
| `types.<type>.changes_when` | path | none: the record's whole mapped content | A field whose value changes whenever the record does, such as `updated_date`. The record's revision is that value plus the manifest's mapping, so a record whose field did not change writes nothing. A record without a value there falls back to its content and is reported. |
| `types.<type>.fields` | mapping | none | Other values to keep, keyed by field name. `title`, `body`, `text` and the date names are reserved. |
| `types.<type>.fields.<field>` | path, or a mapping with `from` and `type` | none | A path alone keeps the value as text. |
| `types.<type>.fields.<field>.from` | path, required in the long form | none | Where the value is. |
| `types.<type>.fields.<field>.type` | `string`, `integer`, `number`, `boolean`, `date`, `datetime` or `list` | `string` | The value's type. A value that does not convert rejects the record and is reported. `boolean` accepts `true` and `false`; `date` accepts `YYYY-MM-DD`; `list` takes a list of plain values. |
| `types.<type>.links` | mapping | none | Relationships to records of this manifest, keyed by relationship name. The names `contains`, `has_part`, `parent_of`, `defines`, `owns` and `wraps` mean containment; two types that would contain each other are refused. |
| `types.<type>.links.<link>` | mapping | none | One relationship. It is written only when its target record is in the same run, from any of the source's reads; otherwise it is left out and counted. |
| `types.<type>.links.<link>.to` | type name, required | none | The target type. It must be declared in this manifest. |
| `types.<type>.links.<link>.key` | template, required | none | The target record's key. |
| `types.<type>.links.<link>.each` | path | none | A list in the record: one link per item, with `{.}` as the item. |

## Starting from samples

`okg source draft <file-or-url>` prints a first manifest from up to 200
sample records; see [`okg source draft`](cli.md#okg-source-draft).

## Changing a saved manifest

Running `add-source --manifest` again for a saved source may only add types,
fields and links, or widen a field from `integer` to `number` or `string`.
It may also change an HTTP read's URL, path, connection, auth and paging,
`max_deleted_fraction` and `changes_when`: the deletion guard bounds what a
changed URL removes. It refuses to remove or rename a type, field or link,
change a `key`, narrow a field's type, change a files read's `files`,
`format` or `records`, change an HTTP read's `records`, switch a read
between files and HTTP, change a `from` or a `parent`, or switch `deletion`
from `never` to `missing_from_full_read`. Remove the source and add it again
to make such a change.
