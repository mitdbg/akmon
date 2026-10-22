---
audience: user, researcher, data owner, developer
type: how-to
canonical_id: search
section: search
visibility: primary
status: active
owner: documentation
---
# Query and trace the graph

AKMON queries one published version of the graph at a time. Each result names
that version and carries the evidence of where the record came from. This page
shows how to search, keep related reads on one version, follow relationships,
and tell an empty answer from a failed one.

Run the commands where `okg` is on your path; inside a clone of the
repository, prefix each with `uv run`. Below, `<name>` is the deployment's
name. [How information becomes searchable](../../start/data-lifecycle.md)
explains when a change becomes visible.

## Find records

```bash
okg search --deployment <name> --query '<words or a record id>' \
  --limit 20 --json
```

By default this is a keyword search (`--method lexical`): it matches English
word stems, so `index` also finds `indexes`, and ranks by keyword relevance.
It returns at most 50 rows unless you pass `--limit`. Each row carries, among
other fields:

- `node_id`: the record's identity in the graph;
- `subtype`: the kind of record, such as `document_chunk` or `GitCommit`;
- `provenance.source`: the source that produced it;
- `source_record_id` and `source_revision`: the source's own identifier and
  revision, when the source supplies them.

The response also carries `generation_id`, the published version it read.
Keep it with the query, filters and limit: together they let someone rerun
the same search. A missing `source_record_id` means this response did not
carry one, not that the record has no source.

In a keyword search, `--select attrs,source_record_id` narrows each row to
the fields you name, plus a fixed set every row keeps: `node_id`, `subtype`,
`score`, `evidence_kind`, `evidence_kind_provenance`, and `provenance`. An
unknown field name is refused with the list of valid ones. Narrow a search
with `--subtype <subtype>`, with `--attr-filter <key>=<value>`, or with a
search profile or source family that the deployment declares (`--scope`,
`--source-family`). `okg search --help` lists every option, and the
[CLI reference](../../reference/cli.md#okg-search) describes them.

In text form (not `--json`), `okg search` prints a note under its first line.
`! search profile: none (unscoped)` means no search profile was applied. It is
not a warning: `--subtype` and `--source-family` still narrow the results.
When `--scope` names a profile, or the deployment's `search.default_profile`
applies, the note names that profile instead.

`--method semantic` ranks by meaning instead of words, over a filled
[vector set](../../operate/configure.md#embeddings), never mixing in keyword
ranking.

Put words in double quotes to ask for that exact phrase:
`--query 'rollback "pinned generation"'`. The keyword ranking itself ignores
the quotes, but results whose text holds every quoted phrase, word for word
and in order (any case, any spacing), come first, and each row then carries
`phrase_match: true` or `false`, kept even under a narrow `--select`. Two
exceptions: with `--rank-by passages` the rows are only marked, not moved;
and a search narrowed to one record type moves matches up only within the
part of the list whose order is known exactly, never past it. When no
result holds the phrase, the response says so (`phrase.status:
no_exact_match`, and in the text form `phrase=no_exact_match` on the first
line) and names the exact-text search to run instead: the MCP
`search(method="grep")` call with the same quoted phrases. A lone quote is read as ordinary text, and a phrase
written `-"..."` is not required, since a keyword search cannot exclude.

## Keep related reads on one version

A new publish can land between two searches. To keep a set of related reads
consistent, pass the first result's version to the rest:

```bash
okg search --deployment <name> --query '<words>' \
  --generation-id <generation_id from the first result> --json
```

To read an older version on purpose, list the published versions first:

```bash
okg generation list --deployment <name> --json
```

This lists published versions only, not failed attempts. Pinning a version
does not grant access, and it cannot bring back a version that is no longer
retained. If a pinned read fails, keep the error; do not quietly switch to
another version.

## Concept expansion

A deployment can declare groups of equivalent terms under
`search.concept_groups` in its configuration, for example a word and its
abbreviation. A keyword search then also matches the other terms of any group
the query names. Only whole-term matches count: words ignore case, and
notation keeps it. Rows found only through an added term rank below the exact
matches, and the response's `concept_expansion` block says what was added.
`--no-concept-expansion` searches the literal query.

## Follow relationships

Search finds candidate records. Two results being similar does not mean they
are related; a relationship is an edge stored in the graph.

To walk edges, use the graph read interface described in
[MCP graph reads](../../reference/mcp.md#a-reliable-read-sequence): `inspect`
describes one record, and `expand` follows its relationships, both at one
pinned version. Keep the relationship type, both endpoint ids, and the
supporting evidence with the result. If the interface cannot return a field,
report it as unavailable rather than inferring it from nearby records.

`okg trace node <node_id> --deployment <name>` is an operator diagnostic, not a
relationship read. It prints the record's fact log, the raw history of
changes the imports wrote. Its `--generation-id` option only pins the graph
preview it prints when that log is empty; it does not filter the history to
that version.

## Read the evidence

Provenance explains where graph content came from. Depending on the record,
it can include:

- the source and the source's own identifier;
- the source reader that produced the fact;
- the import that observed it;
- the rule and inputs that derived a relationship;
- the published version where it first appeared;
- any policy decision that affected whether it was accepted.

Provenance answers "why is this in the graph?" It does not prove that the
source's claim is correct. When a paper or report needs a citation, cite the
original source by its own identifier.

## Passage results

A passage (a chunk of a larger record) says what it belongs to, so you do not
need a second read to find out. When the deployment's catalog declares the
passage relation, a resolved `parent` (and each `candidate` when a passage
has several parents) carries:

- `title`: the first unmasked value of `title`, `summary`, `name`,
  `display_name`, `label`, `key` or `change_id`, first line only, at most 120
  characters. `title_attr` names the attribute used. A path is never a title.
- `status`: the parent's `status`, at most 40 characters, when it has one.
- `passage_count`: how many passages the parent holds at this version, or null
  above 1,000. When the deployment enforces read policy the count is always
  null, because it could count passages you may not read;
  `parent_relation.passage_count` says so once per page (on `inspect`, only
  when the count is withheld).

Text from an untrusted parent is fenced as untrusted data, by the parent's own
trust. Lexical and semantic results also carry `position` unless `select`
leaves it out. It is read from the passage itself:
`index` is the producer's 0-based `chunk_ordinal` or `chunk_index`
(`index_attr` names which), and `heading_path` is at most 120 characters. It
is null when the record carries neither. Inspecting a passage returns the same
`parent` and `position`; other records carry neither field.

Lexical search returns one row per record by default (`rank_by="records"`):
each matching passage folds into its record, the record ranks by its best
member (its own text or a passage), `rollup_best_chunk_id` and `position` name
the winning passage, and `matching_passages` counts the readable passages
that matched. `limit`, `offset` and `total_count` count records. A passage
whose parent cannot take it (not exactly one readable parent, no link, a parent outside your
scope or filters, or one you may not read) stays its own row. Naming a passage type in the filter asks
for passages: nothing folds and `result_unit` is `passage`. `rank_by="self"`
ranks each row by its own text, as before; it is also the default when you
pass `max_per_parent`, which asks for sibling rows. A profile's own
`max_per_parent` is not applied under `records` (`diversity_reason`).
A row that matched on its own text keeps the later rows of the same type and
text (copies at other paths): they leave the page and its count, and the row
lists their ids in `copies` (the first 10, with `copies_total` when more).

Filtered to a record type that declares passages (`subtype_filter=
"jira_issue"`), a record ranks by the better of its own text and its best
readable passage, so a long ticket with the answer deep in one comment is not
sunk by shorter near misses. Ranking reads stored text: a passage whose text
is masked for you still lifts its record, and you see the record with the
masked excerpt. Here a passage that does not fold is left out, so every row
has the filtered type. The order is exact only for the first
`ordered_exactly_through` records; see bounded results below.

The passages are read from the search index in best-first windows, so the
cost follows the matching passages rather than every passage of the type.
When that read cannot prove its answer complete, it uses the slower full read
instead, and the response says so in `passage_read_fallback`. The values are
`plan_not_index_scan` (the database did not read the index in order),
`index_statistics_changed` (the index changed during the read, for example
while it was being vacuumed), and `index_scan_cap` (the matches run past the
100,000 results the index returns per scan). The rows are the same either
way; only the time differs. The key is absent on the normal path, and
`ranked_cap` has no passage-index entry: the windowed read never returns a
partial answer.

In both shapes, for a single-user session, the passage links and the parent
records this search checks but does not return are recorded in the Nomos
audit trail as counted, not read; the rows it returns are recorded as read.
When the search filters on attributes (`attr_filters`) or a profile excludes
paths, parent records it fetches are read first, so a masked value never
matches. A session that identifies a user (multi-user HTTP principals) keeps
the per-row check and is slower, about 3.6 times an unfiltered search on a
large graph.

Upgrade note: a search that names no `rank_by` now presents
`rank_by: "records"` to Nomos. A policy grant that allows only
`rank_by: "self"` denies default lexical searches until it also allows
`records`.

The excerpt is the `max_chars` window holding the most distinct query words,
matched the way the ranker matches them (stemmed, stopwords ignored, whole
words only). `excerpt_window_basis` says how it was placed: `lexized`, or
`whole_word_fallback` when a page held too many candidate words.

## Understand bounded results

No request returns an unlimited graph. When more rows exist, the response sets
`truncated` to true and gives `next_offset`. Fetch the next page with
`--offset <next_offset>` and the same `--generation-id`, so every page comes
from one version.

Under `completeness: "exact"` (below), lexical `total_count` counts what the
ranked read found; by default it is null. `total_count_basis:
"lower_bound"` means a read hit its cap, named with its unit in `ranked_cap`
(for example `{"rows": 200}`), and more may exist. A filtered records page
also reports `ordered_exactly_through`: the records whose order is exact
(null on a default lexical page, whose candidates may be cut; paging still
stops at the exact prefix of the retrieved candidates). A
page never goes past them. Once the next page would, `next_offset` is null
with `next_offset_reason: "beyond_exact_order"`. An `offset` above 0 at or
past them is refused with `records_offset_beyond_exact_order`, but only when
the count is a lower bound; when the count is exact such an offset returns an
empty page.
Narrow the query to reach later records. When a read of a concept-expanded
query filled its limit and no read of the typed words did, the exact order
ends at the records the typed words matched, and `next_offset_remedy` (and
the refusal's `data.remedy`) holds the whole remedy, for example
`{"expand_concepts": false, "rerun_each": ["hafnium", "zircaloy"]}`: rerun
the same search with `expand_concepts: false` once per word in
`rerun_each` (the words of `concept_expansion.expanded_query`), one call
per word. Each rerun is its own ranking; the pages cannot be merged back
into the expanded query's order. Together they hold every record only when
each word matches fewer records than the candidate limit, which an exact
rerun proves by reporting `exhaustive` retrieval. When a read of the typed
words itself filled its limit, rerunning without expansion fills it again,
so no remedy is named and the page says to narrow the query. Under exact,
a page that reaches the end of the exact prefix under a lower-bound count is
`truncated`, even when no retrieved record lies past it. If a row the search admitted is then
hidden when it is read, the search fails with
`search_fold_read_check_mismatch` instead of returning an incomplete page.

One text-index scan can stop after 100,000 entries before subtype and other
filters run, so a lexical page can miss matching records even when it is
short or empty. By default (`completeness: "bounded"`) lexical search returns
the index's candidates quickly and says so: `total_count` is null with
`total_count_basis: "unknown"`, `coverage.retrieval` is `{"status":
"bounded", "basis": "bm25_index"}` (that basis is the possible cut; `bounds`
appears only for a filled candidate or work cap), and an empty page never
claims the words are absent. `coverage.retrieval.recovery` holds the
arguments for the next step, `{"completeness": "exact"}`: rerun the same
query with them from offset 0 (`--completeness exact` on the CLI). A
pre-joined bundle tool cannot pass that argument, so its page names no
recovery. The top-level `completeness` field names
the mode the page was read in. A record's `matching_passages` counts only the
passages the ranked reads retrieved; it is every matching passage only when
retrieval is `exhaustive`.

Exact mode scores every short read's whole scoped text input directly. It
permits at most 200,000 scoped rows and a five-second timeout (or the
caller's stricter one), and refuses with
`lexical_index_scan_cap` when it cannot finish. Then narrow by subtype,
source or records; more query words do not help. A page that continues an exact read must ask for exact again; the
`next:` hint says so. The two modes rank different candidate lists: do not
switch modes while paging; start again at offset 0.
On `grep` and a query-less `where`, `completeness="exact"` pages every match
(`exact_over_pages`) or refuses; `"bounded"` is every
method's ordinary reading. An exact lexical read filling its 200-row cap
adds an incomplete `listing` (`scan_bound`): exact
within the first 200 ranked; paging stops there. A filled work cap
names `bounds` instead; under enforced access no cap is named.
Semantic, alias and identifier-mention searches refuse `"exact"`: no
set of every match.

Exact retrieval is `{"status": "exhaustive", "basis": "exact_scalar"}` only
when no contributing read returned its full candidate limit, judged on the
read before attribute filters and path exclusions drop rows. Under access
rules the read already leaves out rows the reader may not read, where the
rules can be placed in SQL.
A read that returned exactly its limit counts as full. Otherwise it is
`bounded` with `bounds: ["candidate_cap"]` and the count is a lower bound,
however few rows the filters left. On a filtered records page, a full read
of the expanded query also ends the exact order at the first expanded record.

Under exact, `index_scan_cap_reads` names the reads that ran exactly and
`lexical_exact_reads` reports their stored scoped candidate and matching
text-row counts (not a global or folded record count). A filled candidate limit
remains a bounded selection with a lower-bound count. Under enforced access
rules the reads are measured before reader checks, so neither field is
emitted, `candidate_cap` is never named, and exact retrieval is `exhaustive`
or else `{"status": "unknown"}`. With `unknown`, `total_count` is null
with basis `unknown` and `ordered_exactly_through` is null, as on a default
page. Which rows set `rank_capped`, `ranked_cap` and a `lower_bound` count
depends on the page. With `rank_by: "self"`, or `"records"` without a
subtype filter, they come only from rows the reader may read; with
`"passages"` they follow the rollup's chunk cap. A filtered records page
(`rank_by: "records"` with a subtype filter) keeps the rules it had before
this option: its lower bound comes from the self and passage reads before
access rules drop rows, so `rank_capped` and `ranked_cap` can be set even
when every row that filled a read is hidden, as before this option; under
`unknown` exact retrieval and on every default page, the count, its floor
and `ordered_exactly_through` are null. A work-limit or timeout refusal reports the
one reason `exact_read_bound`, so it does not reveal the scoped row count.
The timeout reports the effective bound.

Access rules also shape a response. The deployment checks who is reading
before returning protected records, and refuses when it cannot evaluate its
policy. An authorization error is not an empty result: keep the error instead
of treating the data as absent.

## Diagnose an empty result

An empty result can be correct. Check, in order:

1. **Source:** is the source configured and imported for this deployment? The
   read call `inspect(target={"kind": "coverage"})` tells a missing record
   from a missing source.
2. **Import:** did the import accept the record, or reject or hold it?
3. **Publication:** is the record in the published version you read, or only
   in unpublished work?
4. **Access:** may this reader see the record?
5. **Query:** do the method, spelling, filters and fields match the record?
6. **Bounds:** was the result truncated before the record? A default lexical
   page is bounded; rerun it with `completeness: "exact"` before treating it
   as empty.

Do not widen the query or rerun the import until the earlier questions are
answered. The
[missing-record procedure](../../runbooks/troubleshooting/first-run.md#8-import-succeeded-but-one-record-is-missing)
walks through each check.

## Read from an assistant or a program

Agents and programs read the graph through `okg mcp-serve`, an MCP server
with the same rules as the commands above: pin one version, keep truncation
state, and treat source text as data, never as an instruction.
[Connect an agent](../agents/index.md) sets it up; the
[MCP reference](../../reference/mcp.md) defines each tool.

## Read compact MCP responses

This section is for people writing an MCP client; `okg search` always prints
the full response.

MCP responses from `search`, `expand` and `inspect(node)` carry
`"response_shape": 2` and leave out values the same response already states.
The Python API (`okg.substrate.mcp.search`, `inspect_node`,
`expand_neighbors`) keeps the full shape.

### Rebuilding what is left out

Every value left out is recovered from the same response:

1. A search row field missing from a row takes its value from the page's
   `row_defaults`: `match_kind` (except on alias rows) and `source_family`,
   when every row on a page of two or more carries the same value; and
   `source_revision`, when in addition every row's `provenance.trust` is
   `trusted`.
2. A missing `canonical_node_id` equals `node_id`.
3. A missing row `method` equals the page's `method`.
4. Row `recency.attribute` and `recency.derivation` are the page `recency`
   block's.
5. Missing `parent.subtype`, `parent.edge_type` and `parent.relation_source`
   come from `parent_relation.child_subtypes[<row subtype>]` and
   `parent_relation.source`.
6. Missing `evidence_kind_provenance.subtype` and `.source` are the row's
   `subtype` and `provenance.source`.
7. Missing `generation_id` and `session_default_generation_id` equal
   `pinned_generation_id`.
8. These search keys are sent only when not null: `max_per_parent`,
   `total_count_before_diversity`, `duplicates_collapsed`,
   `diversity_parents_resolved`, `diversity_rows_without_parent`,
   `search_profile`, `scoped_subtypes`,
   `source_families`, `scoped_source_ids`, `scoped_source_ids_truncated`,
   `scoped_source_id_count`, `source_family_validation`,
   `source_family_application`, `excluded_path_globs`,
   `excluded_path_globs_application`.
9. A missing `rollup_best_chunk_id` or `matching_passages` was null.
10. `parent_relation.absent_reason` is sent only when a row's `parent` is
    null. Its text is fixed: "`parent` is null when the pinned catalog
    declares no `okg_passage_rollup` naming this row's subtype as a child: the
    graph carries no passage relation for it, so there is no parent to cite
    and no query can produce one. A declared relation that reaches no parent
    is reported as an object with a state instead, so the two are never the
    same value."
11. A search row's `parent`, and each candidate in its `parents` list, keeps
    `state` and `node_id`. Its `citation`, `citation_source`, `title`,
    `title_attr`, `status` and `passage_count` are sent once per page, in
    `parent_summaries[<parent node_id>]`, unchanged (fences included). A
    summary that differs from that entry stays on its card.

The `pagination` object is not sent: `truncated`, `offset`, `limit` and
`next_offset` are at the top level. `inspect(graph)` and every other
operator keep the full shape.

### What never moves

A field named in `select` is sent on every row and never moved into
`row_defaults` or left out. Identity-floor fields, `provenance`,
`read_provenance`, `source_record_id` and every fenced value stay on their
row, and so does `source_revision` on any page with a row that is not
trusted. The one exception to the fence rule is a parent summary (rule 11):
that is the parent's own text, fenced by the parent's own trust, and moves
whole. Refusals are never compacted.

An MCP `search` call without `limit` returns up to 10 results (10 records
for lexical search); pass `limit`
(up to 1000) or follow `next_offset` for more. The Python API keeps its own
default of 50.

### Source lineage on request

On these operators `attrs` does not carry `nomos_source_lineage` when its
status is `ok`; a lineage with any other status stays, because it reports a
problem. Add `"source_lineage"` to `select` (on `search` with `method`
`lexical` or `semantic`, on `expand`, and on `inspect(node)`) to receive each
row's lineage as a row-level `source_lineage` field, fenced like any other
untrusted value; `null` means the row carries none. With only that token, a
lexical search keeps its default card and excerpt preview. Other search
methods omit it with an `adjusted` note.

The lineage is what lists every source of a merged or derived row;
`provenance.source` names only the producer.
