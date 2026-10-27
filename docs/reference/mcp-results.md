---
audience: developer
type: reference
canonical_id: mcp-results
section: development
visibility: primary
status: active
owner: architecture
---
# MCP result format

Every okg MCP tool result is sent once, as one text block in `content`. There
is no `structuredContent`, and no tool publishes an `outputSchema`. A model
reads the text described here. A program asks for the compact JSON of the
same dictionary instead (see [JSON for programs](#json-for-programs)).

A payload whose `status` is `"refused"`, returned or raised, is served with
`isError: true`. Its one block carries every field a caller needs to fix the
call.

## Parts of a result

A result has four parts, in this order.

1. **The status line.** Exactly one line. It is the only part programs parse.
2. **Notice lines.** Zero or more, each starting `! `, one per abnormal
   condition.
3. **The body.** Shaped per operator.
4. **Other fields.** A last `--- other fields` section holds every top-level
   field with no place in the grammar, and every placed field that is not in
   its normal state and was not printed in its place.

A placed field in its normal state prints nothing: a fresh freshness block,
`diversity_applied: false`, an unapplied concept expansion or recency
annotation, `pin_scope: transport_session`, `total_count_basis: exact`,
`complete` and `truncated` (the status line's ending says them),
`pagination` when `next:` says it, and the trust label of a trusted row.
Search pages print
`row_defaults` once under other fields. `inspect(target={kind: "graph"})`
always carries both freshness blocks whole.

### The status line

```text
status-line = tool SP status [SP code] *( " · " fact ) [ " · " size ] " · " ending
size        = bytes *( " · " knob )        ; from 2 KB
tool        = [A-Za-z0-9_]+                 ; the registered tool name
status      = "ok" | "refused" | "error" | "status=" payload-status
fact        = subject | method | where | counts | phrase
ending      = "complete" | "completion unknown"
            | "bound reached (" what ")" " · next: " ( "offset=" n | token | "continuation_token=" token | "none" )
            | "retry: " change              ; on refused and error lines
counts      = span " of " ( n | n "+" | "count unknown" ) | span ", more follow"
            | span " on this page of " n " read, not a total"
phrase      = "phrase=no_exact_match"        ; after the coverage facts
```

Every status line ends one way (PACT `read-model-s6-page-next`, design
`read-model` section 3.5). The ending is the last fact:

- `complete`: the page is the whole answer.
- `completion unknown`: a page with `coverage` (`search`, `filter`, `map`,
  `aggregate`) whose `coverage.delivery.complete` is `null`: nothing
  continues the read, and nothing proves every selected row was delivered.

A page with `coverage` ends as `coverage.delivery.complete` says, so the
text and JSON forms agree: `complete` for `true`, `bound reached` for
`false`, `completion unknown` for `null`. Delivery is independent of
retrieval: a rank cap with every selected row delivered ends `complete`,
and the cap shows only in the coverage facts (`retrieval bounded`,
`bounds candidate_cap`). Other pages end by their own bound facts.
- `bound reached (<what>) · next: <...>`: the page stopped at a bound,
  named: `max_bytes <n>`, `limit <n>`, `page_size <n>`, `rank cap <n>
  <unit>`, `exact order`, `payload bytes <n>`, `row cap` (`query`), or `page`.
  `next: offset=<n>` continues an operator that pages by offset (call it
  again with that `offset`); after an exact lexical page it reads
  `next: offset=<n> completeness="exact"`, and the next page is exact only
  if that mode is passed again. `next: <token>` continues a token read: pass
  the token back as `next` to the same tool, alone (`inspect(next=...)` for
  a `text` or `subtype` read, `expand(next=...)` for a path read).
  `search` takes no `next`, so a grep page ends
  `next: continuation_token=<token>`: pass the token back to `search` as
  `continuation_token` (see Grep search).
  The published schema accepts this next-only call. Omit `max_bytes`: a
  text, subtype or path continuation restores the budget its first page
  signed and checks it against the current deployment ceiling. A `target`
  or `input` that repeats, exactly, the read the token already holds is
  served as `next` alone, and the reply carries the adjusted note
  `next_repeats_read` naming what was dropped. Explicit numeric
  `max_bytes`, `select`, or a target/input that names a different read
  alongside `next` is refused (`next_with_read_arguments`). `generation_id` and `authority` may still be supplied and must
  match the binding. To read a page under a different budget, use the
  legacy spelling: a text or path token passed as `continuation_token`
  accepts any valid `max_bytes`, which overrides the signed one and is
  signed into the following token (see Byte budget). A subtype token binds
  its budget; restart that read instead. A refusal for a signed budget the
  ceiling now refuses, or for a path page that does not fit, names that
  call in its `retry:` (and in `error.retry_call`).
  `continuation_token` and `offset` are still accepted. `next: none` means
  nothing continues the read; narrow it instead.
- `retry: <change>`: a refusal's one-line `fix_hint` (or `remedy`); else
  the fallback it names; else `none given`. The error block below still
  carries every field, the hint included.

A `next` token is bound to its deployment, generation, catalog, target,
order, reader authority and policy, and the server's signing key. A token
refused by `continuation_token_invalid` says why in `data.reason`:
`signing_key_changed` (signed before a restart or key change),
`binding_mismatch` with `data.mismatched` naming the parts that differ,
`wrong_tool` (the retry names the tool), `kind_unroutable`, `malformed`,
`signature_invalid`, `payload_invalid`, `version_invalid`,
`token_unknown` or `cursor_invalid`.

A reply prints a continuation token as a 40-character handle,
`c1.<epoch>.<nonce>.<mac>`, when this server keeps the signed token it
stands for. The handle is kept by one server process: it ends at a restart,
after 2 hours, or when the process evicts it to stay inside its count and
byte limits. Every one of those reads `token_unknown` (a miscopied epoch
reads the same). When a deployment sets a shared signing key, the reply
prints the full signed token instead, which any process can open. A refusal
whose token did not open carries `error.retry_call` with `mode` `restart`:
the same read, sent again without the token. The `retry:` line reads "copy
the token exactly as printed, or restart: <call>" when copying again could
help (`malformed`, `signature_invalid`, `payload_invalid`,
`version_invalid`), and "restart: <call>" otherwise. A restart is offered
only when the server would serve it.

| status | meaning |
|---|---|
| `ok` | the payload has no `status`, or its status is `ok` |
| `refused <code>` | a payload with `status: "refused"`, returned or raised |
| `error <code>` | a raised error whose payload carries no `status` |
| `status=<value>` | any other payload status, copied verbatim, such as `status=writes_disabled` |

- Counts read `1-20 of 200`. `200+` means the count is a lower bound, and
  `200+ (cap 200 rows)` names the rank cap that made it one, with its unit.
  `of count unknown` means the page cannot count its set; a page with no
  count that is not cut and starts at row 1 counts its own rows.
- A search fact `passages` means every card is a passage (the filter named a
  passage type); `rank <mode>` names a rank mode other than the method's
  default (lexical: `records`).
- A search fact `where <predicate>` names the predicate the page applied:
  the page's `where_applied` when it carries one, else its scope as
  `type in [...]`, `source in [...]` (when the id list is whole) and
  `path not glob [...]`, joined by ` and `. No `where` fact means nothing
  narrowed the page.
- A status value, error code, or status-line subject (`node_id`, `subtype`,
  `direction`, `method`, the `rank` mode, a cap value) that holds a space is
  a JSON string. A cap value that holds a parenthesis or a comma is too.
- A search fact `phrase=no_exact_match` means a lexical query held a
  `"quoted phrase"` and no checked result holds every quoted span word for
  word (the page's `phrase` field says which were checked). It follows the
  coverage facts and precedes the ending; a `! no exact match` notice
  repeats it with the `search(method="grep")` call that finds the exact
  text. A quoted search that matched prints nothing here: the `phrase`
  field (spans, unmatched spans, matched and checked counts, status) is
  then in its normal state and stays in the JSON. On every quoted search
  each card says `exact phrase` or `no exact phrase`.
- Read replies name no generation. `inspect` of the graph adds
  `generation <id>` after the ending; the JSON form carries
  `pinned_generation_id`.
- A write or admin tool's raised exception is the SDK's own error text, with
  no status line.

### Notices

One `! ` line per abnormal condition, none otherwise:

- stale or unavailable source freshness, once per logical session and again
  when the stale set changes (a call pinning its own generation shares its
  session's state; without a stable session identity it prints every time):
  the oldest age, the count never polled, and up to five stale source names,
  never polled first, then oldest, ties by name;
- stale or unavailable publish freshness;
- a page cut to fit `max_bytes`, with the requested and served limits;
- a zero-hit diagnosis;
- a quoted lexical search with no exact match: `! no exact match for
  "<span>", ... among <n> candidates (retrieval <status>); results rank by
  shared words; exact text: search(method="grep", query="\"<span>\" ...")`,
  the grep call omitted when grep's index rule would refuse it,
  `among <n> candidates` when the page withholds its count;
- suppressed siblings, an applied concept expansion, a parent scan cap,
  withheld passage counts, recency annotation;
- a filtered records page whose order is exact only through a record, an
  excerpt placed by the whole-word fallback, and a profile `max_per_parent`
  not applied to records;
- a session `warning`, `schema_behind_by`, and a `pin_scope` other than
  `transport_session`.
- an `inspect` text read at a location: `! read starts at <chunk> @<offset>;
  earlier text was not sent` when it began after the first character, and
  `! location checked by offset only (no hash): changed chunk text is not
  detected` when `at` carried no hash.
- a rewritten call: `! adjusted: <sent> read as <served>; ...` (JSON
  `adjusted`: `{rule, sent, served}` records), for a clause with no `op`
  read as `eq`, `select` or `max_bytes` lifted out of `target` or `input`,
  and `select` fields the rows lack.
- `! not a total` and `did you mean`.

## Body shapes

```text
search ok · lexical · where type in ["ticket","document_chunk"] · 1-2 of 2 · complete

[1] ticket:DEMO-7 (ticket) · "Quota rule moved to storage" · created 2026-03-12T09:30:00+00:00 (attr="opened_at")
Rule moved.

[2] chunk:aaaa1111 (document_chunk) · "docs/handbook.md" · in doc:handbook, 12 passages · § Storage > Quotas
Each project gets 1 TB by default.
```

| operator | body |
|---|---|
| `search` | one card per row: the handle `[n] <node_id> (<subtype>) · <title> · <path> · <date>`, then facts (parent, heading, citation, trust and source), then the excerpt. Each of the three is optional and prints only when the record has it: the title and path as JSON strings, the date in ISO 8601; a record with none prints only `[n] <node_id> (<subtype>)`. A row with a `location` adds `@ <chunk>:<offset>` (the hash is in the JSON only), and then drops `best <chunk>` when it names the same chunk. One the read filter hid prints `title masked`, `path masked` or `date masked` in its place. `text_hash`, `source_revision` (also in `row_defaults`), `evidence_kind_provenance`, `score` and `distance` print only when the call's `select` names them; the `source_record_id` record prints too unless the handle shows a path. Default JSON retains the existing fields; explicit select projects normally. A parent reads `in <id> (<subtype>) "<title>", via <edge_type>, relation <source>, <n> passages, cite ...`; an unresolved parent reads `parent <state>` with its other keys as `parent.<key>=` facts and one `candidate <id> ...` fact per candidate |
| `inspect` node | `key: value` lines for the attributes, then blocks |
| `inspect` text | `[n] <chunk_id> @<offset>` per segment, then its text |
| `expand` | 1-hop: `[n] <node_id> (<subtype>) "<title>"` per linked record, with `via <edge_type> →` or `←`, `edge <id>` and `<n> links` when several reach it; a fenced or long title prints as a `title` block. A select of only link fields prints `[n] <edge_type> → <node>` or `← <node>` per link |
| `filter`, `map` | `[n] <id> (<subtype>)` per row, with indented `key: value` columns |
| `aggregate`, `query` | a tab-separated table: a header row, then one row per result |
| other tools and `inspect` kinds | `answer` first, then `key: value` lines and blocks |

A refusal:

```text
search refused semantic_search_unavailable · retry: retry with method='lexical'
message: search method='semantic' requires an embedder
fix_hint: retry with method='lexical'
fallback_method: lexical
fallback_operator: search
```

Fix-it lines come in a fixed order: `answer`, `message`, `fix_hint`, `remedy`,
`fallback_method`, `fallback_operator`, `allowed_operators`, `example`,
`schema_hint`, the other `error` keys, `data`, `warning`, `schema_behind_by`.

## Search handles

Search handles are available on lexical, semantic, attrs and alias rows, and
only when `select` is omitted or includes `handle`. Identifier mentions have
no handle: they count supplied-text occurrences. Alias handles describe the
canonical node without deduplicating alias occurrences or changing their unit.
Title and path retain that node's trust treatment, even for a curated alias.

A visible or masked date carries `date_meaning` and `date_attr`. Text names
`created` or `modified` and quotes the source attribute. A visible date that
names a calendar day carries `date_precision: "date"` (the catalog contract's
`precision` below): its value is written as that day's midnight, and text
prints the day itself (`created 2026-10-03`, or `2026-10-03-07:00` for a day
in a zone), never a midnight clock time. A date without it names a time of
day. Timezone state is not repeated: an
aware date keeps its offset and a naive one has none. A declared creation
date that is missing or invalid never substitutes modification; absent dates
add no placeholder or date provenance. Older handles lacking date provenance
still render their date. Handles without any visible or masked field are omitted.

## Search views

`view` chooses how selected records are delivered, never which records are
selected. Omitted or `"cards"` is the search card, `limit` default 8
(maximum 1000). `"list"` delivers `node_id`, `subtype`, `handle`, the
row's trust (`provenance`) and its `evidence_kind` when known; no excerpt,
text, attrs, score, parent summary, recency annotation, `"unknown"`
evidence kind or `evidence_kind_provenance`. Its `limit` defaults to 100 and refuses above 100, and a boolean
`limit` is refused, not read as 1; `select` keeps `node_id`,
`subtype`, `handle` and `parent`, and an empty `select` is refused. Order,
membership, counts, coverage and `next_offset` are those of the same cards
page, and `max_bytes` pages a list as it pages cards.

A list delivers records. A `subtype_filter` naming a passage type (a
records page of passages) is refused with `search_view_unsupported` after
the pinned catalog is read and before ranking. A passage the records fold
keeps as its own row (no readable record, or the passage an exact catalog
alias names) stays on the list, as on the cards, and is labeled
`"unit": "passage"`; a record row has no `unit`. A row whose card names a
resolved parent keeps that parent's id as `parent: {node_id}` (the text
line says `in <id>`), so a listed chunk says which document or ticket it
belongs to; the parent's summary and any unresolved parent state stay on
the cards.

| Method | `rank_by` | Views |
|---|---|---|
| `lexical` | `records` (default) | cards, list, metadata |
| `lexical` | `passages` | cards, metadata |
| `lexical` | `self` | cards |
| `semantic`, `alias`, `identifier_mentions` | `self` | cards |
| `attrs` | `self` | cards, list |
| `grep` | `records` (only) | cards, list, windows |

`metadata` delivers no rows: it returns a result reference to the selected
records and their counts (see Result references below).
`windows` needs `method="grep"` and reads a record `scope`, or without one
the grep's own top `limit` records; a `windows` call on another method or a
`children_of` scope is refused with `window_argument_unsupported` (see the grep record
windows below). A `list` anywhere else is refused with
`search_view_unsupported` before the database is read, including an
explicit `max_per_parent` (which selects `self`). The served matrix is in
`inspect(target={"kind": "surface"})` under `capabilities.search.views`.

In text, a list adds `list` to the status line and prints each record as
exactly one line: `[n] <id> (<type>)`, `passage` on a passage row, the
handle facts above, then the card's evidence-kind, trust and source facts.
A value a card would print as
a block is quoted inline instead (untrusted text keeps its inline fence), so
source text never starts a row or adds a fact. No excerpt follows.

### Every match, compactly: the grep list

`search(method="grep", query=..., view="list")` lists every matching record,
100 rows a page: `node_id`, `subtype`, trust, a resolved parent's id, and:

- `matches`: how many of the record's text units (its own text, its
  passages) match; not occurrences. Unless the page read every match, the
  numbers count units scanned so far (`matches_basis: "scanned"`, `3+` in
  text).
- `first_match`: `{chunk, line, text}`: the earliest match served in the
  record's own text, else in its first served passage; the 1-based line;
  that line cut to about 100 characters. Absent for a `path:`-only query.

A `select` cuts rows. Records, order, counts, bounds, continuation and
read checks are the grep cards page's own: a hidden record is never listed
or counted, and `completeness="exact"` gives the exact number of matching
records (passages under a passage `subtype_filter`) or refuses naming the
bound. One call reads at most 2,000 candidate text units.

## Values, keys and blocks

- **Inline values keep their JSON type.** Non-strings print as JSON (`null`,
  `1`, `true`, `[...]`). A string prints bare unless it would read as
  something else: empty, edge whitespace, a line break or control character,
  ` · `, any middle-dot lookalike (U+00B7, U+0387, U+2027, U+22C5, U+2219,
  U+30FB, U+FF65, U+2981 and others), any whitespace other than the ASCII
  space, a `"` or `\` anywhere, valid JSON (`"null"`, `"1"`), a literal
  word such as `True` or `yes`, or a number JSON would not parse (`007`,
  `+1`, `0x1f`). Then it prints as a JSON string. So `null` and `"null"`, `1` and `"1"` stay
  distinct, and a value copied back into a `filter` keeps its type.
- **Row headers and parents.** A row id or subtype holding `(` or `)` is a
  JSON string, so the header still splits into id and `(<subtype>)`. A
  parent value holding `, `, `(` or `)` is a JSON string, so the parent
  fact still splits into its parts.
- **NaN and infinities** print as `null`, in text and in JSON.
- **Tables.** A missing key prints as an empty cell; `null` and `""` print
  as themselves. A table line that reads as structure (a list cell `[1]`
  first) gets the leading backslash. The declared columns print even when
  there are no rows.
- **Keys** print bare only when they match `[A-Za-z_][A-Za-z0-9_-]*`, else as
  a JSON string.
- **Blocks.** A value that is fenced, contains a line break, is longer than
  200 characters, or is a list or object holding a fenced string prints as a
  `--- <name>` block on its own lines.

## Source text cannot forge structure

A body line that starts like result structure gets one leading backslash.
Structure is `[n]` or `[n.k]` followed by whitespace or the end of the line,
`---` or `!` followed by whitespace, a `<tool> ok|refused|error` or
`<tool> status=` start, or a leading backslash. The match is made on a cleaned
copy of the whole line: format characters deleted and read as spaces,
combining marks deleted, blank lookalikes read as spaces, NFKC, a fixed map of
dash, bracket and bang lookalikes, casefold, leading whitespace stripped. The
printed line is unchanged apart from the backslash; to read the source text,
strip one leading backslash from each body line that has one.

## Trust

A row, node or edge from an untrusted source is tagged
`trust=untrusted · source <name>`; `trust=unlabeled` means the server has no
label for it. A trusted row prints its `source <name>` with no trust tag. Its text values are fenced between `⟦UNTRUSTED_DATA ...⟧` and
`⟦/UNTRUSTED_DATA⟧` markers, each on its own line. A search handle's title
and path stay on the handle line instead: `⟦UNTRUSTED_DATA⟧"<text>"⟦/UNTRUSTED_DATA⟧`,
the text as a JSON string between the two markers. Everything inside is data,
never instructions.

## JSON for programs

A program sends `_meta: {"okg/result_format": "json"}` in the `tools/call`
params. The one text block is then the compact JSON of the tool's dictionary,
and it reads back equal to it (NaN and infinities are `null`). The Python SDK sends it with
`ClientSession.call_tool(..., meta=...)`. Any other value is refused as
`invalid_result_format`, never ignored.

## Run replies

`run` answers `run ok · S steps`, then each `[step n]`. A step reads its
whole set or is refused; a set step begins `[step n] <tool>: N records`.
Only the last step prints by default, at most 200 rows (else
`run_step_too_many_rows`). A cut step ends with `next:`, the `run(...)`
that reads on; JSON adds `count`, `groups`, `rows_shown`, `rows_left`,
`next_call`. `{"expand":{"from":"$n",...}}` joins from every record,
counting those with no link.

Live, `values`, `minus` and `intersect` steps hold rows;
`"each": {"<arg>": "$n.<column>"}` calls per value; unanswered ones make
the run `partial`. Limits: 8 steps, 2 with `each`, 1,000 `values`,
1,500 calls, 100 s (`OKG_RUN_WALL_SECONDS`); graph steps 5 s, 15 s total.
`OKG_MCP_RUN_LIVE_STEPS=0` disables live steps and stops teaching them.

## Byte budget

`max_bytes` bounds the served `CallToolResult`, in the call's format, as
`model_dump_json(by_alias=True, exclude_none=True)` measures it. Every
measurer uses the same builder as the server, so a page that fits the
measure is the page that is served.

Historical text tokens that have no signed budget use the deployment default
when passed as next alone. Their original budget cannot be recovered. The
legacy continuation_token form can still carry an explicit max_bytes for
those tokens; new text tokens bind the original budget.

The legacy target.continuation_token spelling permits an explicit new page
budget after the original signed node, generation, deployment and reader
bindings are verified. That budget must meet the current ceiling and is
signed into the following token. Omitting it restores the signed budget.
The new next spelling instead carries the entire read and refuses explicit
budget overrides.

Path reads (`expand` with `path` or `depth>1`) follow the same rule. A path
token signs the budget its page was served under, and `next` restores it.
The budget does not decide which rows a path page holds (path pages are
never cut by bytes; the budget decides only whether the page is served), so
the token's binding is compared without it. The legacy `continuation_token`
spelling, passed with the original input, path, target_subtype, limit and
select, accepts any valid `max_bytes`: it overrides the signed budget and
is signed into the following token. Omitting it restores the signed budget.
A path page that does not fit its budget is refused, and the refusal names
that legacy call with a larger allowed `max_bytes`.

When the page's budget is already the deployment ceiling, no larger
`max_bytes` exists: the refusal's `retry:` says so and names a restart of
the path read without a token, with a smaller `limit` or a narrower
`select`, and it carries no `error.retry_call`.

A subtype token binds its budget, which sizes the summary sections. When the
deployment ceiling drops below a token's signed budget, `next` is refused
and its `retry:` names the call that works: the legacy continuation with a
valid `max_bytes` for a text or path token, a restart for a subtype token.

A refusal that names such a call also carries it for a program as
`error.retry_call`:

```json
{"tool": "expand", "mode": "continue",
 "arguments": {"input": {"node_id": "doc:u"}, "path": ["contains:out"],
               "limit": 1, "generation_id": "<pinned generation>",
               "continuation_token": "<this token>"},
 "repeat": ["authority"],
 "max_bytes": {"min": 4097, "max": 65536}}
```

- `tool`: the tool to call.
- `mode`: `continue` reads the token's next page by the legacy
  `continuation_token` spelling; `restart` reads again without a token (a
  subtype token). A refused condition offers `corrected` (your input
  decides it) or `guess` (assumes a stored attribute), with `repeat`.
- `arguments`: the call's arguments, without `max_bytes`. The literal
  string `<this token>` stands for the caller's own token, which is not
  echoed: replace it with the token. A `continue` call names
  `generation_id`, the generation the token is pinned to; the legacy call
  without it opens the session default, so after a publish it would be
  refused `binding_mismatch`. A path read is rebuilt as a typed `path`, or
  as `direction` and `depth` for any-edge hops.
- `repeat`: arguments the caller passes again as before. Today this is
  `authority`, never echoed. Omit it if the first call omitted it.
- `max_bytes`: the inclusive range, `min` to `max`, of budgets the call
  accepts. Add `max_bytes` from that range to `arguments`.

### Read selected sources together

`inspect(target={"kind":"texts","items":[{"key":"a","node_id":"record:a"},
{"key":"b","node_id":"record:b","at":{"chunk":"chunk:b","offset":250}}]},
max_bytes=4096)` shares one response budget across up to eight requests.
Keys are unique, nonempty strings of at most 128 Python characters. Copy a
search location and generation pin as with single text; different keyed anchors
may target the same record. There is no per-item budget override.

Each item reports `delivered`, `partial`, `deferred`, or terminal `refused`.
Delivered means its requested extent has arrived: a whole record from its
start, the remaining record from an anchor with up to 200 leading characters,
or the named chunk alone. Existing source coverage is reported separately.
A located item carries the single read's `read_from` and `location_check`
on every page, and the text format prints them as that item's `!` notices.
A page that stops before every item is
finished ends `bound reached (max_bytes <n>) · next: <token>`.
An empty readable extent is delivered. A missing or stale source refuses only
that item; reader or generation mismatches refuse the entire continuation.

The server divides available space fairly, redistributes unused space, and
rotates unfinished requests using their original order. Pass `next` alone to
continue; it carries the original requests, budget, pins and unfinished
positions. Completed and refused requests do not reappear in that traversal.
Retrying the same token repeats the same logical page. The floor is 4096; the current deployment ceiling still applies.
A batch token signs its budget with the items, and the `texts` target has no
`continuation_token` spelling, so no call continues it under another budget.
When the ceiling drops below a token's signed budget, `next`
is refused and its `retry:` names a restart: `error.retry_call` has `mode`
`restart`, `target.kind` `texts` and the pinned `generation_id`, and its
`repeat` lists `target.items` and `authority`. The items are not echoed,
since eight keys and ids can outgrow the smallest budget: pass the same items
again. The restart reads every item from its start or anchor again.

Work metadata measures prepared visible text bytes and segments. The existing
5000-occurrence limit bounds source rows per edge family, not text bytes or
memory.

### Read several records together

`inspect(target={"kind":"nodes","node_ids":["ticket:1","ticket:2"]},
select=["attrs"], max_bytes=16384)` reads up to 50 records in one call. Pass
one of `node_ids` (1 to 50 unique ids) or `ref` (a result reference, read in
rank order 50 a page; `reference_members` says how many rows it holds, which
can be fewer than its search matched). `select` and `max_bytes` go beside
`target` and apply to every record, as for `node`. Any other shape is refused
with the form that works.

The reply stays within `max_bytes`. `records` come in request order, each with
a `status`: `ok` (as `inspect(node)` returns it), `cut` (shortened, with the
same `attrs_cut` and `read_whole` as a single node over its budget) or
`not_visible` (missing or hidden; the others are still read).

Space is shared fairly: a record that fits an equal share is whole and the
larger ones split the rest. Ids that could not be served even cut are listed
in `omitted`; the page is then `truncated`, `remaining_count` says how many are
left and `next` continues, alone or with a new `max_bytes`. If not even one
record fits, the call is refused `node_batch_budget_too_small` with
`required_budget_bytes`.

## Grep search

How `search(method="grep")` reads, ranks, bounds and counts; the other
search methods are in [MCP graph reads](mcp.md#search-methods).

`search(method="grep", query=...)` finds every text unit (one node's
`attrs.text`) that matches, through a trigram index, and returns records; a
matching passage folds into its record. Terms are AND-ed within one text
unit, never across a record's passages (`and_scope: "text_unit"`). Every
term must appear in one text: use 1-2 distinctive terms, and one
`re:/a|b|c/` for variants (synonyms, spellings, related terms):

- `word` matches as a substring (`uuid` matches `uuid4`);
- `"a quoted phrase"` matches its words in order, any whitespace between them;
- `a NEAR/80 b` matches both literals within 80 characters, either order (at most 255);
- `-term` excludes;
- `re:/<pattern>/` is a regular expression (at most 256 characters), matched
  by PostgreSQL. Back-references, lookaround, inline flags, named groups,
  `\b`/`\B` and escapes the two engines read differently are refused as
  `grep_query_invalid`;
- `path:<text>` (or `path:"a b"`) matches a case-insensitive substring of
  the record's `attrs.path`: the text unit's own path, or the path of the
  record it is a declared passage of. It ANDs with the other terms;
  `-path:<text>` excludes, and a text unit with no path at all is not
  excluded. To search text that reads `path:x`, quote it: `"path:x"`.

**Order and page.** Records rank by the BM25 score of the positive terms'
literal words (words, phrase words, `NEAR` operands), on lexical search's
node-text index; a record takes the best score of its text and matching
passages, and ties sort by node id. A substring inside a longer token
(`uuid` in `uuid4`) matches but scores 0, so it follows the BM25 matches.
`order_basis: "bm25"` and `rank_words` say so; an alternation of plain words
(`re:/rollback|revert/`) ranks on those words. Grep walks the index's ordered
scan, decides rows in batches (visibility, then the query on the values the
reader reads) and stops once the page and `next_offset` are certain, so the
page is the top of all visible matches (`page_basis:
"top_of_all_visible_matches"`) and its cost follows `offset` + `limit`, not
the match count. `offset` counts records in that order; pages neither repeat nor skip a record while the data stays
the same. A query with no literal word (only regular expressions or `path:`
terms) pages in node-id order (`order_basis: "node_id"`,
`order_basis_reason: "no_literal_words"`): in R1 (below) grep reads up to
2,001 matching units unordered and sorts them when there are at most 2,000;
otherwise it walks the scope in node-id order, checking each row, at most
50,000 rows in R1 and 6,000 in R2 (`id_rows`). Matches no BM25 word reaches
follow the same way.
A text unit that is a passage with no single declared parent is listed on
its own text, unless a subtype filter names a record type.

**Bounds.** Every bound counts rows of a sequence whose length does not
depend on what the reader may not see or read. Which sequence depends on
the regime, decided per call from the Nomos enforcement mode, never per
row:

- **R1**, Nomos `off` or `audit`: bounds count stored matching rows. A page
  takes at most 2,000 (`candidate_rows`); a match set of at most 2,000 units
  is read whole first, unless a term is a `NEAR`. With `off` the count reads
  at most 10,000 matching units in SQL. Under `audit` a refusal is logged and
  the row shown; rows `audit` still hides (no source lineage, masked text or
  deciding values) are neither charged nor counted. Unless audit can hide no
  row (no redacting rule, identity claim or access context; all rows with
  lineage), when the SQL count stands, the count judges the first 10,000 stored matching
  units (2,000 with a residual `where`) on the values the reader reads.
- **R2**, `enforce` (with or without a policy bundle): no bound depends on how many stored rows match. A word
  query reads in-scope BM25 rows in order and charges to the 2,000
  (`candidate_rows`) only units the reader may see and read. A query with no
  literal word, the matches no word reaches, and the count walk the scope
  (matching or not) in node-id order, charging every row: at most 6,000 for a
  page (`id_rows`), 2,000 for the count.

Both regimes read at most 50,000 positions of the BM25 scan (`index_rows`:
every row the words match, hidden or out of scope included, and tie re-reads;
below pg_textsearch's 100,000-per-scan limit). An R1 match set over 2,000
units is walked like R2's scope.
`attr_filters` and a profile's path exclusions are part of the predicate,
never of the scope the bounds count (exact equality on stored attributes;
the read values are checked again). A passage that does not fold into a
record is listed only if it passes the selections itself, as in lexical
search. A bound is reported only
when a further row exists past it: the result then says `page_basis:
"scan_bound_reached"`, `page_scan_bound_hit` and, when no `next_offset` can
be given, `next_offset_reason`, and the status line says "bound reached".
Such a page also returns a `continuation_token`: pass it back (with no
`offset`) to read on with fresh bounds, leaving out records already listed.
The token is signed and bound to the query, filters, scope, generation and
the reader's authority; its cursor (a scan position that counts hidden rows
too, plus the listed ids) is encrypted; its length does not depend on the
position. A continued page's `offset` is the records listed before it; its
next page comes by token too. Hidden rows change
how many calls a chain takes only through the index-row, node-id-walk and
timeout bounds. No token
is issued past 200 listed records (`continuation_withheld: "listed_cap"`),
when the scan cannot move past where it stopped (`no_progress`, for example
a tie of more rows than the index-row bound) or at pg_textsearch's scan
limit (`index_scan_limit`), or when it would be longer than 16,384
characters (`token_size`). The cursor also pins the chain's read path (whole match set, or index
windows and node-id walk); if a pinned whole read times out, the call stops
as timed out.
When the last index window allowed ends
inside a run of equal scores, its tied rows are served in node-id order and
`index_rows` is reported. A statement past the 5 s timeout gives
`page_basis: "timed_out"`. R1's unordered match-set read (stops at 2,001) and SQL count (stops at
10,001) are bounded by that timeout, not by rows; when the page's match-set
read timed out, the count is not run (`total_count` null,
`total_count_basis: "timed_out"`). `rank_scan`
says how scores were read when not from the index's scan (`expression`:
scored row by row; `match_set`: the page came from the whole match set,
scored with the `<@>` expression, which can differ by up to about 6% from
the index's score for rows still in the index's in-memory buffer).
`rank_read_text_differs` counts matched units whose effective
text differs from the stored text the index scored (an attribute overlay);
they match on the read text and keep the stored text's place.
`matching_passages_basis: "scanned"` means a record's `matching_passages`
counts the passages read before the page was complete. The index's word
statistics (document frequency and average length) cover every row, as in
lexical search, so rows the reader cannot see can change the relative order
of visible rows for any query, one word included (never which rows match).

An unscoped query needs one term with three literal letters or digits in a
row; otherwise it is refused (`grep_regex_unindexable`,
`grep_pattern_too_short`). With `path_prefix`, a `path:` term of three or
more literal letters or digits, `subtype_filter`, a profile, `source_family`
or `filter.source` it runs, bounded by a 5 s statement timeout
(`grep_timeout`). A `path:` term uses a trigram index on `attrs.path`;
grep refuses it with `SearchIndexUnavailable` until `okg migrate` builds
that index, and refuses `path_prefix` or a `path:` term with
`path_prefix_not_applicable` when no live, undeleted record at the pinned
generation has a nonempty string `attrs.path`. That check always runs; if it
times out, grep refuses
with `grep_timeout`, phase `path_applicability`, never an unchecked empty
result. Exclusion-only `-path:` still
runs on a graph without paths. `path_prefix` keeps text units whose `attrs.path`,
or whose record's `attrs.path`, starts with it. Each result carries up to
three `match_windows` (`line`, `char_start`, `char_end`, an excerpt, and
`passage_id` when the window is in a passage, with offsets relative to that
passage). `total_count` counts what the page lists (`count_unit`): records (passages
folded as the page folds them), or passages when the filter names a passage
subtype, with the page's own predicate; paging never reads it. Past its bound it is
`total_count_basis: "lower_bound"` with `scan_cap` (`rows` in R1,
`scope_rows` in R2), never below the page's own records; past the timeout it
is `"timed_out"` with no number (status line `count timed_out`, never
`of 0`). Under a policy that can redact, a row whose text, path or filtered
attribute the read would mask or drop is never counted, served, windowed or
ranked, and the count is a lower bound. A record whose path the
reader cannot read lends no path to its passages; under `-path:` such a
passage is not counted either. Case-insensitive matching follows
the database's locale; on a C-locale database only ASCII letters fold case.

**`where`.** Grep applies `where` as lexical's record view does: a clause is
judged on the record (a passage matches through its one declared parent, and
a passage whose readable parent fails is dropped), on the values the reader
reads, and only a true verdict admits. Its `id`, `type` and `source` clauses
become SQL in grep's predicate through the shared compiler; attribute,
title, path and date clauses stay residual, checked on every candidate the
scan decides (`where_evaluation_scope: "grep_scanned_candidates"`).
`where_qualification` says `exact` when the SQL decides the whole predicate,
`superset` when a residual remains. `type = X`, like `type in [X]`, judges
the record unless X is a passage type, which lists those passages (noted
under `adjusted`). A passage type beside other conditions (a `type in`
list, `or`, or under two `not`s) would never hold on the record, so it is
refused `unsupported_operator_form`, reason `passage_type` (`data`:
`passage_types`, `record_types`, `profile_all_needed`); the hint names the
record type, with `profile="all"` when the scope leaves it out, or the
passage type alone. With `within="record"`, a profile listing a passage
type reads the record types that declare it, whole, noted under `adjusted`
(rule `grep_record_passage_profile`). For the search profile, a top-level
`type = X` or `type in [...]` names those types as `subtype_filter` does: the
deployment default profile does not apply, and an explicit profile that
leaves one out refuses `subtype_outside_profile`. A query or `path_prefix` with no JSON form (an
unpaired surrogate) is refused `grep_query_invalid`, reason
`invalid_scan_value`. With a residual, the R1 count finishes
up to 2,000 stored matching units one by one rather than count stored rows
in SQL; past them it is a lower bound (`scan_cap: {"rows": 2000}`). A count is exact, and retrieval exhaustive (`coverage.retrieval.basis`
`grep_whole_sql` or `grep_terminal_keyset`), only when one statement returned
the whole match set or a node-id walk reached the scope's end, with every
candidate decided; a bounded, timed-out or continued scan never claims it.

## Record windows

`search(method="grep", query=..., scope={"record": id}, view="windows")`
returns, for each scoped record the reader can see, the literal source
windows around its matches. `scope={"records":
[...]}` takes up to 100 ids; a hidden record and a missing one both give no
row. Without a scope the call first runs the grep and reads its top `limit`
records (a continuation reads the records it was signed with); a grep that
matches nothing returns its own empty page. `matching="record"` (the windows default) needs every positive term
somewhere in the record's readable text and no negative term in it;
`matching="passage"` evaluates each readable passage alone and returns only
passages that satisfy the whole query. `context_chars` (integer 0-4096,
default 200) is the Unicode characters kept before and after each match.
Windows refuse, before reading the graph, a call without
`method="grep"`, a `children_of` scope, and any `where`, `path_prefix`, `offset`,
`rank_by` other than `records`, explicit `profile`,
`source_family`, `subtype_filter`, `attr_filters`, `filter` or
`max_per_parent`, a `path:` term, and a query with no positive text term
(`window_argument_unsupported`, naming the argument). `context_chars`, and
`matching` other than `"passage"` on grep cards (which already match within
one text), without `view="windows"` are refused the same way, and the
in-process `search()` refuses the same filters, families, excluded paths and
explicit `max_per_parent` passed through its arguments or scope. The named
records are the scope: a deployment's default search profile does not narrow
it, so a `doc:` or `file:` record outside the profile is searched. A scoped
record with no readable passage whose content a readable document holds (a
source file or code symbol, whose own text is a path or name) is listed in
`text_elsewhere` as `{record_id, document_id}`; scope the windows to that
document to search its content.

A record's readable text is its own `attrs.text` plus the passages its type
declares through the pinned catalog's passage relation, as this reader
reads them; masked passages are left out and counted in
`masked_occurrences`. The response's `evidence_domain`
(`readable_record_occurrences` or `readable_passages`) names that set:
absence of a negative term is absence from it, not from the original
document. Each record's `source_coverage` says separately whether positioned
passages tile the original source (`complete`, `partial` or `unknown`).

**Continuity.** Passages join into one searchable chain only when a
producer contract proves their positions: the occurrence edge and the
record come from the same producer, the deployment's source registry
declares that producer as the doc-corpus source, the edge names the
record's own source record and revision, it stores integer `char_offset`,
`char_end` and `doc_char_length` and its `separator_before` unchanged by any
attribute override, and the readable text equals the stored text. Then
verified overlap is trimmed and a stored whitespace separator fills a gap of
exactly its length; nothing else is joined. Equal lengths or matching
attributes alone never prove adjacency. A record's `continuity` is `proven`
only when each producer stream's readable passages form one chain that
covers the whole source (`source_coverage: complete`); any gap between two of
them or before the first or after the last (a paragraph stored once for two
places, a hidden passage, other missing text) makes it `unknown`, so a
hidden passage and a missing one give the same answer, like an unpositioned
passage or a generic passage link. That gap rule needs positions: for a
producer without position authority the read cannot tell how much source
text exists, so a record with one readable unpositioned passage (its sibling
hidden, or never stored) reads as `single` and decides a term's absence over
that passage alone. A hidden sibling and a missing one still give the same
answer there; neither is detected as a gap. `position_basis` counts why. An
unpositioned table or image caption is searched alone and never judges the
stream's continuity; its stored `chunk_kind` or `chunk_source` decides,
never an attribute override. Under unknown continuity a term found inside one
passage still matches, but a word, phrase, `NEAR` or regex term whose
presence or absence a boundary could change is undecided (`term_kind` names
it; a word is a substring and can straddle a boundary too). One named
record then refuses the call (`window_continuity_unknown`); in a
`records` scope that record returns `{node_id, decided: false, refusal:
{code, term_index, term_kind}}` with no windows, `undecided_records` lists
it, and `total_count` counts decided records with basis `lower_bound`; the
text status line says `N undecided` on every page, beside its ending. Such a
page is still the last one when no token follows (`delivery.complete: true`,
text `N undecided · ... · complete`): delivery is complete, the undecided
records are not. Use `matching="passage"` for passage-local evidence.

**Shape and counts.** Each result is one record (`node_id`, `subtype`,
`continuity`, `source_coverage`, `match_count`, `window_count`,
`readable_occurrences`) with `windows`. Overlapping or adjacent context
merges into one window; a window lists its `matches` (term index and
original chunk ranges), any `locations` of overlapping duplicate passages,
and `fragments`: the literal text with its chunk id, chunk-local character
`range`, 1-based `line_range` and provenance (untrusted text is fenced). A
stored separator is a `separator` fragment, never chunk text. `total_count` counts matching records,
while `window_count` and `match_count` count
merged windows and match events. `coverage.selection` (`unit: "record"`,
`basis: "matching_records"`, `size: null` when a record is undecided) is
the only qualifier of that count, as [Counts and
coverage](#counts-and-coverage) describes; delivered records are the
`results` list and the continuation is `continuation_token`. `work` counts
only readable occurrences and text, never stored rows the reader cannot
read.

**Bounds and paging.** One call searches at most 5,000 readable
occurrences and 8 MiB of readable text, both checked before any text is
scanned, and admits 10,000 match events within 2 s of scanning; past one it
refuses with `window_work_bound` naming it, never a partial or zero answer.
One record whose stored occurrence rows pass a fixed safety cap of 50,000
refuses as `raw_occurrence_scan`, with no count. The cap counts hidden rows
too, so the refusal is a one-bit signal per record that no other scoped
record moves. The whole call admits 100,000 readable
occurrence rows (`readable_occurrence_rows`, hidden rows never counted) and
10 s of wall-clock reading and scanning (`call_time`); both refuse by name,
never as a partial or empty answer. `limit` counts records per page.
Pages are fitted to the served `max_bytes` in the call's format, text or
JSON; a window larger than a page continues on the next page at the
character it stopped, with its matches and locations first. The
`continuation_token` is signed and bound to the query, scope, matching,
context, limit, budget, generation and reader; pass it back with the same
arguments (the text status line ends `next: continuation_token=<token>`, as
on any grep page; `search` takes no `next`). Every window part is delivered
once across the chain. The token
carries the room reserved for the server's per-call additions (freshness and
session notices; a first call reserves from its own), so a retried token
returns the same page while they fit; when they outgrow it the page can end
elsewhere, and its token still continues without loss or repeat. Every page
leaves at least 256 bytes over the additions measured at its start (for the
60-second freshness cache refilling mid-call); a new room adds 192 bytes
more, so a retry whose additions grew that much keeps its page. Metadata that cannot fit with one
character of text is refused as `window_budget_too_small`.

## Semantic search

Semantic search ranks the set `embeddings.search_set`
([embeddings](../operate/configure.md#embeddings)) over rows visible at the
pin, in scope, readable, with a vector under the pin's watermark. Up to
5,000 nodes with non-blank text rank exactly; more walk HNSW (`ranking: approximate`), and
a walk stopped at `max_scan_tuples` adds `listing: {complete: false,
reason: "scan_bound"}`. `attr_filters`, `where` and excluded path globs
filter after ranking, so a page can be short. Pages name `vector_set`
(`status`; `index`, with `rebuild` while a new index is pending) and
`embedding_coverage` (`text_nodes`, `embedded_nodes`; withheld under access
rules). A set `unregistered`, `inactive`, `no_vectors` or `filling`
(`unfilled` under access rules) falls back to the node columns.

Refusals: `embedding_watermark_missing`, `semantic_index_unavailable`
(index missing, building or invalid, or pgvector below 0.8),
`semantic_scope_too_large` (`index.kind: none` above 20,000 nodes) and
`embedding_model_skew` before the query is embedded;
`semantic_query_embedding_failed` after.

## Counts and coverage

Successful row responses from `search`, `filter`, `map`, `aggregate` and
one-hop `expand` also carry `coverage` with three independent guarantees:

- `retrieval.status`: `exhaustive` only with producer-established exhaustion,
  `bounded` when a scan stops at a known bound, `approximate` for semantic
  ranking, or `unknown`. `basis` identifies the proof and `bounds` identifies
  reported candidate/work caps; `result_reference` means a `filter`, `map` or
  `aggregate` read only the `reference_members` rows its reference holds. A
  short page is not an exhaustion proof.
- `selection`: the `unit` (record, passage, row or group), `basis` (matching
  set, retrieved candidates or input set), `size: null` when the full size is
  not proved, and a declared membership `limit`. The ordinary top-level
  `limit` bounds a delivered page; it does not establish selection membership.
- `delivery`: `complete` (true, false or null when unproved) and output-byte
  `bounds`. This is delivery of the selected set, not proof that retrieval
  found all relevant evidence.

An absent coverage field is empty or derivable, never unknown: empty `bounds`
and an undeclared `limit` are left out; an exact `size` is `total_count`
(basis `exact`); delivered rows are the row list and a usable continuation is
`next_offset`, `next` or `continuation_token`. Unknown stays explicit:
`size: null`, `complete: null`, `status: unknown`.

`coverage.selection` is the only place that qualifies `total_count` and its
basis; no top-level copy is emitted. Exact semantic candidate counts and
diversified survivors count `retrieved_candidates`, not all relevant matches.
Aggregate counts count groups, not input records.
Under Nomos `audit` or `enforce` with a policy that can redact, lexical,
semantic and grep search keep a matched text unit (a record, passage or chunk) only
when the reader reads its text exactly as stored: a match in masked or
withheld text is left out before any count, page fact or record fold, as a
hidden row is, so it never lifts its record.
Lexical search fills `retrieval` itself. By default it is `bounded` with
basis `bm25_index` (one index scan can stop before filters run) and
`recovery` `{"completeness": "exact"}`: the arguments to rerun with, from
offset 0. Under `completeness: "exact"` it is `exhaustive` (basis
`exact_scalar`) only when no contributing read returned its full candidate
limit, else `bounded` with `candidate_cap`, and `listing` (not complete,
`scan_bound`) names the cap and a literal grep.
Under enforced access rules it is `exhaustive` or `unknown`, never naming a
cap; `unknown` nulls the count. The capped reads apply the reader's access
rules in SQL before their limits, so rows the reader may not read never fill a
limit. Where those rules cannot be placed in SQL (any session with an
identity claim, as in every multi-user deployment, or an access context; a
rule on an unsupported key; no read-class catalog), the reads still run
before the per-row filter: there
`rank_capped` comes only from readable rows, except on a filtered records
page, whose lower bound (`rank_capped`, `ranked_cap`) comes from reads taken
before access rules. A filtered records page whose exact order a filled expanded
read ended, while no typed-word read filled, names `next_offset_remedy`
`{"expand_concepts": false, "rerun_each": [...]}`: rerun once per listed
word, one call each; the per-word rankings cannot rebuild the expanded
order, and they are complete only when each word matches fewer records than
the candidate limit (each exact rerun `exhaustive`). The top-level
`completeness` string names the requested mode.
Identifier mentions count occurrence rows in the supplied text (`input_set`),
with retrieval basis `input_text`. Exhaustion there describes that text, not
all graph matches or a count of unique graph records.
Existing subtype-valued `total_count_unit` is retained and means records of that
subtype; canonical granularity is in `coverage.selection.unit`. An explicitly
unknown count is null even if a producer supplied a numeric zero. A lower bound
retains its numeric floor. Future reference consumers must retain the selected
set's provenance rather than turn its count into a global matching count.

Coverage is included in response-byte fitting. After fitting, delivery
reflects the final rows and continuation, or reports partial/unknown delivery.
It never converts missing continuation into exhaustive retrieval.
For responses carrying delivery coverage, the `complete` page ending
requires `delivery.complete: true`: `null` ends `completion unknown` and
`false` ends `bound reached`, with its `next:` cursor when one exists;
retrieval status remains an independent fact.
A proven exact selected set can be delivered completely despite a retrieval
cap; an output-byte cut or remaining delivery cursor still makes it partial.
`filter`, `map`, `aggregate` and one-hop `expand` count the selected set
exactly when no row was hidden from the page: from the page itself when no
row lies past it, otherwise from a bounded COUNT over the page's own SQL
(100,000 rows, 5 seconds). Past the row bound the count is a lower bound;
past the time bound it is unknown, never zero. When the reader's policy is
evaluated (Nomos audit or enforce), a later page or attribute-lookup page has
an unknown count, but when its scan read past the page and found nothing more
it is the last page and reports `complete: true`. `truncated: false` alone is
not that proof.

The text form prints coverage's retrieval status, count set and bounds,
never a delivery fact: the status line's ending (`complete`, `completion
unknown`, or `bound reached ... · next: ...`) says delivery, once.

## Catalog date meanings

Named dates separate a question's meaning from the attribute a source uses.
These five meanings are the initial supported set, not all possible domain dates.

| Meaning | Class annotation | Subtype metadata |
| --- | --- | --- |
| `created` | `okg_created_at_from` | `created_at_from` |
| `modified` | `okg_last_modified_at_from` | `last_modified_at_from` |
| `published` | `okg_published_at_from` | `published_at_from` |
| `resolved` | `okg_resolved_at_from` | `resolved_at_from` |
| `effective` | `okg_effective_at_from` | `effective_at_from` |

Each annotation names one declared attribute. Composition refuses invalid
names and unknown attributes. A deployment can add a declaration, but cannot
change a library's existing declaration. Publication, resolution and effective
annotations describe read-side meaning; they do not stamp timestamps on write.

`resolve_named_date(metadata, attrs, meaning)` returns `value`, `attr`, `status`,
`meaning`, `timezone` and `precision`. It reads only the requested meaning's declaration.
An absent effective date remains absent even if creation time exists. Missing
declarations remain `not_declared`; missing values, hidden values and invalid
values remain `absent`, `masked`, and `unparseable`, respectively. Unknown
meanings raise `ValueError`; an operator using this API must return a structured
refusal with the supported meanings rather than exposing that internal error.

The generic handle date remains display metadata: creation time when declared,
otherwise modification time. `HandleFields.date_meaning` identifies that choice;
`HandleFields.date.attr` identifies the source attribute. A missing creation
value never falls back to modification. `values()` still returns only title,
path and date. Display defaults are not predicate defaults.

ISO-8601 and RFC 2822 strings normalize to ISO-8601. `precision` says what
the source named: `date` for a calendar day with no time (ISO `2026-10-03` or
`20261003`, or an ISO week date with its day such as `2026-W40-6`), `datetime`
for a time of day, and `null` when there is no value. A `date` value is written
as that day's midnight so it sorts with datetimes; it is not an instant at
midnight, and `precision` is the only thing that tells it apart from a source
that wrote `00:00:00`. An extended day followed directly by `Z` or `+HH:MM`
is the XML Schema date form (`2026-10-03-07:00`, `2026-W40-6+05:30`,
`2026-10-03Z`): `date` precision with the offset kept, a day in that zone,
never a clock time. Its offset may not exceed 14:00. Compact or hour-only day
offsets (`20261003-1230`, which is more likely a build stamp, `20261003+05`,
`2026-10-03-07`) and offsets beyond 14:00 (`2026-10-03-23:59`) are
`unparseable`. An ISO week without a day (`2026-W40`) names seven days, not
one date, and is `unparseable`.

Accepted ISO-8601 text, ASCII digits only: the day as above; then nothing, the
XML Schema day offset, or a time after exactly `T` or one space; after a time,
an optional offset, which may follow one space (`2026-10-03 12:00:00 +0000`,
as Git `--date=iso`, Ruby and Go write it). A time offset is `Z`, `+HH`, basic
`+HHMM[SS]` or extended `+HH:MM[:SS]`; a mix such as `+05:3000` is refused.
Anything else is `unparseable`, including another separator
(`2026-10-03.12`), a lower-case `t`, two spaces, and any digit string other
than an eight-digit `YYYYMMDD` day (`4460012345`, `2026100312304`): epoch
seconds versus milliseconds is ambiguous, so numbers are never guessed.

`timezone` is `aware` when the source gave an offset (`Z`, `+02:00`, `GMT`,
`-0700`) and `naive` when it gave none or said the offset is unknown: RFC 3339
`-00:00` (also `-00` and `-0000`) and RFC 2822 `-0000` mean "offset unknown",
not UTC, and return a naive value. No timezone is invented. Also refused as
`unparseable`: invalid calendar dates; offset components out of range anywhere
in the value (`+00:60`, `+0260 (CEST)`); and an email zone name RFC 2822 does
not define (`CEST`), whose offset cannot be known from the name.

Email (RFC 2822) dates: an optional weekday, which must be a real day name
(`Mon` to `Sun`); it is redundant, so one that contradicts the date is ignored
and the numeric date wins. Years follow RFC 2822 section 4.3: two-digit years
00-49 are 2000-2049, 50-99 are 1950-1999, and three-digit years add 1900.
Digits must be ASCII. An offset written with a colon (`-07:00`), which RFC 2822
does not allow but real archives contain, is kept as an aware offset.

A consumer that compares, sorts or groups named dates must define or refuse a
comparison between a `date`-precision or `naive` value and an `aware` instant.
It must never read the midnight wall time, or any naive value, as UTC.

This slice supplies the catalog and resolver. Predicate, sorting, grouping and
handle wire consumers are delivered by the dependent read slices. They must
use a named meaning, identify the resolved attribute, and refuse incompatible
aware/naive comparisons. Other domain dates remain available through explicit
attribute predicates with date validation. No generic `date` predicate default
is introduced here.

## Search predicates and named dates

`search(where=...)` accepts the predicate tree on id, type, source, catalog
title/path, explicit attrs fields and dates. Canonical `title` and `path` read
each record's declarations at the session's pinned catalog version. The raw
attribute spelling `attrs.path` retains its historical meaning. A record
predicate tests parent metadata, rather than a matching chunk's metadata.
Title predicates use the full first line; the 120-character display cap does
not limit equality or suffix matching, and later message body lines are excluded.

Dates use `date.created`, `date.modified`, `date.published`, `date.resolved`
or `date.effective`. For another domain date, use `date.attrs.<key>`, such as
`date.attrs.valid_until`. Plain `attrs.<key>` operands retain their existing
literal string/object semantics. Generic `date` refuses; the display date is
not a predicate default.

Supported date operations are `eq`, `in`, `lt`, `gt`, `between`, and `exists`.
Invalid operands and date prefix/suffix/contains operations refuse before
database reads. Comparison follows "Date conditions" below. Operands keep
their day form in `where_applied`.

Missing or malformed row dates remain unknown under comparison and negation,
with no substitution from another meaning. `exists` tests valid date
availability: missing/invalid is false; masked values remain unknown even
under `not exists`. Catalog aliases no scoped type declares refuse explicitly.

`where_applied` is the short text of the whole applied predicate, old filter
arguments included. It is present only when the caller supplied `where` or an
old filter argument; a call with neither carries no `where_applied` key.

Responses disclose bounded unique `where_fields` mappings and candidate
`where_missing`/`where_unparseable` counts. Lexical checks before paging,
on ranked candidates; grep, on candidates its scan decides; the query-less
lookup, on a bounded SQL page (`where_evaluation_scope`). A capped read
checking `where` or `attr_filters` after ranking adds `where_checked_on`
(`ranked_cap`). Under enforced access rules any such page, capped or not,
carries `{"ranked_rows_only": true}` instead: no number. These diagnostics are not
exhaustive global counts. Shared scanners must provide safe pre-retrieval
filtering and completeness; no unsafe SQL timestamp cast or raw attribute
equality is used for a typed date clause. The lookup page is narrowed in SQL by
exact `type` clauses, by exact `id` clauses, and by a superset test on the
stored attribute each scoped type declares for `path`, `title` (only when
every scoped type declares it) or a date. The `id` and attribute narrowing
apply only under Nomos `off`, where no value can be masked; otherwise those
clauses stay on the read rows. A lookup should therefore name a type, and an
exact path should use `attrs.path`; both narrow the page under every mode:
`{"op": "and", "clauses": [{"op": "eq", "field": "type", "value": "source_file"}, {"op": "eq", "field": "attrs.path", "value": "src/app.py"}]}`.
Lookup paging stops at offset 10,000. A page whose next page would start past
it carries `pagination_capped`, `pagination_max_offset`, a `pagination_notice`
and no `next_offset`; matches past the ceiling are reached only by narrowing
the `where` (a type or another exact clause), never by paging.
An `attrs.<key>` equality lifts into `attr_filters` under every Nomos mode,
exactly as the `attr_filters` argument does, so both spellings run the same
code. Both shape ranked bounds and the lookup page from stored values: under
`audit` or `enforce` a stored value on a maskable attribute can shape those
bounds, as on dev, until the reader's view (design slice 7, #3208). Every row
is still checked on read values. `source` is never narrowed on the lookup page.

### Ranked prefilter

Lexical search turns the parts of `where` on `id`, `type` and `source` into
SQL on the ranked query, so the 200-row ranking cap is spent on rows that can
match. It does so only where each ranked row is judged by itself:

- `rank_by="self"`: always.
- `rank_by="records"` with a `subtype` filter left in SQL: a passage subtype
  (rows are passages), a record subtype with no passage relation, or a record
  subtype with one when no record `scope` is given (the ranked rows are
  records of that subtype).
- `rank_by="records"` with no `subtype` filter in SQL (the default, or a
  record `scope`, which turns a record subtype with passages into a `where`
  clause): not used. Passages are judged through their record, whose `id`
  and `type` differ, so these clauses are checked only on read rows.
- `rank_by="passages"`: not used, for the same reason.

Every clause is still checked again on the read row. Attribute, title, path and date clauses never become SQL:
they stay true in the ranked query and are checked on read values, so masked
and pseudonymized values compare as the reader sees them. `where` values that
have no JSON form (NaN, Infinity, or text with an unpaired surrogate) are
refused with `where_unsupported` and reason `invalid_where_value`.

Bounded searches checked after ranking rank 1,000 rows (not under `scope`,
`passages` or a filtered fold); their substring read stops at 500 ms
(`literal_read_bounded_ms`).

Two changes in this compiler can only add rows the reader accepts, never
remove them:

- `lt`, `gt` and `between` on `id`, `type` and `source` compare text in
  code-point order, as the reader does (SQL `COLLATE "C"`), not in the
  database's locale order. Under a locale collation the old SQL could drop
  rows the reader accepts. A locale-collated btree index cannot serve these
  comparisons; a future keyset scanner needs keys indexed with `COLLATE "C"`.
- An `or` that mixes a stable-field branch with an attribute branch (for
  example `type = ticket and attrs.status = open`, or `type = comment`) now
  narrows the ranked query instead of ranking every row. When other rows
  would have filled the ranking cap, more matching rows can now appear.

`filter`, `map`, `aggregate` and one-hop `expand` compile their predicates
with the same compiler over the effective-attribute relation (#3201). Caller
values are bound parameters, and each response carries
`predicate_fingerprint`, a hash of the compiled predicate. An ordered
`filter`, a set `aggregate` and a filtered or ordered one-hop `expand` no
longer go through `query`'s SQL path, so they compare the same corrected
attribute values the other reads serve. A generation without the
effective-attribute relation refuses them with `unsupported_operator_form`.

### Children of one record

`scope={"children_of": id, "edge_type": name}` limits a lexical search to
the records `id` reaches by its outgoing `edge_type` edges, never `id`
itself (#3363). Without `edge_type` it uses the record's declared passage
relation (a document's chunks, a ticket's comments); a record that declares
none is refused with `search_scope_unsupported` and asked for an
`edge_type`. Each child is ranked by its own text: `rank_by` is `self`, and
any other rank mode, `view="list"`, `"windows"`, `"metadata"` and `retain`
are refused, because a fold or a record view could land on a record outside
the scope (the excluded parent, or a hidden child through its readable
passage). Under Nomos a parent the reader may not see answers exactly as an
id that names nothing, and a hidden child is dropped like any hidden row.
The child edges are checked for liveness, not for Nomos visibility, as a
record scope's passage edges are: an edge the reader may not see still
selects a child the reader may see. `scope_applied` echoes the scope as the caller wrote it.

## Counts per date period

`aggregate(input={"kind":"nodes","subtype":"ticket"},
by=["month(date.created)"], metrics=["count"])` counts records per calendar
period of a date. A period field is `<period>(<date>)`: `<period>` is `day`,
`week`, `month` or `year`; `<date>` is a named date as `search(where=...)`
names it (`date.created`, `date.modified`, `date.published`, `date.resolved`,
`date.effective`, read from the attribute each record's type declares) or
`date.attrs.<key>` (that attribute on every record).

A group's key is the period's first day, `YYYY-MM-DD`; weeks start on Monday;
groups come oldest first. Records with no value form one group with a null
key, listed last. A period works beside plain fields, filters, paging and
result references.

A date range in `filter` follows "Date conditions". A `between`, `gt` or
`lt` on a period's own date, alone or directly inside one top-level `and`,
also reports `date_filter`: the ranges and how many records they left out.

`date_fields` names the attribute each period field read, per record type
(`{"*": "<key>"}` for `date.attrs.<key>`). Values that are not dates are not
grouped: `date_unparseable` gives their count per field (a
record bad in two fields is in both counts). A masked value counts as no
value. The call is refused only when no selected value is a date
(`reason: not_a_date`) or no selected type declares the named date
(`reason: field_not_declared`). Counts are exact. One call reads at most
250,000 distinct stored date values; past that it is refused.

A count over a result reference covers only the rows it holds
(`reference_members`).

## Date conditions

`date.<meaning>` and `date.attrs.<key>` are condition fields in `search`,
`filter`, `map`, `aggregate` (with or without a period in `by`) and on
`expand`'s far end. A date is an instant; a bound is a day or a
timestamp; a day is its first instant, except as the upper bound of
`between` (its last) and with `eq` or `in` (that whole day). No time zone
means UTC, noted in `dates_read_as_utc`. A value that is not a date matches
nothing and is counted (`date_unparseable`, over the records the call's
other conditions select; `where_unparseable` on search). Under `not`, search
leaves out a record with no date; the set operators keep it. Under Nomos
`audit` or `enforce` the set operators compare only ISO-8601 stored values
(another form matches neither a condition nor its `not`) and say so
(`date_forms_compared`, `date_forms_reason: access_rules`). Otherwise the
page (or count) is read first and other stored forms are compared exactly
after it, unless they cannot be examined in the time left, at most 1.5 s
(`date_forms_reason: scope_too_large`); `date_forms_remedy` names what
narrows it. A cut page whose total is not counted within 1 s says
`total_count_reason: count_not_computed_in_time`. A call's date work has
one 5 s budget: only a page that cannot be read in it is refused
(`date_call_over_budget`) and told what to add. A declared `ts` index on
the attribute narrows the read.

## Group keys and list elements

`by` is checked first. An unknown key is refused by name with the set's keys
and any corrected call. `link:<type>:<in|out>` on records is refused with the
call counting those links, offered when the conditions name record ids alone.

`each(attrs.<key>)` gives one group per list element, counted once per row.
Replies add `rows_counted`, `rows_with_none` (the null group),
`rows_not_a_list` (in no group) and `groups_overlap: true`. A count over
links or elements taking over 5 s is refused.

## Location anchors

Copy the returned location and keep the generation pin. Its offset counts
Python characters; negative and boolean offsets are invalid. The optional
hash checks the reader-visible chunk text and is supplied by search; callers
do not compute it.
`inspect(target={"kind":"text","node_id":...,"at":...})` delivers the remaining
record from that anchor with the documented leading context. Its delivery
completion does not include earlier omitted text. An at-only call reads that
chunk alone. A text token signs the
node it reads (the record, or the chunk for an at-only read) and its
position, not `at`, so the `error.retry_call` for such a token names that
node and no `at`, and resumes where the token stopped.

Two fields make a located read's limits visible. `read_from` =
`{chunk, offset, text_before_sent: false}` is on every page of a read whose
requested extent starts after the first character: `chunk` and `offset` name
where it starts (the offset counts characters of that segment's text, the
basis of `text_range`), and the text before it was not sent. So `complete:
true` with `read_from` means the extent from there to the end was delivered,
not the whole record.
`location_check: "offset_only"` is on the first page when `at` had no hash:
the location was checked by chunk and offset alone, so changed chunk text at
the same offset is not detected. A chunk read's `next_move` copies the
caller's hash into the `at` it suggests. A hash that is not `sha256:` and 64
lowercase hex digits is refused as `invalid_operator_arguments` before any
read.

## Line ranges

`inspect` text and each `texts` item take `lines`: 1 to 20 ascending
`[first, last]` ranges, ends included; `[first, null]` reads to the end;
not beside `at`. A line is a line of the served text, split on line feeds
only. Every text page has a `lines` block: `basis` is `source` when the
numbers are the source text's, else `shown_text` with a
`! lines:` notice; `total`; and `first`/`last`, or on a line read `served`,
`past_end` and `cut` (`last_line`, `next_line`). Hidden text has no lines.
A grep match's `line` is a line of the row's record, or says
`line_basis` (`own_text`, `passage`) when it is not.

## Search capabilities and selection

The `search` schema publishes only what this server can run, probed at start:
`semantic` needs an embedder, `alias` registered aliases, `rank_by="passages"`
a subtype's passage rollup. Each missing one adds a "Not available on this
server" line to the instructions and appears in `inspect(target={"kind":
"surface"})` `capabilities`. Naming one is refused by name before any query:
`search_method_unavailable` (`semantic_search_unavailable` for `semantic`),
`search_rank_by_unavailable` for `passages`. One added later needs a restart.
A probe read error refuses the start (`agent_surface_probe_failed`); a probe
timeout publishes it unavailable, "not verified at start". `select` is an enum
of the available methods' fields; a field the chosen method lacks returns
`select_unknown_field` and the allowed set, never a silent drop.

## Result references

A lexical record search with `retain=true` also returns `result_reference`:
`{id, parent, order, expires_at}`, plus `next_offset` when the page did not
deliver every member and `passages_excluded` when it is not zero. The id
names the search's ranked members (node id, rank and hit location; never
text or attributes), up to its `limit` and at most 200, kept for 600 seconds
on this server process for this reader only. The text form prints it on its
own line:
`reference r1.… · order rank · expires 2026-10-05T12:00:00Z`.

A reference holds records only. A row the records fold kept as a passage
(its record is not readable to you, or it is an alias-promoted passage) is
delivered on the page but is not a member; `passages_excluded` counts those
rows, and the reference line adds `passages excluded <n>`. The count is of
rows on your page, so it says nothing about rows you cannot read.

- **Producers.** `retain=true` or `view="metadata"`, with `rank_by` `records`
  or `passages`, at `offset` 0. Every other form is refused before any SQL
  with `result_reference_unsupported` and a `reason`: `method` (semantic,
  alias, identifier mentions, attrs, grep), `rank_by` (`self`) or `argument`
  named in `argument` (`offset` above 0, a passage-unit `subtype_filter`, or
  `select` or `max_chars` with `view="metadata"`). A `limit` over 200
  refuses `result_reference_capacity`. A pin whose overlays were never
  materialized (no overlay marker) refuses `overlay_generation_unmaterialized`.
- **Metadata.** `view="metadata"` stores the same members `retain=true`
  stores for the same arguments, and returns no row: no node id, title,
  handle or excerpt, in text or JSON. It returns `result_reference`,
  `coverage.selection` with the exact member count as `size`, and
  `coverage.delivery` `{"complete": true, "view": "metadata", "rows": 0}`;
  `coverage.retrieval` keeps its own status (`bounded`, `exhaustive` or
  `unknown`). Search's own continuation is not returned. The text status
  line prints `metadata` and `<n> selected`, and always ends
  `bound reached (limit <n>) · next: none`.
- **Summary.** `summary=[field, ...]` names 1-4 fields (`subtype`, another
  stable field, or `attrs.<key>`) and adds `summary`
  `{by, groups, groups_total}`: count groups over the members, largest
  first, equal to `aggregate(input={"ref": id}, by=[...])` and refused where
  that aggregate refuses. At most 50 groups are returned; `groups_total` is
  exact. When more exist, `summary.bounds` and `coverage.delivery.bounds`
  name `summary_groups`, `summary.next` names the `aggregate` call (with
  `offset`) that reads the rest, and the text ends
  `bound reached (summary_groups 50) · next: aggregate(input={"ref": ...},
  by=[...], metrics=["count"], offset=50)`. When the
  byte budget cannot hold every group, the smallest are cut and
  `output_bytes` is named the same way (text: `bound reached (max_bytes)`).
  `summary` without `view="metadata"`, a summary naming an identity field
  (`node_id`, `handle` or `attrs.title`, whose groups would name the
  records), a field named twice, or a field named `count` refuses
  `result_reference_unsupported` (`argument` `summary`) before any SQL.
- **Paging.** Byte fitting shrinks the delivered rows, never the reference. A
  retained search pages its reference, not search: `next_offset` is null,
  `next_offset_reason` is `page_reference`, `result_reference.next_offset` is
  the number of members this page delivered, and the text ending reads
  `next: map(input={"ref": "<id>"}, offset=<n>)`. Search's own continuation
  is kept under `search_next` (`{offset, reason, remedy}`, present when any
  is set): when every member was delivered but search has more hits, the
  text ending reads `next: search offset=<n> without retain`, and the
  reference line adds `search next offset <n>` (and the reason and remedy,
  when set). `coverage.selection.size` is the member count, not a recall
  claim.
- **Consumers.** `filter`, `map` and `aggregate` take `input={"ref": id}`. They
  read only the stored members at the reference's pin through the
  effective-attribute path, in rank order, with an exact `total_count`;
  `order_by` is refused, except `aggregate`'s, which ranks its groups.
  `filter(input={"ref": id}, retain=true)` returns a child reference holding
  the matching members in the parent's order; the parent never changes.
  `not` here is two-valued (a member without the
  attribute matches `not eq`), unlike search's three-valued `where`.
- **Refusals.** Before any member is read, `result_reference_invalid` names a
  `reason`: `malformed`, `store_lost`, `signature_invalid`, `expired`,
  `evicted`, `unknown`, `generation_conflict`, `generation_unavailable`,
  `binding_mismatch` (with `field`: `authority`, `catalog`, `branch`,
  `database`, `deployment`, or `attributes` when an attribute observation
  changed a member), or `member_missing` (with a `count` only). Another
  reader's id answers exactly like an unknown id. Nothing is rebuilt or
  dropped silently. A deployment with several server processes needs sticky
  sessions; an id used on another process refuses `store_lost`.

### Live handles

Every live set read returns a handle: `filter`, `map`, `aggregate` and
`expand(live=...)` (`retain` asks for nothing more). The next call passes it
as `input="l1.…"` (or `{"ref": "l1.…"}`; a JSON string of either is read
the same) and makes no upstream call. JSON carries `handle` `{id, rows,
columns, columns_more, order, pins, expires_at}`, and `from` on an
aggregate, whose handle holds its groups (`by` fields, metric labels and
`rank`, which a later `filter` takes in `where`, `select` and `order_by`).
The text prints `handle l1.… · 24 rows · 10 min` (or `handle l1.G (groups)
· from l1.S · 11 groups · 10 min`) and suggests no call; a cut page's one
`next:` (JSON `next_call`) is the whole call on that handle. A set over 50,000 rows or 8 MiB
has `handle: null` and `handle_reason`, and a cut reply over it refuses.

- **Life and scope.** 10 minutes from mint; a read does not extend it. A
  filter that narrows or orders a handle, or a read that fetches a missing
  field, makes a new handle with its own 10 minutes; its parent stays. A
  handle is readable only by the reader and logical session that made it
  (the reader alone on stateless HTTP); another's answers `unknown`.
- **Caps.** 64 handles and 32 MiB per reader and session, 128 per principal,
  1,024 and 128 MiB per server. A full server evicts the oldest handle of the
  session holding the most; the evicted id answers `evicted` to its owner.
  On stateless HTTP a reader's concurrent sessions share one 64.
- **Decoding.** Rows read from handles (growth included) count at most
  16 MiB of compact JSON per call and 32 MiB in flight per process (about
  4 to 6.6 times that as decoded objects); past either,
  `live_decode_capacity` (`scope` `call` or `server`), after the caller
  checks, before any row.
- **Conditions.** A `where` that is only a handle is read as `input`; a
  handle inside `and`/`or`/`not` refuses `invalid_operator_arguments`
  (`reason` `handle_as_condition`). `{"op": "in", "values": {"ref": "l1.…",
  "field": "<column>"}}` compares with that handle's values when `input` is
  a live handle; elsewhere it refuses by name.
- **Dead handles.** `result_reference_invalid` with its `reason`, no rows
  and no upstream call. `expired` and `evicted` name the call that rebuilds
  the set (`retry:`, `authority` removed), or for a chain the call it began
  with; another reader's id gets none. One server process: a deployment
  with several needs sticky sessions (`store_lost`).

Rows read from a handle are `{"attrs": row}` under the filter grammar;
`offset` stops at 10,000 (`paging_limit`); reads carry `served_from:
"retained_answer"` and `original_call`. A cut `expand(live=...)` also
carries `next`, which `expand(next=...)` pages. Other arguments refuse
`live_result_predicate_unsupported`, a denied gate `live_result_denied`, and
another `generation_id` `generation_conflict`.

### Chain handles

From the third kept answer of direct `expand(live=...)` calls to one relation
on one record that differ in one argument, the reply's
`external_live.chain_hint` `{chain, answers, argument, failed, missing,
dropped, calls}` names one handle over the newest 60 kept answers and two
`aggregate` calls on it, a total and a ranking (text: `! these 30 answers are
kept as one handle; ...`). Only a declared string argument with no enum forms
a chain, and no value with `*`, `?` or `%`; a string used as a filter still
forms one (relations declare no id flag). Reading it combines the answers into
the set one `live.each` call would keep, re-checks every answer as a single
handle read does, and makes no upstream call; nothing combined is stored. The
reply adds `combined` `{chain, answers, of, rows, rows_in_several_answers,
varying, failed, read, upstream_calls_cut, not_kept}` (text `input: 30 kept
answers of <relation>, one per <arg>`); `upstream_calls_cut` sums the outside
system's cut over the answers, in calls. Identical rows found in more than
one answer are counted (`! 3 identical rows appear in more than one answer`),
never dropped. Every answer the read does not cover is named in `not_kept`
with its retry call and counted in `of` (text `! over 29 of 30 answers; 1 no
longer kept: q=...`): `reason` `expired`, `evicted` or `unknown`, or
`gone_before_chain`, or `not_in_chain` for an answer older than the newest
60. While the chain lives its answers are kept ahead of the session's other
handles, but never at the cost of a refusal. A newer chain replaces the
older, whose id answers `replaced` with `replaced_by` to its owner. Other
refusals: `live_handles_not_combinable` (`reason`, `index`, `handles`); a
refusal of one answer keeps its own code and adds `index` (its 1-based
position) and `chain` to its `data`. The chain line prints only where it
still fits after the `run` line, so a full or cut reply carries neither.

### Ranked groups

`aggregate(..., order_by="<name> desc")` ranks every group, then `limit` and
`offset` cut the page. `<name>` is one of the call's metric labels as the
reply prints them (`count`, `max(attrs.size)`), `occurrences` on `match`
groups, or one of its `by` fields as written. It is written `<name>`,
`<name> asc`, `<name> desc` or `-<name>`; no direction is smallest first.
A name the call does not compute, a metric missing from `metrics`, two
directions, or the record-anchor and edge-summary forms refuse
`invalid_operator_arguments` (`data.reason` `order_by_unknown`,
`order_by_not_in_metrics`, `order_by_spelling`, `order_by_needs_set`) before
anything is read; on a live set that means no upstream call.

The rule is the same on stored and live sets. Numbers compare as numbers; then
text, booleans, and other values by their JSON text, in that order both ways.
A group with no value (a null metric, a JSON null key, or a missing key,
printed `(absent)`) ranks last in both directions, null first. Ties go by the
group key. A ranked reply adds `order_by` (`name`, `direction`, `kind`),
`rank` on each group (its position across pages), `ranking_complete`,
`ranked_last_no_value` and `tie_at_page_end`; a live read with nodes not
read, rows not comparable or a cut listing says `ranking_complete: false`
(and `ranking_cut` for the cut), on every page of its kept set
(`ranking_left_out` there).
The text adds `ordered by <name>, largest first`. A cut stored page ends
`next: offset=<n> generation_id=<g>`; a cut live page ends with the whole
`aggregate` call on its kept `l1.` set, with its condition and page size.
Without `order_by`, a live page or `run` step cut by `limit` while it
computes another metric carries `order_note`.

### Computed columns

`map(input=<live set, live handle, groups handle or chain handle>,
compute={"fill": "attrs.used / attrs.total"})` adds `calc.fill` to every row:
the same rows in the same order, kept as a new handle (`from` its input).
`where` narrows the input first (text `input: 24 of 263 rows of l1.…`);
`select` picks the printed columns (by default `node_id`, the fields the
expressions read and the new columns). Later reads of the handle name the
column `calc.fill` in `where`, `select`, `order_by`, `by` and
`sum`/`min`/`max(calc.fill)`; a bare `fill` that equals a computed column
reads it, with an `adjusted` note, and is never fetched upstream.

- **The language.** Arithmetic, comparisons, `and`/`or`/`not`, and
  `abs round floor ceil min max coalesce is_null if lower upper len concat
  substr split_part starts_with ends_with contains regex number string
  to_unit days_between`. A field is `attrs.<key>` (or `attrs['odd-key']`),
  `node_id`, `subtype`, `calc.<name>` or `date.attrs.<key>`; over a groups
  handle a metric is its label (`round(to_unit(sum(attrs.file_size), 'B',
  'PB'), 3)`) and any column `col['…']`. A bare name is `attrs.<name>`, with
  a note. Nothing runs code.
- **Numbers.** Integers are exact; a whole value prints as an exact JSON
  integer at any size. Other values are kept as 15-digit floats, so a value
  copied from a reply matches in a later `filter`. A sum of a computed
  column is today's live sum: whole values exactly, others in
  `sum(calc.x) floats`, named in `sum_not_exact`.
- **States.** Each column counts its rows in `computed` (text `calc.fill: 22
  ok · 2 not read (site:A, site:B: attrs.static_used unavailable)`): `ok`,
  `not read` (an input's live call failed or was denied; never turned into a
  value, not by `coalesce` either) or `error:<kind>` (`division_by_zero`,
  `not_a_number`, ...). Such a row stays in the map's handle with a null. A
  later `filter`, `by` or metric on that column sets it aside and counts it
  (`not_computed`; it matches no condition, not even under `not`); a total
  then says `over 22 of 24 rows; 2 not computed`, a count is at least the
  value shown and a sum, min or max is over the rows received only.
- **Live fields.** A field the expressions read that the set lacks is
  fetched for its rows only (one call per row and relation, part 1's
  1,500-call cap planned before any call) into the new handle; a second map
  over it makes no call. Text built from an upstream value is served as
  untrusted text; text built from numbers is plain.
- **Refused by name, before any upstream call or graph read**
  (`invalid_operator_arguments`, `data.reason`): every expression refusal;
  `calc.` in `where` and `order_by` on `map` (send `filter(input="<the map's
  handle>", ...)`); `projection` beside `compute`; a stored set or `r1.`
  handle (`compute_on_stored_set`, until stored handles), an edge set;
  `node_id` on raw rows; `date.<meaning>` on live rows; a column named like
  an upstream field the set holds or declares (`column_shadows_field`) or like
  a record or groups column (`reserved_column_name`); an `attrs.` field no
  row holds and growth cannot fetch (`key_absent_on_every_row`, with the
  closest keys); and every `map(compute)` while access rules are on
  (`compute_under_access_rules`). A bare name equal to the column being
  defined reads the input's column of that name (`{"fill": "fill * 100"}`).
- **Bounds.** At most 50,000 input rows (refused with the exact count), 8
  columns a call, 32 a handle, 5 s and 8 MiB of computed values
  (`map_output_too_large`); an uncut reply too large to keep has `handle:
  null, handle_reason: too_large`, a cut one refuses `kept_set_too_large`.
- **A refusal after growth.** When growth fetched fields and the call is
  then refused for any reason (a bound, a field absent from every row, a
  full store at commit, a server fault), the grown set is kept and named:
  `error.kept_handle` gives its `id`, `rows`, `fields` (read on some row;
  `fields_on_no_row` apart), `upstream_calls`, `expires_at` and a
  `template`: the refused call on the kept handle, refused again if sent
  unchanged. The retry line starts `map over the kept handle l1.… (no
  upstream call)`; changed as it says, the call makes no upstream call.
  The kept set is one of the caller's handles: at the caller's cap it
  evicts the caller's oldest unpinned handle, never another session's or
  reader's. A store that cannot keep it says so (`id: null`, `reason`).

`map` without `compute` serves what it always served, with no note.

## History reads

Two `inspect` kinds read across generations instead of at one pin. Both are
decided by the session pin's Nomos read filter, the one `inspect(kind node)`
uses, applied to each version's own row.

`inspect(target={"kind": "node_history", "node_id": <id>, "attr_keys"?:
[...], "from"?: <generation>, "to"?: <generation>, "limit"?: n, "cursor"?:
<token>})` lists one record's versions, newest first. `to` defaults to the
session pin and `from` to the oldest generation in the branch's history;
both take any form `generation_id` takes. A fork's history is its parent's
versions up to the fork, then its own. Each version names its `first` and
`last` generation (`generation_id`, `branch`, `created_at`); the newest
version's `last` is `to` while the record is visible there. `change` is
`oldest_shown`, `changed` (with `added_keys`, `removed_keys` and
`changed_keys` against the next older version shown) or `subtype_changed`
(the old subtype retracted and the new one added). `values` holds the
`attr_keys` values as they stood at `last`, fenced by producer trust like a
node read, with `values_provenance`.

Every version, with or without `attr_keys`, carries `provenance`: the
producer (`source`) and `trust` of the version as it stood at `last`, as a
node read labels a node. An attribute name is the producer's text, so when
the version, or the older version a change was computed against, comes from
an untrusted producer, every name in `added_keys`, `removed_keys` and
`changed_keys` is fenced like an untrusted value.

- `attrs_layer` is always `graph`: the attributes as the source projected
  them, before the reviewer and proposal overlay `inspect(kind node)`
  merges in. Overlay history is `inspect(kind attr)`. This is also a
  privacy difference: a reviewer overlay that replaces or masks a value in
  `inspect(kind node)` does not apply to the values served here. Which
  versions you may see is still decided after the pin's reviewer overlays
  are applied, so an overlay that reclassifies a record hides every version
  of it.
- Adjacent versions equal under the stored-fact rule merge into one: the
  subtype's volatile fields (from the pin's catalog) and a lineage revision
  that moved with no content change do not count as changes.
- Under Nomos `audit` or `enforce`, a hidden version is left out and a
  masked attribute is compared masked, so a change to a value you cannot
  see is neither a changed key nor a version boundary. A generation where
  the record is hidden reads like one where it is absent. Each version is
  decided on its own row by the policy active now. So a record hidden now
  still shows its older versions when the change that hid it came from the
  source (for example the source reclassified it in a later generation):
  those versions, and their `attr_keys` values, are served as
  `inspect(kind node, generation_id=<that generation>)` serves them. A
  policy rule that hides the record's source hides every version. A record
  hidden in every generation of the range, or absent from all of them, is
  refused `node_not_found`, as `inspect(kind node)` refuses it.

`inspect(target={"kind": "subtype_diff", "subtype": <type>,
"from_generation": <generation>, "to_generation"?: <generation>,
"attr_keys"?: [...], "limit"?: n, "cursor"?: <token>})` lists the records of
one type `added`, `retracted` or `changed` between two generations, by
`node_id`, with `added_count`, `retracted_count` and `changed_count`.
`to_generation` defaults to the pin. A record that moved into the type is
added, one that moved out retracted, and a visibility flip is added (hidden
to visible) or retracted (visible to hidden). A changed entry names its
keys as `node_history` does; `from_values` and `to_values` hold the
`attr_keys` values. Every entry carries `provenance` for its newer row (its
older row when retracted), and its key names are fenced when either row is
from an untrusted producer. Under `audit` or `enforce` the counts and entries are
exact while the type's delta is at most 5,000 rows; above that they are
null, with `truncated: true` and a `note`, so the number of rows you cannot
see does not leak.

Both kinds page at `limit` (default 50, at most 500) and at the call's
`max_bytes`. A cut page carries `cursor` and `next_target`, the same target
with the cursor added. The cursor is bound to the resolved generation range
and to the other arguments: a page read after a new publish reads the same
range, and a cursor sent with other arguments is refused
`continuation_token_invalid`. Refusals:

| Code | When |
| --- | --- |
| `generation_not_found`, `generation_compacted` | an end is not a published generation, or history compaction retired it; nothing falls back to the latest |
| `generation_out_of_scope` | an end is not in the session branch's history |
| `history_range_invalid` | `from` is newer than `to` |
| `history_generation_unordered` | a published generation in the range has no ordinal in the generation-ordinal registry; its order is never guessed from timestamps |
| `history_row_budget_exceeded` | the record has more than 5,000 stored versions in the range read (a narrower `from` and `to` reads fewer), or a type more than 200,000 records at one end (20,000 changed records with Nomos off); the reply names the budget and is never a shortened list |
| `nomos_principal_unidentified` | a multi-user server's request carries no subject; refused before any read, under every enforcement mode |

What these reads can still reveal, by design:

- **Budget refusals count hidden rows.** The `subtype_diff` 200,000-row
  refusal is checked before any row is filtered, so it tells a reader the
  type holds more than 200,000 records at one end, hidden ones included: one
  threshold bit, as `generation_delta`'s withheld counts give. The
  `node_history` refusal on a record you can see at the pin tells you it
  has more than 5,000 stored versions in the range, hidden versions
  included. A record hidden at the pin is refused `node_not_found` instead.
- **Audit rows.** Each version is decided by the read filter, so under
  `audit` or `enforce` the Nomos decision rows look like current reads of
  the record and carry the pin's generation, not the version's. The call
  log records the arguments (`node_id`, `from`, `to`).
- **No switch.** Both kinds are `inspect` kinds (`READ_INSPECT_KINDS`): they
  are served wherever `inspect` is served, and no deployment setting turns
  them off on their own.
