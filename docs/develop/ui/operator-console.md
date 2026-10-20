---
audience: developer
type: reference
canonical_id: operator-console-contract
section: development
visibility: primary
status: active
owner: operator-ui
---
# AKMON operator console contract

The console is the whole [operator UI](operator-contract.md): every URL opens
it, and a hash that is not `#console/<view>` -- the bare URL, `#home` and
`#health` from the pages it replaced, anything else -- opens its Home. It serves
five views -- Home, Updates, Sources, Explore and Graph -- and its code is its
own lazily loaded chunk. A `#console/<name>` naming none of the five quotes the
name back.

## Read-only boundary

The console issues the `GET` reads below and no other verb, so the read-only
boundary holds for it. Missing, stale,
unreadable, capped or failed data is shown explicitly, never as an empty or
healthy default. It links only to views it serves, and a test renders the
production router for every link a page emits.

A failed read says so in its content's place: the read's own headline, the route
once, status, reason code, the server's sentence ("The data server is not answering"
for a bodiless 502 or 504; a 20 s timeout, a non-data body, an HTML page and a 422's
reasons each worded), the next check, and "Read again". A partial failure keeps the
page: minor reads settle alone, and the one lost is named in its place. Any failed
read, page or bar, re-reads at 5, 15 and 45 s, when another read recovers, and on a
click of the view's own link; a read out past 3 s counts its seconds.

The bar's Open chat link is the deployment's declared `chat.deep_links.chat_site_base`,
never a guessed host. Declaring none, no entry stamped current, and a read still
outstanding are three absences the bar tells apart, from the shell's read when it answered.

The version picker changes what is read only where a read takes a version.
Explore's and Graph's reads do; every other read answers for the generation the read
API pinned at start-up, so on Home, Updates and Sources the picker shows "latest"
and a one-clause note under the title names the version shown. Each option is the
short id and its UTC publish minute. A `?version=` not shaped like a generation id
is printed nowhere, rides on no generated link, and is named without its value on
Explore's and Graph's ignored-parameter lines; `?sort=` is named on Updates.

## Backend reads

| View | Required reads |
|---|---|
| Console | `GET /viz/api/deployments`, `GET /viz/api/console/health`, `GET /viz/api/console/generations`, `GET /viz/api/console/attempts`, `GET /viz/api/console/attempts/detail`, `GET /viz/api/console/versions`, `GET /viz/api/console/producers`, `GET /viz/api/console/source-diagnostics` (Updates' empty-deployment sentence only), `GET /viz/api/console/ontology-rollup`; the Graph view's `GET /viz/api/console/graph/types`, `GET /viz/api/console/graph/source-links`, `GET /viz/api/console/graph/record`, `GET /viz/api/console/graph/ontology`, pinned, authorized and refused as the record reads (optional `version`), each body `{generation_id, payload}`. `graph/types` reads the two maintained rollups only; `graph/source-links` is joined on request under an 18 s timeout (`503 source_links_unavailable`) and cached per generation; `graph/record` lists each (direction, relationship, type) group's first 25 neighbours by id with their producer, reads each relationship up to 2,000 edges (500 under an enforced policy) and marks counts past that as floors, and under a policy counts only what the reader may see (`count_basis: visible_to_reader`); `graph/ontology` takes declared attributes from the deployment's schema files at read time (`attributes_basis`) and samples field fill from the first 1,000 records per type |
| Explore | `GET /viz/api/console/versions`, `GET /viz/api/console/records/types`, `GET /viz/api/console/records/search`, `GET /viz/api/console/records/list`, `GET /viz/api/console/records/detail`, `GET /viz/api/console/records/artifact`, `GET /viz/api/console/records/raw`, `GET /viz/api/console/producers` (a record's source link resolves an ownership id to a producer name) |

Every response identifies its deployment and generation or states why that
identity is unavailable, and a capped one states what was omitted and why.

The console composes its own readiness. Sources takes three things from it --
each source's sync convergence, its freshness, the published generation -- and
nothing from the other two dozen checks `okg dogfood status` runs.
`compose_console_readiness` runs the four that reach the page -- the MCP probe
among them, because a fresh source's convergence is composed from it -- and
names each one it declined in `checks_not_run`, printed under the table so a
reader knows it is not a release gate. The CLI and the deployment gates keep the
full composition.

## Home

Home counts producers from `console/producers`, as Sources does; the
`*-diagnostics` routes list recorded keys, `_enricher:` outputs included. Its
chart draws attempts by outcome per UTC day: an n-day span holds the read's day
and the n before it, and an age past 48 hours counts whole UTC days, so a row
reading "n d ago" is inside the span. A `null` `duration_ms` renders as nothing,
and freshness prints the read's words (`not evaluated`), never a blank.

## Sources

Sources is one table of every producer -- sources, enrichers and views -- each
appearing exactly once. A producer is identified by the `ownership_id` its
`source_registry.yaml` entry declares, never by the document key or a
normalisation of it: a deployment `my-graph` may declare `bib-references` as
`my-graph.bib_references`; a read joining on a name it builds itself lists
each twice, once configured and empty, once as an unconfigured runtime key.
Hence `GET /viz/api/console/producers`, and no name folding in the view.

Sources states only what the reads record. No next-run column: no read records
when a producer is next due. No error text on a row or a producer page:
`okg.v_source_run_diagnostics` has no message column, and a failed run is named
by its outcome and the phase it stopped in. No "Open in Updates" link:
`console/generations` sets `stopped_producer` to null on every attempt it returns
(no checkpoint column records it). No registry entry: serving it
means serving credentials-adjacent configuration. Records by type always appear,
one type included; relationships by type appear nowhere, no rollup dimension
pairing one with the producer that emitted it. A run's phase is words where
known, its code in the cell's title.

Every state the table prints is a word, never the code a read served; the code
stays, once, in a muted line on the producer page and in "Copy as text". Three
reads fill that column, each with its own vocabulary: a source carries the
source-sync convergence state (`fresh`, `converging`, `stuck`, `blocked`,
`manual_required`, `unsupported`), an enricher its health status (`healthy`,
`failing`, `withheld`, `wired-but-silent`), a view its run status. Each gets a
word and a glyph: a check for a good state, a warning for one to act on, a stop
for one held off, a muted circle for an absence. A state no read recorded is
"State not recorded" in muted neutral, never amber. `blocked` with an overdue
reason reads "Overdue"; `manual_required` reads "Needs a manual run". Freshness is
worded to agree with the state beside it: a source no freshness policy names reads
`no cadence`, the same fact as its `fresh` state, not `not configured`.

Under the table one clause says what Home's verdict counts a source by (an update
recorded in this version); it is dropped once `health.source_breakdown.basis`
names the table's own basis. A producer in a state to act on -- `blocked`,
`manual_required`, `overdue`, `stuck`, `unsupported` -- prints the code the
convergence read recorded beside that state, in words, with its incident count
where that is nonzero; a fresh one prints none. An enricher is listed once: the
rollup counts its output in the `source` dimension under `_enricher:<name>`, its
own key, so the read folds that row in and says what it measured; its run count
comes from a window of published versions stated beside it. A producer with no
recorded run prints "never run" and leaves its contribution cells empty; a view's
relationships cell reads `a view emits no relationships`; a run count from a
capped query is withheld as a floor with the history named truncated. No read
reconciles a run's items out against the records the version holds.

## Updates

Updates states only what an attempt records. No column holds a publish's wall
clock or the stage it stopped in, so there is no duration column, no pace phrase
and no stage list. What it shows is recorded per attempt: the checkpoint's
blocked reason, the sources the publication gate excluded, and the view
producers' runs, joined by the target generation the publisher wrote. Per-source
results are not shown and the page says why: a source run is recorded against
its own run id, not a generation. `GET /viz/api/console/generations` pages by a
cursor naming a checkpoint; one the history does not hold is refused by name,
not answered with the first page.

One attempt's page states what that attempt recorded, from `GET
/viz/api/console/attempts/detail?id=<full generation id>`, issued only on the
detail page, after an id in the hash resolves to a full generation id. It shows
what the version holds and what the publish moved -- added, updated and
retracted, which the list's totals-subtraction delta cannot see -- then the
counts by record, relationship and source type at this version against its
parent, the publication checks that ran, the producer runs recorded against it,
where it sits in the history, and what was configured when it ran.

A section whose data the deployment did not record is CUT, and an absent section
says nothing; one the read REFUSED prints the read's own reason code inline. A row
whose parent could not be read says so instead of a delta; a first version
(`no_predecessor_version`) prints "+N (first version)". A checkpoint that never
published still has a maintained count rollup, and the page says the counts
describe a version that was not published -- driven by the read's own
`counts_describe_an_unpublished_version` marker, not by the page. A deployment
with no consistency check records no `consistency_gates` key, which the page
states in one sentence, never as "0 checks ran, all passed". A gate is named in
words with its code once, muted; its withheld keys are named with why, not their
size. A rewrite that moved no count says so. A `404 attempt_not_found` renders as
a plain named line.

The ontology-rollup read feeds one panel and is issued on its own, so a failure
takes that panel alone and no count is substituted. An unavailable rollup (`503
ontology_rollup_unavailable`) and one a policy refused (`403 policy_denied`) are
shown as different things.

## Explore

Explore states only what the record reads carry. It has no sort control and no
date filter -- the graph stores no per-record date and the server refuses both --
and a URL carrying `sort=` or `since=` gets one line naming each. Each search box
is a `<form role="search">`: Enter submits the draft to the results, and opens a
result row instead only once the reader has arrowed to one. No row carries a date,
related count or citation link; a capped total prints as a floor; a search that
matched nothing says so in words, with the type chips as the way out; a hidden
field says "hidden by policy" and names none; Provenance has no fact log,
last-write attempt or enricher tally; and every cap, masked key and refusal is
stated with the server's `detail` verbatim. Explore mounts no PDF, HTML, Markdown
or code viewer (these reads serve images alone), and its controls are the
browser's own elements.

A result is titled by what the SOURCE calls it -- `source_record_id.path`, or a
recorded title -- with a chunk's position under it; a document's chunks anywhere
on the page are one entry, in first-seen order, and the arrow keys walk the rows
as drawn (`aria-activedescendant` names the row); the type rail comes from
`type_counts` where served and is never hidden; a record with no path keeps the
served label; and the record id is on the record page and in the copy text, not on
a row. A record opens on Content, and Fields sits after Related. A chunk names its
document and its position by either edge shape -- `contains` outward or
`member_of` inward, both served, with `chunk_edge` naming the one used. Related
lists one row per RECORD: a neighbour recorded both ways is one row naming both
relationships, and the count is the distinct records. A colliding title is told
apart by the record's hash tail or a digest of its id, never by the title itself.
"Open source" comes from `citation_url` alone. Provenance renders
`source_record_id` and `source_revision` as labelled rows, never as JSON: a hash
is shortened with the whole to copy, and a value the read shortened says so on its
row, the rule in its `title`. A read naming a table this deployment lacks refuses
by name, not with a 500. Raw prints "First 100 shown; this record has more" for
`edge_total: null`.

Content renders the chunks the read listed as prose at the console's reading
measure, in the order served; only a record that IS code or structured data is set
in the monospace face, and the text is preformatted either way, so every break the
source recorded survives. The order is named from the read's `chunk_order_basis`
-- the stored ordinal or the character offset -- and Related lists a
document's parts in order; an unknown basis fails the read by name. Chunks no
basis could place are counted and named as unlisted; where the offsets overlap the
page says by how many. A record whose body is its chunks HAS a body: the missing
original is stated in one clause, never twice. Content and excerpts are the stored
text as stored -- masking covers identity and provenance fields, not record
content (an ingest defect) -- and "Copy record as text" copies every tab.

## Verification

Changes to this contract carry the proof the [Operator UI
contract](operator-contract.md) requires of the shell: a test proving the app
issues only `GET` requests, unit coverage for every view including loud-failure
and capped-data states, a browser proof on a real published deployment at
desktop and phone widths with a console-error check and the operator's sign-off,
and the rule that a route may not import another.
