---
audience: developer
type: reference
canonical_id: operator-ui-redesign-requirements-v2
section: development
visibility: primary
status: active
owner: operator-ui
---
# Operator console requirements, v2

Status: for operator review, 2026-09-18. Replaces the 2026-09-16 version.
That version, the synthesis it grew from and the twelve-product teardown with
its screenshots are kept outside the repository, in the maintainer's
`ui-research-2026-09-18` research brief.

## 1. Mission (operator, on record)

"It is an operator console. Users should be able to see the status, contents,
and health of the graph without resorting to CLI commands."

Someone opens it when:

1. something broke and they were told;
2. a routine check;
3. they are looking for a specific record;
4. they just set something up and want to see it took.

Each view below names which of the four it serves. A view that serves none is
cut.

## 1a. User stories

Three people. The **operator** runs the deployment. A **source owner** added
or configured a source and does not run the deployment. A **reader** got a
record link from chat and does not know what a version is. Stories are
grouped by the four arrival situations. Each ends where the view can end it
and names the fixture that tests it.

### Something broke and they were told

| # | As | I want | Done when | Fixture |
|---|---|---|---|---|
| S1 | the operator, from an alert | to know whether the graph people are reading is stale | Home's identity line names the version being read and its age, and says in one sentence when the latest attempt did not publish and why | archiv3-small |
| S2 | the operator | to know which producer stopped the update and what it said | Home's Needs attention names the producer; Updates shows the attempt with the stage it stopped in, the one-sentence cause, and the recorded error verbatim; Sources shows that producer's row failed with the same error in its rail | archiv3-small, broken-runtime |
| S3 | the operator | to know whether the update failed on data or on the system | Updates distinguishes blocked on a source, blocked on a publication check, and a running attempt; Home's Needs attention lists runtime checks (worker, queue, dead-letter) separately from attempts | broken-runtime |
| S4 | the operator, on a phone | to answer S1 and S2 above the fold at 390 wide | The identity line and the first Needs attention row are visible without scrolling at 390 | all |

### A routine check

| # | As | I want | Done when | Fixture |
|---|---|---|---|---|
| S5 | the operator | to see in one glance that nothing is wrong | Home's count strip reads all-healthy and Needs attention is one sentence with the fact that makes it so; no red on the page | workspace-mixed (healthy variant) |
| S6 | the operator | to see the rhythm of updates and spot a change in it | Home's chart shows attempts per day by outcome over a chosen range with a now marker; a run of blocked days is visible without reading rows | scale-200 |
| S7 | the operator | to check that a source that should have changed did change | The Sources row shows records and relationships contributed in the selected version and the last run age; zero change is a visible number | workspace-mixed |
| S8 | the operator of a large deployment | the same Home at 220 sources as at 5 | Home renders the same elements at scale-200 and archiv3-small; only numbers differ; nothing scrolls that did not scroll before | scale-200 |
| S9 | the operator | to know when the next update is due | Sources shows "next due" from the registry cadence; the column is absent, not blank, when no cadence is declared | workspace-mixed, archiv3-small |

### Looking for a specific record

| # | As | I want | Done when | Fixture |
|---|---|---|---|---|
| S10 | a reader who clicked a link in chat | to see the record the answer used, at the version the answer read | The link opens Explore with that record open, its version named in the header, in one hop; the reader never passes through Home | archiv3-small |
| S11 | a reader | to know whether the answer is out of date | The record header shows the version read from and its publish time; when a newer version exists, one sentence says so | workspace-mixed |
| S12 | the operator | to find a record by id or by text | Typing an id matches it first; text matches show the match highlighted; arrow keys and Enter open it; the result count is stated | workspace-mixed |
| S13 | the operator | to walk from a record to what it is connected to and back | Related lists relationships by type with counts; a row opens that record in the same rail; back returns; the cap and omitted count are stated | scale-200 (a record with 500 relationships) |
| S14 | the operator | to see the stored data behind a record, unedited | Raw shows the stored row, the fact-log entries and the retained payload, or one line naming which condition blocks it | workspace-mixed |
| S15 | the operator | to tell a hidden field from a lost one | Fields shows "hidden" with the policy name; Raw shows the same key masked, not absent | workspace-mixed |
| S16 | the operator | to read a record as it was before the last update | The version picker switches every view to the older version and the record re-reads there | workspace-mixed |

### They just set something up

| # | As | I want | Done when | Fixture |
|---|---|---|---|---|
| S17 | a source owner who added a source | to confirm it ran and what it added | The Sources row shows updated, the last run age, and records and relationships contributed; the rail shows the run history | workspace-mixed |
| S18 | a source owner whose source has not run yet | to know that nothing is wrong, it just has not run | The row says never run, with the next due time when known; it is not shown as failed | workspace-mixed |
| S19 | the operator who added an enricher or a view | to confirm it ran in the last attempt and what it produced | The Sources row for the enricher or view shows its state and contribution; the attempt detail in Updates shows its stage with the count or the withheld reason | workspace-mixed |
| S20 | the operator of a new deployment | to know what to do next | Home says no version has been published for this deployment and lists configured sources as never run; Explore says there is nothing to search yet | empty |
| S21 | a source owner | to know why my source is excluded from the published graph | The Sources row says excluded with the recorded reason in the rail | workspace-mixed |

Stories the console does not serve, by decision: running, retrying or
editing anything (read-only); browsing the graph as a picture (Graph view
deferred); uploading data (chat side).

## 2. Decisions already taken

| Decision | Source |
|---|---|
| Read-only. No run, retry, publish or edit control. | operator, 09-16 and 09-18 |
| Four views: Home, Updates, Sources, Explore. Graph view deferred. | operator, 09-18 (option B2) |
| Sources lists every producer of facts: sources, enrichers, views. | operator, 09-18 |
| Identity: repo `tokens.css`, four firings, meander band, AKMON mark. Theme picker, deployment picker and chat button stay in the top bar. | operator, 09-16 |
| Charts allowed where they carry a fact. | operator, 09-16 ("I like the plots") |
| Ant Design components themed to the tokens. React, Vite, hash routing. | issue #1924 |
| Served at `/console` behind the chat front door; chat record links open Explore. | issue #1924 |
| Verified against the live Archiv3 copy, with one successful and one failed update. | issue #1924 |
| Nothing on a summary page grows with the number of sources or record types. | operator, 09-16, twice |
| Every sentence carries a fact the reader could not have supplied. | operator, 09-16 |
| #1931 (Search) is held; it becomes the base of Explore. | operator, 09-18 |
| Top bar rather than sidebar. | recommended; operator said "good with a topbar" 09-16, not re-confirmed |

## 3. Rules for every view, from the research

Each rule held in every product studied that has the feature
(`redesign-synthesis.md` section 2 cites the screenshots).

1. **Counts before lists.** A fixed-size count line states the summary. Each
   count is also a filter on the table below it.
2. **Status is a word plus a glyph.** Never colour alone. One colour for
   failed, one for running or blocked, none for healthy.
3. **Failure text is one level down, verbatim.** A list shows a pill and a
   count. The detail shows a one-sentence cause, then the recorded error text
   in monospace, unedited. No page hands the reader a command as the only way
   to learn why.
4. **Runs are one flat table, newest first.** One row per run: what ran,
   when, how long, what triggered it, status, what it produced.
5. **Time on every row.** Relative age plus duration on the row; absolute UTC
   time in the detail.
6. **Time range and next event on every summary.** A range control on any
   chart, and the next expected run stated as a fact when the registry
   declares a cadence.
7. **Scale by filter, group and "View all (N)".** Never a longer page, never
   more cards. Tables page at 50 rows and filter by name and state.
8. **Detail is a fixed header plus tabs.** Tab labels carry counts. No page is
   one long scroll.
9. **One search box, everywhere.** In the top bar, with a keyboard shortcut,
   type-ahead, and facet counts that move with the query.
10. **Version position is printed.** The version read from is named on every
    page in the top bar and in every detail header, with its publish time.
11. **Type: 14 px base, two sizes, two greys.** Weight carries emphasis.
    Identifiers in monospace, prose not. Nothing under 12 px.
12. **Hairlines, not card grids.** Rows 36 to 56 px. A box only around a
    standalone fact. No greeting, no tiles, no decorative panels.
13. **Loud failure.** A failed or hidden read says so in place. Policy-hidden
    values say "hidden". Nothing renders as a healthy blank.
14. **Two widths.** 1440 and 390, no horizontal overflow. At 390 the top bar
    stacks to two lines and rails move under their table.
15. **Everything copies.** (operator, 2026-09-18: "easy to copy information
    throughout the UI so we can pass it to our coding agents.") Identifiers
    (version, attempt, producer, record) copy on click with a copy icon and a
    "copied" confirmation. Recorded errors, configuration YAML and settings
    copy as whole blocks. Every page carries a "Copy as text" control in its
    header that copies the page's facts as plain Markdown. A producer's page
    shows its configuration as raw YAML with credentials redacted in place.

## 4. Objects

- **Deployment**: one per front door, chosen in the top bar picker.
- **Version**: a published generation. Reads pin one; the picker lists
  published versions newest first, default latest.
- **Update attempt**: one run of every source, then extraction and enrichers,
  then publication checks, then views, then publish. Outcome: published or
  blocked, with the stage and the producer it stopped on.
- **Producer**: a configured source, enricher or view. Each has runs with
  counts, a phase reached, and an error when it failed, and contributes
  records and relationships to a version.
- **Runtime**: the worker's queue and dead-letter jobs. About now, not about
  any attempt.
- **Record**: a node of one type with fields, a source, related records and
  stored raw data. Relationships are typed.

## 5. Top bar

Left to right: AKMON mark; Home, Updates, Sources, Explore; the search box;
deployment picker; version picker showing the 12-hex short id and publish
time; theme picker; Open chat. The meander band beneath. At 390 the links
and pickers stack to a second line and the search box takes the full width.
The version read from is always visible.

## 6. Home

Serves: something broke; routine check. Fixed size at any scale.

Reading order:

1. **Identity line.** Deployment, version short id, published time and age.
   A second sentence only when the latest attempt is not the version being
   read: "Latest attempt 16:03 blocked on cmssw-releases; reads still use
   15:55."
2. **Count strip.** Producers by state ("5 sources, 0 enrichers, 0 views ·
   5 updated, 0 failed, 0 never run") and attempts by outcome over the chart's
   range ("14 days: 1 published, 5 blocked"). Each count links to the view
   filtered.
3. **Updates chart.** Attempts over time, one bar per day, published and
   blocked in two colours, range 24 h / 7 d / 14 d / 30 d, a "now" marker.
   Under it the newest four attempts as rows. Link: Updates.
4. **Needs attention.** Ranked: blocked attempt, failed producers, failed
   runtime checks, never-run producers. Subject in ink, cause in grey, time
   right-aligned. At most eight rows, then "and N more" into the view that
   holds them. When nothing is wrong: one sentence with the fact that makes
   it so. Runtime checks (queue, dead-letter, worker evidence) appear only
   here.
5. **Contents.** Records, relationships, record types, relationship types as
   four numbers. Record types by count, top six, then "N more types · M
   records". Relationship types the same. Link: Explore.
6. **Footer.** Count basis and queue state, one line.

Reads: health (vitals, latest_attempt, history, checks), versions, records
by type, sources, ontology rollup for relationship types. New: attempts by
outcome over a time window.

## 7. Updates

Serves: something broke; routine check; just set something up (did the
publish take?).

One table of update attempts, newest first, paged at 50 with a cursor:
time, outcome (Published, Blocked, Running), version short id, change versus
previous, duration, the producer it stopped on. The row for the version
being read is marked "reading". A count strip above: attempts by outcome in
the selected range, each a filter.

Selecting a row opens the attempt detail: header with full version id,
outcome, times; then the stages in order, each with status, count produced
and duration:

1. Sources: N ran, M failed; per source one row.
2. Extraction and enrichers: per enricher, records and relationships added,
   or the withheld reason.
3. Publication checks: passed and failed counts; each failed invariant named.
4. Views: per view, rows produced or the error.
5. Publish: published as version X, or blocked with the one-sentence cause and
   the recorded error text.

Choosing a published attempt here switches every view to that version.

Reads: generations, generation describe (stages), enricher diagnostics, view
diagnostics. New: the attempt must record the producer and stage it stopped
on; today the reason is a bare code and the source is not stored.

## 8. Sources

Serves: something broke (which source); just set something up (did my source
run and what did it add); routine check. Name alternative: Producers.

One table, one row per configured producer, kind chips as the first filter:
Sources, Enrichers, Views. Count strip: by kind and by state. Columns: name
(registry name), kind, state (updated, failed, running, never run, excluded,
withheld), last run with age, next due when the registry declares a cadence,
records and relationships contributed to the selected version, last error
first line. Default order: failed, never run, running, then by last run.
Filter by name and state, sortable, paged at 100. Producers with runs but no
configuration appear at the bottom as "not configured".

Selecting a row opens the rail: kind and adapter, settings with credentials
omitted, scope; run history as a flat table (time, outcome, items in, items
out, duration, phase reached); for a failed run the one-sentence cause and the
full recorded error text; which record types this producer emits.

Must fix by design: registry name and runtime key collapse onto one row.
Today Archiv3 returns eleven rows for five sources.

Reads: sources, source detail, source diagnostics, enricher diagnostics, view
diagnostics. New: one producers read that unions the three kinds with a
common row shape.

## 9. Explore

Serves: looking for a specific record; just set something up (are my records
there). Base: #1931's Search view.

Landing: the search box focused, and beneath it the contents of the selected
version as type chips with counts, ordered by count; a chip lists that type
in id order, paged. One line names which types have searchable text.

Results update as the reader types; exact id matches first, then text
matches with the match highlighted; "Showing 1–20 of N". Arrow keys move,
Enter opens. Result row: label, type, source, snippet.

Record page, in the rail at 1440 and full width at 390: fixed header with
type, record id with copy, source, version read from. Tabs with counts:

- **Fields.** Every attribute as name and value; hidden values say "hidden"
  with the policy name; long text expands in place.
- **Related (N).** Incoming and outgoing by relationship type; a row opens
  that record in the same rail with a back stack; cap and omitted count
  stated.
- **Source.** Source identifier, source record id and revision as policy
  allows, producer evidence. Neighbours are never shown as provenance.
- **Raw.** The stored node row after policy, the fact-log entries, and the
  retained payload when the source stores raw and policy allows; otherwise
  one line naming which condition is unmet. Monospace, copyable, unedited.

The equivalent `okg search` command under the results, after a search only.
Chat deep links open the record page at the version the chat read.

Reads: existing #1931 reads. New: record detail returns the stored row, fact
log and retained payload or refusal reason.

## 9a. Fixtures and stories

Every story above names the fixture that tests it. The design-phase PACT
(`operator-ui-redesign-mocks`) captures each view against each fixture, and a
view's static approval is judged story by story.


### 9.1 The record page, thought through (2026-09-19, proposal)

Facts from the substrate. "Raw" is three things: the stored node row, the
edge rows, and the retained source payload, which is always JSON (a canonical
record set), chunked at 1 MiB with a byte count and SHA-256, and refusable by
policy. The human-readable content is not in "raw": a document's body is an
attribute section, its chunks are separate records linked by `contains`, a
code file's content is an attribute. The original artifact (PDF, Markdown,
HTML, image, code file) exists on the deployment's disk when the source keeps
a corpus root (doc_corpus, upload staging) or an HTTP cache, with `format`,
`path` and `byte_count` recorded on the record; git-backed and web records
carry `citation_url`, a truthful link to the record where it lives.

Header: "Open in source" (external link) whenever the record carries
`citation_url` or a URL attribute.

Tabs, named for what they hold:

| Tab | Holds | Rendering |
|---|---|---|
| Fields | scalar attributes; hidden ones say hidden with the policy | table |
| Content | the artifact as a person reads it, when the record has one | by media type: PDF in the browser's viewer; Markdown rendered (sanitised) with a Rendered / Source toggle; HTML in a sandboxed frame; images inline; code with line numbers and language; anything else as download. Toolbar: format, size, SHA-256, "Open original" (new tab), "Download", Copy. When the original is gone or policy refuses it: one sentence saying which, then the extracted text. Beneath: the record's chunks in order. A chunk's page says "part of <document>" first |
| Related | one table, relationship-type chips with counts, direction toggle, paged at 50; row opens the record | table |
| Provenance | source, source record id and revision, fact log, the attempt that last wrote it (link to Updates), enricher contributions | tables |
| Raw | stored row as a foldable JSON tree; edge rows as stored; retained payload pretty-printed inline to 256 KB, else first 200 lines with "Open raw" and "Download"; byte count and SHA-256 always; refusal reason when refused | mono blocks, each with Copy |

Backend reads this needs: (a) a raw read: row, edges, bounded payload with
size and hash, plus a raw endpoint serving the full payload; (b) an artifact
read: whether the original exists, its media type, size and hash, plus an
artifact endpoint streaming the bytes. Both governed by the same source
policy that gates the retained payload: serving an original file must never
bypass the masking applied to attributes. (Operator, 2026-09-19: agreed.)

Decided 2026-09-19 (operator): the Content tab shows BOTH the rendered
artifact and the extracted text, and uses third-party libraries for the
rendering: pdf.js for PDF, react-markdown with remark-gfm and rehype-sanitize
for Markdown, a syntax highlighter for code, a sandboxed frame for HTML,
native images. Viewers load lazily so Home's bundle budget is untouched.

## 10. Backend changes the views need

1. Attempt metadata records the stage and producer it stopped on.
2. One producers read: sources, enrichers and views in one row shape, registry
   name and runtime key collapsed.
3. Attempts by outcome over a time window.
4. Record detail with raw layers.
5. A cursor on the attempts read (today capped at 50).

Each is its own PACT task and lands before or with the view that needs it.

## 11. Verification

- Runbook gate per view: reference pinned from `research/` and approved by
  the operator; slice defined; static proof at 1440 and 390 on fixture data;
  operator approval recorded under the PACT's `ui/approvals/static/`; live
  proof on the Archiv3 copy with one published and one blocked attempt, and on
  okg-workspace for enrichers and views; sign-off under `ui/approvals/live/`.
- Lanes `operator-ui-unit-contract` and `operator-ui-audit-linux` pass, or
  the PACT retires the canvas-only audit jobs. Test-count drop inside the
  per-platform allowance.
- Bundle works under `/console/`.
- Docs: `docs/operate/operator-ui.md` and the daily-use guide name the four
  views.

## 12. Delivery

Four view PACTs against `dev`, in this order, each preceded by the backend
task it needs: Home, Updates, Sources, Explore (rebased on #1931). The
delete-four-views branch (step 2 of #1924) lands first as-is. `/console` and
the front-door route last.

## 13. Open

1. Name of the producer view: Sources or Producers.
2. Top bar, to confirm.
3. The operator's two pasted images of 2026-09-18, if they are references.
