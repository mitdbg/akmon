"""The startup wire cost every MCP session pays, measured and pinned.

PACT `mcp-tool-surface-profiles` (issue #1457):
req.mcp.startup-budget-and-restart-contract and
req.mcp.instructions-follow-the-surface.

Hermetic by construction. ``OKG_MCP_SURFACE_PROBE=1`` makes ``build_server``
construct both connection pools UNOPENED, so the real tool surface registers
against an unreachable loopback DSN with no database anywhere. Nothing in this
module needs Postgres.

The strings measured are the SDK's own ``tools/list`` serialization and the
``initialize`` instructions, not a proxy. Since `surface-token-budget` (Jason,
2026-10-07, ``tokenbudget A``) every pin and ceiling here is in TOKENS, counted
with tiktoken's ``cl100k_base`` over those exact strings, loaded hermetically
from the copy litellm ships. cl100k is a proxy for Claude's own tokenizer,
which is not published for offline use. Byte figures stay in the comments and in
``breakdown()`` as context; no ceiling or baseline pin is in bytes.
"""
from __future__ import annotations

from asyncio import to_thread as asyncio_to_thread
from pathlib import Path

import pytest

from okg.substrate.mcp.capability_probe import (
    NOT_AVAILABLE_HEADER,
    split_generated_instructions,
)
from okg.substrate.mcp.operators import OPERATOR_TOOLS, WRITE_ADMIN_TOOLS
from okg.substrate.mcp.server import (
    OPERATOR_INSTRUCTIONS,
    operator_instructions_for,
)
from okg.substrate.mcp.surface_budget import count_tokens, measure_surface
from okg.substrate.mcp.tool_profiles import (
    BUILTIN_TOOL_PROFILES,
    tools_named_in,
)

pytestmark = pytest.mark.anyio


# Batch texts adds 753 B to every inspect-serving surface: the bounded keyed
# request schema plus truthful continuation guidance. Hard ceilings, tool
# counts and WRITE_MINUS_READ_BYTES remain unchanged.
#
# ── pinned baselines ──────────────────────────────────────────────────
#
# EXACT, not ceilings. PACT `typed-operator-inputs` (issue #2009) replaced
# stale ceilings with measured numbers; PACT `operator-output-contract`
# (issue #2010) re-pins them because every operator now also publishes an
# `outputSchema`.
#
#              pre-sprint  after #2009  after #2010  after #2012  after #2013  after #2156
#   full read     17,391 B     17,138 B     19,498 B     18,663 B     19,167 B     19,174 B
#   discovery     10,728 B     10,506 B     11,834 B     10,999 B     11,043 B     11,046 B
#   reader            —            —        19,498 B     18,663 B     19,167 B     19,174 B
#   write/admin   25,914 B     23,390 B     25,554 B     25,554 B     26,058 B     26,065 B
#   inspect+srch      —            —         9,687 B      8,852 B      8,896 B      8,898 B
#
# #2156 moved every read tool by 1 B: `max_bytes` lost its published
# `{"default":65536}` (17 B) and is now a plain optional `{"type":"integer"}`
# (18 B). The SDK filled that published default into every call, so no
# deployment default could ever apply. The live-http-ci fixture moved
# 11,999 -> 12,002 (read) and 13,030 -> 13,033 (write). `WRITE_MINUS_READ_BYTES`
# is unchanged: the seven read tools are served on both surfaces.
#
# #2668 moved every surface that serves `inspect` by 318 B, measured: the
# new `text` target member (a record's chunk text in reading order, paged by
# bytes) is 232 B of input schema and one docstring clause is the rest. Its
# result reuses the declared `items` and `continuation_token` fields, so the
# output schema did not grow. After #2668: full read 19,492 B, discovery
# 11,364 B, reader 19,492 B, write/admin 26,383 B, inspect+search 9,216 B,
# opensearch-reference 12,320/14,102 B, live-http-ci 12,320/13,351 B.
# `WRITE_MINUS_READ_BYTES` is unchanged.
#
# Merged with #2676 (which had moved every `search`+`expand` surface by
# 1,140 B of instructions), #2668 also rewrote #2676's whole-document
# sentence: the path-expand recipe (id order, no cursor over the byte
# budget, nothing under Nomos enforce) became the `inspect` kind "text"
# read, plus one expand-only sentence. Instructions on the full read
# surface went 3,493 -> 3,247 B (-246 B); on surfaces that serve `inspect`
# but not `expand` (discovery, inspect+search) they grew by 336 B, since
# the whole-document sentence is now served there. Measured totals, #2676
# alone -> both: full read and reader 20,314 -> 20,386 B (+318 - 246),
# discovery 11,556 -> 12,210 B (+318 + 336), write/admin 27,205 -> 27,277 B,
# inspect+search 9,408 -> 10,062 B, opensearch-reference 13,142/14,924 ->
# 13,214/14,996 B, live-http-ci 13,142/14,173 -> 13,214/14,245 B.
# One more `inspect` sentence (issue #2669: a `file:` node's `documents`
# list names the `doc:` node to read) then added 108 B to every surface
# that serves `inspect`: full read and reader 20,494 B, discovery 12,318 B,
# write/admin 27,385 B, inspect+search 10,170 B, opensearch-reference
# 13,322/15,104 B, live-http-ci 13,322/14,353 B.
#
# #2012 split `inspect`'s seventeen target kinds by what the surface serves.
# A read launch publishes the eleven read kinds; the admission ledger, the
# curation queue, the two record-shape kinds and `surface` are withheld,
# because a read surface has no `record` tool and no expansion door. Every
# read surface drops 835 B, all of it `inspect`'s input schema (3,098 ->
# 2,263 B).
#
# THE WRITE/ADMIN NUMBER WAS THE CONTROL, frozen at 25,554 B so a read-surface
# narrowing could be told from a read-surface loss. #2013 moves it, because
# `expand` is served on BOTH surfaces and gains a `live` parameter there.
#
# Two assertions replace the frozen number, and they are a better control
# than it was. Read and write move by the SAME 504 B, and the DIFFERENCE
# `write - read` is pinned at 6,891 B -- the eleven write/admin tools, which
# this change does not touch. That number held before (25,554 - 18,663) and
# holds after (26,058 - 19,167), so it does not have to be re-derived every
# time a shared read operator grows, which is exactly what made the frozen
# figure brittle the moment one did.
#
# #2013's 504 B on a surface serving `expand`: 186 B for the `live` parameter
# (a `LiveCall` $defs plus a $ref), 44 B for `inspect`'s `live_relations`
# output field, 43 B for `expand`'s `external_live` output field, and 231 B
# for the sentence in `expand`'s docstring that tells an agent the parameter
# exists and that it does not disturb the graph rows. `discovery` and the
# two-tool profile move by only 44 B because neither serves `expand`.
#
# The output contract costs 2,360 B on the read surface, 264 B of that
# from declaring the refusal field on all seven tools for
# req.ooc.error-bound. Publishing the full
# result envelope on all seven tools would have cost 14,049 B — 8.7 KB of it
# the same envelope repeated — so only `inspect` carries the full shape and
# the six paging operators publish the subset a caller needs to page and to
# know which generation answered. See `MinimalResult` for why.
#
# #2792 (PACT `mcp-schema-inline-types`) inlines every `$ref` and states a
# top-level `type` on every union, so an Ollama-served model sends object
# arguments as objects rather than JSON strings, and publishes `max_bytes`'s
# 4,096 B floor on the seven read tools. The same change types `search.filter`
# as its three real keys (#2793 part 2), which costs +211 B on every surface
# that serves `search` (+132 B schema, +79 B for the docstring sentence that
# sends conditions to the `filter` tool):
#
#                 before   inlining   +search.filter   change
#   full read     20,314    20,908        21,119         +805
#   reader        20,314    20,908        21,119         +805
#   discovery     11,556    11,535        11,746         +190
#   write/admin   27,205    27,675        27,886         +681
#   inspect+srch   9,408     9,366         9,577         +169
#
# Where the read +594 B goes: `filter`/`map`/`aggregate`/`expand` each carry
# their own inlined predicate instead of one `$defs` entry per tool (`filter`
# has two, +464 B); `"type":[...]` on each union (`generation_id` on every
# tool, +28 B each); `"minimum":4096` on `max_bytes` (+16 B per read tool).
# `inspect` SHRINKS (-111 B read, -291 B write): its members were each
# published once in `$defs` plus a pointer, and inlining drops the pointers.
# That asymmetry, plus `record` and `propose` each gaining a typed
# `generation_id` (+28 B), is why `WRITE_MINUS_READ_BYTES` moves this time.
#
# Merged with #2668 (measured on the merged tree, not summed): every surface
# is #2792's delta plus #2668's delta, minus 33 B. #2668's `text` target
# member was published as a `$defs` entry plus a pointer; inlining keeps its
# 198 B body and drops `{"$ref":"#/$defs/TText"}` (24 B), the `"TText":`
# key (8 B) and one separator (1 B). The member itself publishes no pointer,
# a typed union, and the same keys as before.
#
#                 #2792 alone   #2668 alone   merged   vs #2792   vs #2668
#   full read        21,119        20,494     21,266     +147       +772
#   reader           21,119        20,494     21,266     +147       +772
#   discovery        11,746        12,318     12,475     +729       +157
#   write/admin      27,886        27,385     28,033     +147       +648
#   inspect+srch      9,577        10,170     10,306     +729       +136
#
# Each assertion below fails in BOTH directions, so an addition and a silent
# loss are equally visible. Update a number deliberately, with the new
# measurement in the review.
#
# #2793 moved every surface that serves `inspect` by 71 B, measured on the
# tree merged with #2792 and #2668: `inspect`'s output schema now admits
# `columns` as `{name, type}` objects as well as names
# (`{"additionalProperties":{"type":"string"},"type":"object"}` in an
# `anyOf` with the string), because `inspect` kind `view` returns them and
# the string-only schema refused every view describe. Read and write move
# together, so `WRITE_MINUS_READ_BYTES` is unchanged. Full read and reader
# 21,266 -> 21,337 B, discovery 12,475 -> 12,546 B, write/admin
# 28,033 -> 28,104 B, inspect+search 10,306 -> 10,377 B.
#
# #3059 removed every `outputSchema` (each result is now one text block)
# and added the reworded untrusted-data contract plus the one result-text
# line to the instructions (+425 B on every surface). Measured: full read
# and reader 21,337 -> 17,893 B (-3,869 schemas, +425 instructions),
# discovery 12,546 -> 10,786 B, write/admin 28,104 -> 23,567 B (-4,962,
# +425), inspect+search 10,377 -> 9,011 B.
#
# search-result-shape (#3069/#3070/#3071) moved every surface that serves
# `search` by 177 B: `rank_by` gained `records` and the description says
# lexical results default to one card per record. Full read and reader
# 17,893 -> 18,070 B, discovery 10,786 -> 10,963 B, write/admin
# 23,567 -> 23,744 B, inspect+search 9,011 -> 9,188 B.
#
# #3067 (PACT mcp-contains-honest-3067), measured on #3127's tree, moved
# every surface that serves `search` by 23 B and every surface that also
# serves `filter` by another 175 B: `filter`'s description now says
# `contains` is JSON containment on attrs and that no operator matches
# substrings, and `search` no longer sends substring conditions to
# `filter`. Full read and reader 18,070 -> 18,268 B, discovery
# 10,963 -> 10,986 B, write/admin 23,744 -> 23,942 B, inspect+search
# 9,188 -> 9,211 B.
#
# read-model-s6-page-next, measured against read-model-s2-handle's tree:
# `inspect` and `expand` publish `next` (a token passed back alone; +25 B of
# input schema each), `target`/`input` stay published as required;
# `inspect`'s description gains one sentence (+65 B), `expand`'s says
# `next` for `continuation_token` (-6 B), and the whole-document
# instruction says `inspect(next=<token>)` (+4 B). Full read and reader
# 18,268 -> 18,382 B, discovery 10,986 -> 11,081 B, write/admin
# 23,942 -> 24,056 B, inspect+search 9,211 -> 9,306 B.
# Recovery repair publishes a target/input-or-next alternative: +148 B
# inspect, +158 B expand, +306 B on surfaces containing both. Expand
# retains its existing string anchors. Measured on lab-host.
# Locations add another measured 178 B on every inspect surface.
# mcp-grep-3067 (#3067 phase 1), measured on mcp-contains-honest-3067's tree
# (35693aa03c), moved every surface that serves `search` by 255 B:
# `method` gained `grep`, `search` gained the `path_prefix` parameter, and
# its description gained one sentence on the grep grammar (including the
# `re:/<pattern>/` regex syntax). The instructions did not change (3,780 B,
# under the 4,000 B ceiling). Full read and reader 18,268 -> 18,523 B,
# discovery 10,986 -> 11,241 B, write/admin 23,942 -> 24,197 B,
# inspect+search 9,211 -> 9,466 B.
#
# mcp-grep-rank, measured on mcp-grep-3067's tree (4ee741e285), moved every
# surface that serves `search` by 58 B, all of it `search`'s description:
# grep's sentence now names the `path:<text>` term and says records rank by
# BM25 on the words, else id order (it said "unranked ... records in id
# order"). The instructions did not change (3,780 B on the full read
# surface, under the 4,000 B ceiling). Full read and reader 18,523 ->
# 18,581 B, discovery 11,241 -> 11,299 B, write/admin 24,197 -> 24,255 B,
# inspect+search 9,466 -> 9,524 B.
#
# mcp-grep-page (decision 228), measured on mcp-grep-rank's tree, moved
# every surface that serves `search` by 92 B, all of it `search`'s
# description: one sentence teaching that all terms must match one text, to
# use 1-2 distinctive terms, and re:/a|b|c/ for variants. The instructions
# did not change. Full read and reader 18,581 -> 18,673 B, discovery
# 11,299 -> 11,391 B, write/admin 24,255 -> 24,347 B, inspect+search
# 9,524 -> 9,616 B.
#
# mcp-grep-page (decision 260a) then moved every surface that serves
# `search` by 39 B, all of it `search`'s schema: the `continuation_token`
# parameter a grep page stopped at a bound returns (the result names it, so
# the description did not change). Full read and reader 18,673 -> 18,712 B,
# discovery 11,391 -> 11,430 B, write/admin 24,347 -> 24,386 B,
# inspect+search 9,616 -> 9,655 B.
# #3209 adds 102 B to search's description for catalog title/path and typed
# dates. Input schemas, hard ceilings, tool counts and write-minus-read stay
# unchanged; these exact measurements include only that intended description.
# Combined grep444 B + next/location598 B added to where/date foundations.
# Exact lab-host measurement9148; hard ceilings and tool counts retained.
# #3151 slice 1 (PACT prompt-compiler-3151-slice1) makes the `search` schema
# follow what the server can run, so every surface that serves `search` is
# now pinned PER CAPABILITY FIXTURE. The pins above and below are the `full`
# fixture (every method, every rank_by value), which `_hermetic_probe` sets:
# the only change there is the new items enum on `select` -- the union of
# every method's selectable fields, 33 names -- which adds 443 B to
# `search`'s input schema on every surface that serves it. Full read and
# reader 18,268 -> 18,711 B, discovery 10,986 -> 11,429 B, write/admin
# 23,942 -> 24,385 B, inspect+search 9,211 -> 9,654 B. Instructions do not
# move: with every capability available there is no generated block.
# PACT lexical-index-scan-cap-3162 then adds 195 B to every surface that
# serves `search` (the `completeness` parameter and one description
# sentence): full read and reader 18,906 B, discovery 11,624 B, write/admin
# 24,580 B, inspect+search 9,849 B. Measured after merging dev.
# The `none` fixture (no embedder, no aliases, no rollups) is pinned in
# its own section below.
# The stacked read-model-s3-where-scope contract replaces the published legacy
# filter arguments with where/scope and adds truthful typed-date guidance.
# Its standalone +102 B description measurement remains historical; the
# capability-shaped integrated schema requires fresh exact measurements.
# Measured integrated delta: search wire +512 B, instructions -17 B,
# shared prefix predicate schema +9 B in expand/map/aggregate and +18 B
# in filter. Full surfaces grow 540 B; discovery/reference/live 504 B;
# inspect+search 495 B. Evidence binds old3158 and finalca72 serialization.
# Keep fixture-aware assertions, hard ceilings, counts and admin difference.
# Merge of the grep stack with #3226's shared scanners (lane B, 10-03): the
# two deltas add (grep/next/location +1,042 B, select enum and integrated
# where +452 B over their common base), and grep's own selectable fields
# join the published `select` enum (+27 B, grep is published unprobed).
# Measured on lab-host at the merged head.
# Merge of #3214's final head (with #3209 0ea8e02b6d) into the grep stack
# (lane B, 10-04): #3209's re-review makes the node-lookup instruction
# teach a typed lookup, +103 B of instructions on every surface that serves
# `search` (the `none` pins below move by the same amount).
#
# read-model-s3-where-scope (#3209), measured on top of #3151 slice 1:
# `search` publishes `where` (with `prefix`) and `scope` in place of
# `filter`, `subtype_filter` and `attr_filters`, its description teaches
# `where` and names the date meanings, the predicate op list gains `prefix`
# on every operator, and the node-lookup instruction sentence uses `where`.
# The same deltas it measured alone on #3067's tree, so the two changes do
# not interact: full read and reader +531 B (18,711 -> 19,242), discovery
# +495 B (11,429 -> 11,924), write/admin +531 B (24,385 -> 24,916),
# inspect+search +486 B (9,654 -> 10,140). The re-review then made the
# node-lookup instruction teach a typed lookup (`type` plus an exact
# `attrs.path`, which narrow the page under every Nomos mode): +103 B of
# instructions on every surface that serves `search` (19,345; 12,027;
# 25,019; 10,243).
#
# read-model-s2-handle (#3214), measured on top of #3209 (bd503776fd,
# f82d07f560 and 0ea8e02b6d): `handle` joins every search method's
# selectable fields, so the `select` items enum gains `,"handle"`, exactly
# 9 B in `search`'s input schema (2,038 -> 2,047 B); instructions do not
# move. +9 B on every surface that serves `search`.
#
# read-model-s4-list (#3225), measured on top of #3214 at 4dbd276f54 and
# re-measured on #3214 f84b465b95 merged with dev 66e2e4cc39 (same delta):
# `search` publishes `view` (`cards`/`list`), `limit` loses its published
# default of 10 (an omitted limit now means 10 for cards and 100 for a
# list), and the description names the list view. Search input schema
# 2,047 -> 2,083 B (+36), description +146 B, instructions unchanged:
# +182 B on every surface that serves `search`. Refusing a boolean `limit`
# is a validator only; the published `limit` schema does not move.
#
# read-model-s6-page-next, measured alone against an earlier read-model-s2-handle tree:
# `inspect` and `expand` publish `next` (a token passed back alone; +25 B of
# input schema each), `target`/`input` stay published as required;
# `inspect`'s description gains one sentence (+65 B), `expand`'s says
# `next` for `continuation_token` (-6 B), and the whole-document
# instruction says `inspect(next=<token>)` (+4 B). Full read and reader
# 18,268 -> 18,382 B, discovery 10,986 -> 11,081 B, write/admin
# 23,942 -> 24,056 B, inspect+search 9,211 -> 9,306 B.
# Recovery repair publishes a target/input-or-next alternative: +148 B
# inspect, +158 B expand, +306 B on surfaces containing both. Expand
# retains its existing string anchors. Measured on lab-host.
#
# Combined base (read-model-s6-page-next + #3214 at f84b465b95 + dev at
# 66e2e4cc39, measured on lab-host): the two changes touch different tools'
# schemas (`inspect`/`expand` versus `search`), so their deltas add. Full
# read and reader 19,354 + 114 + 306 = 19,774 B; discovery 12,036 + 95 + 148
# = 12,279 B; write/admin 25,028 + 420 = 25,448 B; inspect+search
# 10,252 + 95 + 148 = 10,495 B (`inspect` only).
#
# lexical-index-scan-cap-3162 (#3219), merged onto the chain above: `search`
# gains the `completeness` parameter and one description sentence, the same
# +195 B it measured alone on dev, on every surface that serves `search`.
#
# Merge of the read-model chain through #3219 (54135d276a) into the grep
# stack (lane B, 10-04). Each pin is dev 66e2e4cc39's value plus each
# change's own measured delta, the deltas adding because they touch
# different parameters or sentences:
#   chain (#3209 where/scope +531 B and typed lookup +103 B, #3214 handle
#   +9 B, #3217 next +114 B and recovery repair +306 B) = +1,063 B full,
#   +850 B discovery (495 + 103 + 9 + 95 + 148), +841 B inspect+search
#   (486 + 103 + 9 + 95 + 148);
#   grep stack (grep 255 + 58 + 92 + 39 = 444 B, locations +178 B, grep's
#   select fields +27 B) = +649 B on every surface;
#   #3219 completeness = +195 B on every surface that serves `search`.
# Full read and reader 18,711 + 1,063 + 649 + 195 = 20,618 B; discovery
# 11,429 + 850 + 649 + 195 = 13,123 B; write/admin 24,385 + 1,063 + 649 +
# 195 = 26,292 B; inspect+search 9,654 + 841 + 649 + 195 = 11,339 B.
#
# read-record-windows (#3223) then adds 363 B to every surface serving
# `search`: the `view`, `matching` and `context_chars` parameters and one
# description sentence teaching `view="windows"`; instructions do not
# change. Merge of the chain through #3218 (4f3a532591) into record
# windows (lane B, 10-04): every pin is the chain head's pin + 363 B.
# Check against #3223's own parent (f467f7fd06): its pins minus 363 B
# sit exactly 298 B (#3209 typed lookup 103 + #3219 completeness 195)
# under the chain head's on every surface, so the deltas add.
# Full read and reader 20,618 + 363 = 20,981 B; discovery 13,123 + 363
# = 13,486 B; write/admin 26,292 + 363 = 26,655 B; inspect+search
# 11,339 + 363 = 11,702 B.
#
# Merge of the chain through #3215 (60f7a4064d) into read-text-batch (lane
# B, 10-04): every pin is the chain head's pin + 753 B, batch's measured
# delta on every surface that serves `inspect` (its pins sit 753 B over
# their merge base 5d146523fc on every surface; the chain's pins sit
# 2,115 B full/write and 2,079/2,070 B discovery/inspect+search over the
# same base, all on `search`, so the deltas add). Full read and reader
# 20,981 + 753 = 21,734 B; discovery 13,486 + 753 = 14,239 B; write/admin
# 26,655 + 753 = 27,408 B; inspect+search 11,702 + 753 = 12,455 B.
#
# Merge of the chain through #3224 (cb74b49b48) into read-model-s4-list
# (#3225, lane B, 10-04). #3225 measured +182 B on every surface serving
# `search` over #3214 + dev 66e2e4cc39 (19,354 full read), the same base the
# chain's pins grew from. Of it, `view` (`"enum":["cards","list"]`) is 49 B
# of schema, `limit` dropping `"default":10` is -13 B, and the list sentence
# is +146 B of description. The chain already publishes `view` as
# `{"const":"windows","type":"string"}`; merged, `view` publishes
# `{"enum":["cards","list","windows"],"type":"string"}`, +16 B over the
# chain's (and +10 B, `,"windows"`, over #3225's). So every pin is the chain
# head's pin + 149 B (-13 + 16 + 146). Check from #3225's side: its pin
# 19,536 + the chain's 2,380 B over the shared base (21,734 - 19,354)
# - 33 B (the chain's 43-byte `view` property replaced by #3225's 10-byte
# enum addition) = 21,883, the same. Measured: these exact pins pass on
# lab-host at c1f8829e41.
# Full read and reader 21,734 + 149 = 21,883 B; discovery 14,239 + 149 =
# 14,388 B; write/admin 27,408 + 149 = 27,557 B; inspect+search
# 12,455 + 149 = 12,604 B.
# lexical-quoted-phrases-3064 adds `phrase_match` to the `search.select`
# items enum (`"phrase_match",`, +15 B) on every surface serving `search`:
# 21,898 full read and reader, 14,403 discovery, 27,572 write/admin,
# 12,619 inspect+search. Measured on lab-host at the #3343 head.
#
# Result references, slice 2 (#3222, read-result-references-3222-consumers),
# measured tool by tool against the same build without it: `search` +222 B
# (`retain`, `"metadata"` in the `view` enum, one docstring sentence),
# `filter` +283 B (the `{"ref"}` input member, `retain`, one sentence),
# `map` +163 B and `aggregate` +135 B (the `{"ref"}` input member). Every
# surface serving all four moves +803 B; discovery (search, inspect,
# aggregate) +357 B; a surface serving `search` but none of the three
# consumers +222 B. Full read and reader 21,883 + 803 = 22,686 B; discovery
# 14,388 + 357 = 14,745 B; write/admin 27,557 + 803 = 28,360 B;
# inspect+search 12,604 + 222 = 12,826 B.
#
# Result references, slice 3 (#3222, read-result-references-3222-summary):
# `search` alone moves +226 B (`"metadata"` published in the `view` enum,
# the `summary` parameter, one docstring sentence), so every surface that
# serves `search` moves +226 B. Measured on lab-host.
# Full read and reader 22,686 + 226 = 22,912 B; discovery 14,745 + 226 =
# 14,971 B; write/admin 28,360 + 226 = 28,586 B; inspect+search
# 12,826 + 226 = 13,052 B.
# Plus lexical-quoted-phrases-3064's phrase_match (+15 B) from train 46c.
#
# select-field-instructions (lane G, `selectdoc A`): `search` +592 B (the
# generated per-method `select` description), `expand` +258 B (the select
# sentence naming its edge and node row shapes). Every surface serving both
# moves +850 B; discovery and inspect+search +592 B. Under the `none`
# profile `search` moves +575 B (no alias line). Measured on lab-host.
#
# tool-text-accuracy (lane G, C4 instructions audit): description fixes on
# search, inspect, expand, filter and aggregate, net of trimmed wording, and
# the instructions drop an unreadable docs pointer. Full read and reader
# +218 B on top of select-field-instructions. Measured on lab-host.
#
# tool-description-dedent (lane G; Jason, via orchestrator chat 2026-10-07,
# `readtrim A`): every tool description is published without its source
# docstring indentation (`publish_clean_descriptions`); no word changes.
# Full read and reader -740 B (23,995 -> 23,255), discovery -462 B,
# write/admin -1,018 B, inspect+search -420 B. Declared surfaces:
# okg-workspace as full read and write/admin; opensearch-reference read
# -574 B (16,996 -> 16,422) and write -616 B (18,439 -> 17,823);
# live-http-ci read -574 B and write -616 B (17,852 -> 17,236). The `none`
# profile moves as `full`: -740, -1,018 and -420 B.
#
# search-children-of-3363 (lane G, `childrenof B`): `scope` publishes
# `children_of` and `edge_type` with one docstring clause, paid for by
# trimming the search description. Every surface serving `search` moved
# -55 B (merged after the dedent; token delta in the re-base below).
#
# ── token re-base (PACT surface-token-budget) ─────────────────────────
#
# Jason, via orchestrator chat 2026-10-07, `tokenbudget A`: the budget
# measures tokens, not bytes. A byte ceiling overstates whitespace: the
# dedent above removed 740 B but only 96 tokens. Every pin below is now
# cl100k_base tokens of the same wire JSON and instructions the byte pins
# counted (`count_tokens`); cl100k is a proxy for Claude's tokenizer. The
# byte history above is kept as context and is no longer asserted.
#
# Measured hermetically on dev 446c3b1b5f1 (before the dedent) and on this
# branch (after it), tokens with bytes alongside:
#
#                 dev 446c3b1b5f1         this branch (dedent)
#   full read     5,697 tok (23,995 B)    5,601 tok (23,255 B)    -96 tok
#   reader        5,697 tok               5,601 tok               -96 tok
#   discovery         —                   3,673 tok (15,290 B)
#   write/admin   6,896 tok (29,669 B)    6,773 tok (28,651 B)   -123 tok
#   inspect+srch      —                   3,199 tok (13,355 B)
#
# Of the full read surface's 5,601 tokens, 4,636 are the seven tools' wire
# JSON and 965 the instructions.
# Merged with search-children-of-3363 (-1 token on every surface): full read
# and reader 5,600, discovery 3,672, write/admin 6,772, inspect+search 3,198.
# The ceilings stay 5,751 and 6,923 tokens (set at 5,601 and 6,773 + 150).
# select-fields-by-method (lane G, benchmark B's 64 select refusals), on
# `search` only: +21 tokens to list identifier_mentions' six fields whole,
# +2 to put the fields grep lacks first ("grep has no ..."), -2 for "a
# refusal lists their fields", and -23 in the search description (drop
# `matching`'s values and `context_chars`' range, which the schema states;
# drop "AND within one text" and "(bounded candidates)", which the
# description says otherwise; "passages folded in"). Net -2 tokens on every
# surface serving `search`: full read and reader 5,598, discovery 3,670,
# write/admin 6,770, inspect+search 3,196.
# dataset-card (lane I): the served instructions lose okg-internal prose
# (the surface's internal name, generation pinning, session rebinding) and
# the unverified path-word sentence, and gain the dataset card. A server
# built without a database serves the short card that says its facts are
# missing. Net -51 tokens on every surface (instructions 965 -> 914 tokens
# on the full read surface): full read and reader 5,547, discovery 3,619,
# write/admin 6,719, inspect+search 3,145. Measured hermetically.
# dataset-card tool prose (req.dataset-card.tool-prose): descriptions are
# rendered for the served surface (`tool_prose`). `inspect` drops the
# `surface` kind's sentence where that kind is not served (-27) and
# backticks `query` (+2; dropped with its clause where `query` is not
# served); `expand` drops the `live=` paragraph where no live relation is
# declared (-50); `aggregate`'s example names a catalog subtype, here the
# `<subtype>` placeholder, not `pact_change` (-2); `filter` drops "no
# sql_admin" (-2); `search` backticks `inspect` (+2) and lists only served
# reference consumers. Full read and reader 5,547 -> 5,470 (-77),
# discovery 3,619 -> 3,580 (-39), write/admin 6,719 -> 6,670 (-49; it
# keeps `surface`), inspect+search 3,145 -> 3,098 (-47). Measured
# hermetically.
# live-result-sets (lane I; Jason `lrspact A`, `approve lrs`): `search` and
# `filter` publish `retain` (keep one live answer to page or re-read): +6
# tokens on every surface serving them. Full read and reader 5,476,
# write/admin 6,676, opensearch-reference and live-http-ci read 3,852, the
# `none` profile 5,504 and 6,704; discovery and inspect+search do not move
# (measured). The read ceiling stays 5,751 tokens.
# mcp-schema-no-top-level-anyof (lane I; Jason, via orchestrator
# chat 2026-10-08: anyof A): `inspect` and `expand` no longer publish a
# top-level anyOf (Claude Code drops such tools); target, input and next
# state the rule in their descriptions instead. inspect -13, expand -17:
# full read and reader 5,446, discovery 3,567, write/admin 6,646,
# inspect+search 3,085, opensearch-reference and live-http-ci read 3,822,
# the `none` profile 5,474, 6,674 and 3,113.
# mcp-refusals-3470 (lane I2; Jason `grephandle A`): grep serves `handle`,
# so `search.select` no longer names it among grep's missing fields: -2
# tokens on every surface serving `search` (full read and reader 5,444,
# discovery 3,565, write/admin 6,644, inspect+search 3,083,
# opensearch-reference and live-http-ci read 3,820, write 4,155 and 4,030,
# the `none` profile 5,472, 6,672 and 3,111).
# mcp-window-scope-3605 (lane I2; Jason `winscope A`): `search` says windows
# read the scope records, else the grep's own top `limit` records: +12 tokens
# on every surface serving `search` (full read and reader 5,456, discovery
# 3,577, write/admin 6,656, inspect+search 3,095, opensearch-reference and
# live-http-ci read 3,832, write 4,167 and 4,042, the `none` profile 5,484,
# 6,684 and 3,123).
# mcp-read-whole-3634 (lane I2, #3634): `inspect` and the reading
# instructions give a text read's default and largest page and say a cut
# page tells what is left, and `next` may take a new max_bytes (Jason
# `nextresize A`): +55 tokens on every surface (full read and reader 5,511,
# discovery 3,632, write/admin 6,711, inspect+search 3,150,
# opensearch-reference and live-http-ci read 3,887, write 4,222 and 4,097,
# the `none` profile 5,539, 6,739 and 3,178).
# search-options-by-method (#3646): `search` says completeness=exact serves
# lexical, grep and attrs, and that windows take matching='record': +3
# tokens on every surface serving `search` (full read and reader 5,514,
# discovery 3,635, write/admin 6,714, inspect+search 3,153,
# opensearch-reference and live-http-ci read 3,890, write 4,225 and 4,100,
# the `none` profile 5,542, 6,742 and 3,181).
# mcp-inspect-shape-3647 (lane I2, #3647; Jason `nestedargs A`,
# `nodepartial A`): `inspect` says select and max_bytes go beside target with
# one example, the target schema says so too, and an oversize node comes back
# cut: +75 tokens on every surface (full read and reader 5,589, discovery
# 3,710, write/admin 6,789, inspect+search 3,228, opensearch-reference and
# live-http-ci read 3,965, write 4,300 and 4,175, the `none` profile 5,617,
# 6,817 and 3,256).
# inspect-node-batch-3657 (lane RB, #3657; Jason `j4batch A`): the `nodes`
# target (up to 50 node_ids or a result ref) in `inspect`'s schema and one
# description clause: +119 tokens on every surface (full read and reader
# 5,708, discovery 3,829, write/admin 6,908, inspect+search 3,347,
# opensearch-reference and live-http-ci read 4,084, write 4,419 and 4,294,
# the `none` profile 5,736, 6,936 and 3,375). Read is 43 tokens under its
# ceiling and write/admin 15.
# aggregate-date-periods-3658 (lane RB, #3658; Jason `gtdate B`): `aggregate`
# says `by` takes month(date.created) (or day, week, year): +14 tokens on
# every surface that serves `aggregate` (full read and reader 5,722,
# discovery 3,843, write/admin 6,922, the `none` profile 5,750 and 6,950);
# the inspect+search, opensearch-reference and live-http-ci surfaces do not
# serve it and do not move. Read is 29 tokens under its ceiling and
# write/admin 1.
# grep-list-view-3659 (lane RB, #3659; Jason `u3listmatch A`): `search` says
# a list is served on "lexical records or grep" (was "lexical records
# only"): +1 token on every surface (full read and reader 5,723, discovery
# 3,844, write/admin 6,923, inspect+search 3,348, opensearch-reference and
# live-http-ci read 4,085, write 4,420 and 4,295, the `none` profile 5,751,
# 6,951 and 3,376). The sentence pointing list and count questions at the
# grep list was dropped to stay under the ceilings; the guidance rewrite
# (#3666) carries that advice. Read is 28 tokens under its ceiling and
# write/admin is exactly at its 6,923 ceiling.
# (Later #3666 steps, recorded in the delta sums below, brought dev to full
# read and reader 5,640, discovery 3,761, write/admin 6,840, inspect+search
# 3,265, opensearch-reference and live-http-ci read 4,002, write 4,337 and
# 4,212, the `none` profile 5,668, 6,868 and 3,293.)
# read-query-guide-3666 (lane RB, #3666 step M12): `query`'s description
# names the two views and their columns, how to read text and a field, $$
# regexes, the caps and the pin, with one example (query 121 -> 235 tokens
# on the wire, +114). Paid for in the same surfaces: the instructions drop
# the `query` escape-hatch sentence (-20), "(no SQL, no sql_admin)" (-7)
# and say the not-found contract in fewer words (-24); `inspect` drops the
# clause that `query` refuses information_schema (-8; the refusal names the
# `view` path). Net +55 where `query` is served (full read and reader
# 5,695, write/admin 6,895, the `none` profile 5,723 and 6,923) and -30
# where it is not (discovery 3,731, inspect+search 3,235,
# opensearch-reference and live-http-ci read 3,972, write 4,307 and 4,182,
# the `none` inspect+search 3,263). Read is 56 tokens under its ceiling and
# write/admin 28. Measured hermetically on the lab host.
# read-query-guide-3666, second step: the description is written at build
# from the dataset card's facts (`query_guide`); with none (this hermetic
# build) it prints three examples that run on any graph (a count, a join
# through v_edges, a text regex) and drops the cast example and the
# row_cap/timeout_ms words (the schema states them): +18 tokens where
# `query` is served (full read and reader 5,713, write/admin 6,913, the
# `none` profile 5,741 and 6,941). Read is 38 tokens under its ceiling and
# write/admin 10. A live build adds the fields line (at most 480 B).
# read-query-guide-3666, third step: the description says a Postgres regex
# word boundary is \y, not \b (an agent's \b silently found 0 rows): +9
# where `query` is served (full read and reader 5,722, write/admin 6,922,
# the `none` profile 5,750 and 6,950). Read is 29 tokens under its ceiling
# and write/admin 1. Measured hermetically on the lab host.
# read-pages-3b-3666 (lane RB, #3666 step 3b; Jason 2026-10-12 path in
# inspect): `inspect`'s node and text targets take `path` and `source`,
# paid for by shorter wording (inspect -5 tokens); `filter` and `map` take
# `next` for keyset pages past offset 10000, and their `input` (and
# `predicate`) are no longer required (filter and map +4 together). Full
# read, reader and write/admin -1 (5,639, 6,839); surfaces that serve
# `inspect` without `filter` and `map` -5 (discovery 3,756, inspect+search
# 3,260, opensearch-reference and live-http-ci 3,997 read, 4,332 and 4,207
# write; the `none` profile 5,667, 6,867 and 3,288).
# read-text-matches-3666 (lane RB, #3666 step M7): `matches` joins the
# predicate operator enum, published in the schemas of `filter`, `map`,
# `aggregate` and `expand`. `filter` names `matches` on
# `text` in its substring sentence and drops two clauses; `aggregate` names
# `match` in `by` and drops "stable view" and "its legacy". By surface:
# `expand` +2 (enum only), `aggregate` +3, `filter` and `map` together
# -4; `search` publishes no `matches` (its `where` refuses a text
# condition), so it does not move. Measured: full read and reader 5,641,
# discovery 3,764, write/admin 6,841, inspect+search 3,265,
# opensearch-reference and live-http-ci read 4,004, write 4,339 and 4,214,
# the `none` profile 5,669, 6,869 and 3,293. No ceiling moves.
# read-expand-records-3666 (merged with dev's 3a, 5a, the inspect sentence
# and 3b): -1 on every surface that serves `expand` for its description
# and -2 for the reading sentence, which now says a one-hop expand returns
# the linked records; discovery and inspect+search unchanged (full read
# and reader 5,636, write/admin 6,836).
# Both merged (M7 with expand records), measured: -3 from M7's counts where
# the full `expand` description is served (full read and reader 5,638,
# write/admin 6,838, the `none` profile 5,666 and 6,866), -3 on
# opensearch-reference and live-http-ci (4,001 read, 4,336 and 4,211
# write); discovery and inspect+search unchanged. No ceiling moves.
# read-surface-diet-3666 (#3666 step M10): the six read tools publish only
# their shown forms (`lean_surface`); every hidden argument, method and kind
# is still accepted, and `inspect(target={"kind": "surface"})` lists them.
# Measured against dev 02af8953283: full read and reader 5,640 to 3,427,
# discovery 3,761 to 2,382, write/admin 6,840 to 4,618, inspect+search 3,265
# to 2,133. (`query`, outside the six, gains 21 tokens: a pointer to the
# `view` kind for a view's columns, which `inspect` no longer describes.) The six read tools alone on the full read surface: 4,672 to
# 2,498 (inspect 1,182 to 803, search 1,351 to 640, expand 672 to 352,
# filter 620 to 309, aggregate 483 to 236, map 364 to 158). Instructions:
# 847 to 787, the node-lookup and reading lines now in `where`/`select`.
# Narrow surfaces move by tool-conditional text: on inspect+search, search
# is 625 (it drops "for map/filter/aggregate(input=...)" and says "nothing
# matches" for "neither tool matches") and the instructions 756 to 705.
# Under the `none` profile search is 15 fewer again (its select enum has no
# semantic or alias fields) and the instructions keep the generated block:
# full read 5,668 to 3,466 (instructions 901 to 841), write/admin 6,868 to
# 4,657, inspect+search 3,293 to 2,172 (search 1,310 to 610, inspect 1,173
# to 803, instructions 810 to 759). No ceiling moves.
# read-surface-diet-3666 round 2: -14 on every surface (search 640 to 626):
# `search.select` says title and path are in handle and source in
# provenance, where it said how grep's rows differ (a field grep rows lack
# is served with a note). Full read and reader 3,413, discovery 2,368,
# write/admin 4,604, inspect+search 2,119; the six read tools 2,484.
# Round 3: +1 on every surface (search 626 to 627). `search.select` lists
# the lexical fields and how grep's differ again, before the title, path
# and source sentence, and publishes no items enum (the list is said once).
# Full read and reader 3,414, discovery 2,369, write/admin 4,605,
# inspect+search 2,120; the six read tools 2,485.
# read-v2 base (#3666: M10, M7 and the expand any-link fixes merged on dev
# 678b74cc4d0, measured on the lab): the lean surface now carries what dev
# (3b, expand records) and M7 publish: inspect +43 (path and source on node
# and text), expand +39 (record rows, link-row select, target_subtype),
# filter +44 (matches, `next`), map +19 (`next`), aggregate +5 (match in
# by). Full read and reader 3,562, discovery 2,417, write/admin 4,753,
# inspect+search 2,163. No ceiling here moves.
# Then -13 wherever `expand` is served: its description no longer names
# target_subtype, which the lean surface hides. Full read and reader 3,549,
# write/admin 4,740; discovery and inspect+search unchanged.
# Merged (lane M2, 2026-10-14): read-query-guide-3666 on top of 3b and
# read-expand-records-3666 adds both sides' deltas: +82 / -30 where the
# query guide moves a surface, -4 / -5 from 3b and expand (full read and
# reader 5,718, discovery 3,726, write/admin 6,918, inspect+search 3,230).
# read-v2 with 2c (M5 rebuild) and dev through M12 #3729, measured on the
# lab: +69 where `query` is served (M12's generated query guide), -31
# where it is not (M12 trims the instructions and `inspect`). Full read
# and reader 3,618, discovery 2,386, write/admin 4,809, inspect+search
# 2,132. No ceiling here moves.
# Then search's retain sentence names no hidden `input` (it said map/filter/aggregate(input={"ref": id})): -1 on
# full read, reader, write/admin and discovery, +4 on inspect+search
# (there no reader is served and the sentence ends "as a result
# reference"). Full read and reader 3,617, discovery 2,385, write/admin
# 4,808, inspect+search 2,136.
# Then inspect drops the `surface` sentence (-12 on every surface): full read and reader 3,605, discovery 2,373,
# write/admin 4,796, inspect+search 2,124.
# loose-match-3666 (like and like_exact): +12 wherever `search` is served
# (its where sentence names filter's like for substrings) and +15 more where
# `filter` is (its contains sentence names like). Full read and reader
# 3,632, discovery 2,385, write/admin 4,823, inspect+search 2,136. No
# ceiling moves.
# publish-paging-args-3666, measured on the lab: offset published again on
# every tool whose cut page names it (+15 each, bare; expand +7 more for
# "One hop only."), search's continuation_token (+8, bare) and max_chars
# (+36 with its line), +126 on the six read tools; the instructions name
# max_chars again and the node id schemes (+74). Then +63 more on the
# read tools when the arguments other hints name were published: search's
# expand_concepts (+8, bare), map's input (+19) and aggregate's input
# (+36), both untyped with one line. Full read and reader 3,632 to 3,895
# (+263), discovery 2,385 to 2,577 (+192), write/admin 4,823 to 5,086
# (+263), inspect+search 2,136 to 2,277 (+141). No ceiling moves.
# read-passage-rows-3666 (segment B, merged onto the above), measured on
# segment B: -22 on every surface. The
# read tools -8 (search hides max_bytes, its max_chars line names no
# default, its select line adds the attrs.<key> form) and the instructions
# -14 (the reading sentence says a row shows the matched passage and the
# reply names the call that reads a record, and no longer teaches select
# of attrs or select's field shape).
# Then search publishes `view` again (D1: "list" gives head lines only;
# its line and its enum): +29 on every surface.
# Then the reading sentence scopes order_basis/coverage/heading_path to a
# document read and says a ticket reads as fields, description and dated
# comments (it no longer prints those three): +29 on every surface.
# Then the prompt redesign, server half (#3666): the instructions drop
# "Use `search`/`filter` to fetch specific nodes" (NODE LOOKUP and READING
# say how) and "absent at the pin" becomes "the graph lacks" (D8: no
# generation jargon): -14 where search and filter are served, -10 where
# only search is ("Use `search` to fetch specific nodes").
# Segment B's four steps on dev's surface, measured on the lab after the
# merge: +22 where filter is served, +26 where only search is. Full read
# and reader 3,895 to 3,917, discovery 2,577 to 2,603, write/admin 5,086
# to 5,108, inspect+search 2,277 to 2,303. The dataset card and the
# not-available block are not measured here (no database; `full`
# fixture). No ceiling moves.
# Then read-defects-rd-3666 (Format v7) merged onto these pins, which
# already held loose-match-3666's `like` sentence in filter. Per tool,
# measured on the lab against PR 1's head: inspect +4 (`lines` reads "a
# list of" [first,last] line ranges); filter +11 (`contains` is also text
# inside attrs.<key>, ASCII case ignored); search +7 where filter is served
# ("`contains` finds text in attrs.<key>, path or title." in place of
# "filter's like matches substrings") and +9 where it is not (in place of
# "nothing matches substrings"). So +22 where filter is served, +13 where
# only search is. (The merge first restored "`suffix` for a path tail",
# +8 in filter, which dev removed in 38861bcf06; removed again.) Full read
# and reader 3,939, discovery 2,616, write/admin 5,130, inspect+search
# 2,316. No ceiling moves.
# Then Format v10 item C (#3666, counting per value): aggregate's
# description shows by=["match"] with a where text regex, and its `where`
# lists the field `text`: +30 where aggregate is served (full read, reader,
# discovery, write/admin), 0 on inspect+search. Full read and reader 3,969,
# discovery 2,646, write/admin 5,160. No ceiling moves.
# Then read-reply-size-3666, measured on the lab: +29 on every surface, all
# of it in `search`'s description, the one line "Read narrow first: ids and
# titles (select=["node_id","handle"] or a small limit), then read the
# records that matter." (limit=10 to limit=8 in its example is
# token-neutral). Full read and reader 3,998, discovery 2,675, write/admin
# 5,189, inspect+search 2,345. No ceiling moves.
# Then read-search-caps-3666, measured on the lab: +46 on every surface,
# all of it in `search` (description +123 B: 'within="record": each term
# anywhere in a record, in id order; add select=["node_id","handle"] to
# list every match cheaply.'; schema +52 B: the `within` enum unit,
# record). Full read and reader 4,044, discovery 2,721, write/admin 5,235,
# inspect+search 2,391. No ceiling moves.
# Then read-aggregate-variants-3666, measured on the lab: +32 on every
# surface serving `aggregate`, all of it in its description ('; norm(match)
# merges case, space and punctuation variants, words(match) word order
# too; one (group) in the regex is the text counted.'). Full read and
# reader 4,076, discovery 2,753, write/admin 5,267; inspect+search
# unchanged. No ceiling moves.
# Then read-names-list-3666, measured on the lab: +21 on every surface, all
# of it in `search`'s description, the sentence 'Grep lists one line per
# record: id, title, path, match count, first matching line.' Full read and
# reader 4,097, discovery 2,774, write/admin 5,288, inspect+search 2,412. No
# ceiling moves.
# Then run-composed-3666 (lane RB's `run`, ported to exp by lane ri),
# measured on the lab: the `run` tool (one line, 106 tokens with its schema
# of steps, generation_id and max_bytes), its name in the served tool list
# and the instructions' run sentence, +147 on the full read, reader and
# write/admin surfaces, as on RB's branch. Full read and reader 4,326,
# write/admin 5,517. Surfaces that do not serve `run` (discovery,
# inspect+search, okg-workspace's declared `served`) do not move. No
# ceiling moves.
FULL_READ_SURFACE_BASELINE_TOKENS = 4_326
DISCOVERY_SURFACE_BASELINE_TOKENS = 2_856
READER_SURFACE_BASELINE_TOKENS = 4_326
WRITE_ADMIN_SURFACE_BASELINE_TOKENS = 5_517
TWO_TOOL_SURFACE_BASELINE_TOKENS = 2_448

#: okg-workspace, the deployment that declares an `agent_surface` block.
#: Measured because req.esd.workspace asks for an exact number rather than a
#: relation: "the server and the chat projection agree" is close to true by
#: construction once both derive from one list, and would pass without
#: proving the surface actually shrank.
#: #3059: 21,337 -> 17,893 read, 28,104 -> 23,567 write (as above).
#: search-result-shape: 17,893 -> 18,070 read, 23,567 -> 23,744 write.
#: #3067: 18,070 -> 18,268 read, 23,744 -> 23,942 write (as above).
#: read-model-s6-page-next: 18,382 read, 24,056 write (as above).

#: mcp-grep-3067: 18,268 -> 18,523 read, 23,942 -> 24,197 write (+255 B).
#: mcp-grep-rank: 18,523 -> 18,581 read, 24,197 -> 24,255 write (+58 B).
#: mcp-grep-page: 18,581 -> 18,673 read, 24,255 -> 24,347 write (+92 B),
#: then 18,712 read, 24,386 write (+39 B, `continuation_token`).
#: #3151 slice 1 (`full` fixture, `select` items enum): 18,268 -> 18,711
#: read, 23,942 -> 24,385 write.
#: #3151 slice 1 (`full` fixture, `select` items enum): 18,268 -> 18,711
#: read, 23,942 -> 24,385 write.
#: #3209 (`where`/`scope`, as above, +531 B): 19,242 read, 24,916 write;
#: typed node-lookup instruction (+103 B): 19,345 read, 25,019 write.
#: #3214 (`handle` in the `select` enum, +9 B): 19,354 read, 25,028 write.
#: read-model-s6-page-next: 18,382 read, 24,056 write (as above).
#: Combined with #3214 and dev (+420 B on each): 19,774 read, 25,448 write.
#: lexical-index-scan-cap-3162 (+195 B, `search`'s `completeness` and
#: description) on top of the combined pins.
#: Merged grep stack + chain through #3219 (as above): 18,711 + 1,063 + 649
#: + 195 = 20,618 read; 24,385 + 1,063 + 649 + 195 = 26,292 write.
#: read-record-windows (+363 B on `search`): 20,981, 26,655 (read, write).
#: read-text-batch (+753 B on `inspect`): 21,734, 27,408 (read, write).
#: #3225 (`search.view`, list description, +182 B alone: 19,536 read,
#: 25,210 write over #3214); merged with the chain, +149 B (as above):
#: 21,883 read, 27,557 write. Measured on lab-host at c1f8829e41.
#: #3222 slice 2 (+803 B): 22,686 read, 28,360 write. Slice 3 (+226 B on
#: `search`, as above): 22,912 read, 28,586 write.
#: Plus lexical-quoted-phrases-3064's phrase_match (+15 B) from train 46c.
#: Token re-base (surface-token-budget): okg-workspace serves the full read
#: and write/admin surfaces, 5,601 and 6,773 tokens (23,255 and 28,651 B).
#: select-fields-by-method (-2 tokens, as above): 5,598 read, 6,770 write.
#: dataset-card instructions (-51): 5,547 read, 6,719 write.
#: dataset-card tool prose (-77 read, -49 write, as above): 5,470, 6,670.
#: read-query-guide-3666 (+55, then +18, on both, as above): 5,713 read,
#: 6,913 write; then +9 (the regex word boundary): 5,722 read, 6,922 write.
#: read-pages-3b-3666 (-1 on both, as above): 5,639 read, 6,839 write.
#: read-expand-records-3666 (-3 on both, as above): 5,636 read, 6,836 write.
#: Later changes moved these with the full read and write/admin pins.
#: read-surface-diet-3666 (#3666 M10), from 5,640 and 6,840 on dev
#: 02af8953283: 3,427 read, 4,618 write (as the full read and write pins).
#: Round 2 (`search.select` text, -14): 3,413 read, 4,604 write.
#: Round 3 (`search.select` field list, no enum, +1): 3,414 read, 4,605 write.
#: read-v2 base (M10, M7, expand fixes on dev; as the full read and
#: write/admin pins, 7 tools served): 3,562 read, 4,753 write; then -13
#: (expand drops target_subtype): 3,549 read, 4,740 write.
#: Merged (lane M2, 2026-10-14): +82 and -4 on both: 5,718 read, 6,918 write.
#: read-v2 with 2c and M12 (as the full read and write/admin pins): 3,618
#: read, 4,809 write.
#: Then the retain sentence (-1): 3,617 read, 4,808 write.
#: Then the `surface` sentence (-12): 3,605 read, 4,796 write.
#: loose-match-3666 (+27, as the full read pins): 3,632 read, 4,823 write.
#: publish-paging-args-3666 (+263, as the full read pins): 3,895 read,
#: 5,086 write.
#: read-passage-rows-3666 merged (segment B: -22, +29 view, +29 ticket
#: reading, -14 prompt redesign; +22 as the full read pins): 3,917 read,
#: 5,108 write.
#: read-defects-rd-3666 merged (Format v7: inspect's `lines` sentence +4,
#: filter's `contains` text form +11, search's contains sentence in place
#: of "filter's like matches substrings" +7; +22 as the full read pins,
#: measured on the lab): 3,939 read, 5,130 write.
#: Then Format v10 item C (+30, as the full read pins): 3,969 read, 5,160 write.
#: Then read-reply-size-3666 (+29, search's read-narrow line, as the full
#: read pins): 3,998 read, 5,189 write.
#: Then read-search-caps-3666 (+46, search's within="record" sentence and
#: `within` enum, as the full read pins): 4,044 read, 5,235 write.
#: Then read-aggregate-variants-3666 (+32, aggregate's merged-key clause,
#: as the full read pins): 4,076 read, 5,267 write.
#: Then read-names-list-3666 (+21, search's grep names-list sentence, as the
#: full read pins): 4,097 read, 5,288 write.
WORKSPACE_READ_SURFACE_BASELINE_TOKENS = 4_179
WORKSPACE_WRITE_SURFACE_BASELINE_TOKENS = 5_370
#: read-query-guide-3666, query-by-default (Jason, via orchestrator chat
#: 2026-10-13): okg-workspace's manifest sets access rules to `enforce`, so
#: its launches withhold `query` (6 read tools, 17 write). The two pins
#: above are the same launches with the rules off (7 and 18 tools).
#: Measured hermetically on the lab host: 5,458 read, 6,658 write, each
#: 255 tokens under its rules-off pin (the `query` tool; the instructions
#: are unchanged). Merged with 3b and expand (-4 on both): 5,454 read,
#: 6,654 write.
#: read-v2 (M10 lean surface with M7, the expand fixes and 2c), measured
#: on the lab under enforce (6 and 17 tools): 3,354 read, 4,545 write,
#: 264 under the rules-off pins.
#: Then the retain sentence (-1): 3,353 read, 4,544 write.
#: Then the `surface` sentence (-12): 3,341 read, 4,532 write.
#: loose-match-3666 (+27, search and filter served): 3,368 read, 4,559 write.
#: publish-paging-args-3666 (+263): 3,631 read, 4,822 write.
#: read-passage-rows-3666 merged (segment B, +22 as the full read pins):
#: 3,653 read, 4,844 write.
#: read-defects-rd-3666 merged (Format v7: +4 `lines`, +11 filter's
#: `contains` text form, +7 search's contains sentence, search and filter
#: served; +22 as the full read pins): 3,675 read, 4,866 write.
#: Then Format v10 item C (+30): 3,705 read, 4,896 write.
#: Then read-reply-size-3666 (+29, search's read-narrow line): 3,734 read,
#: 4,925 write.
#: Then read-search-caps-3666 (+46, search's within="record" sentence and
#: `within` enum): 3,780 read, 4,971 write.
#: Then read-aggregate-variants-3666 (+32, aggregate's merged-key clause):
#: 3,812 read, 5,003 write.
#: Then read-names-list-3666 (+21, search's grep names-list sentence):
#: 3,833 read, 5,024 write.
WORKSPACE_ENFORCE_READ_SURFACE_BASELINE_TOKENS = 3_915
WORKSPACE_ENFORCE_WRITE_SURFACE_BASELINE_TOKENS = 5_106

#: The opensearch-reference deployment (PACT `opensearch-reference-integration`,
#: issue #2014, req.ori.budget). Its `agent_surface.served` names five tools;
#: a READ launch registers the three read operators among them and
#: `--allow-writes` registers all five.
#:
#: Pinned HERE, hermetically, and not only in that change's live walk. The
#: walk is marked `live_opensearch` and every lane deselects that marker, so
#: a pin that lived only there was a pin no automated run ever checked --
#: which is the whole failure mode `req.ori.budget` exists to catch.
#:
#: #2156 moved both by 3 B, one per read operator served (11,999 -> 12,002
#: read, 13,781 -> 13,784 write), for the same `max_bytes` schema change as
#: the surfaces above.
#: #2792 moved both: 13,142 -> 13,344 read, 14,924 -> 14,974 write.
#: Merged with #2668 (13,322 read, 15,104 write alone), measured: 13,491
#: read, 15,121 write -- both deltas minus the 33 B `text` pointer.
#: #2793 moved both by 71 B (`inspect`'s `{name, type}` columns):
#: 13,562 read, 15,192 write.
#: #3059 (no output schemas; contract and result-text line): 13,562 ->
#: 11,755 read, 15,192 -> 13,198 write.
#: search-result-shape moved both by 177 B (`search`'s `rank_by` and
#: description): 11,932 read, 13,375 write.
#: #3067 moved both by 23 B (`search`'s description; `filter` is not
#: served there): 11,955 read, 13,398 write.
#: read-model-s6-page-next moved both by 114 B (`next` on `inspect` and
#: `expand`, as above): 12,069 read, 13,512 write.

#: mcp-grep-3067 moved both by 255 B (`search`'s grep method, `path_prefix`
#: and description): 12,210 read, 13,653 write.
#: mcp-grep-rank moved both by 58 B (`search`'s description): 12,268 read,
#: 13,711 write.
#: mcp-grep-page moved both by 92 B (`search`'s description): 12,360 read,
#: 13,803 write; then by 39 B (`continuation_token`): 12,399 read, 13,842
#: write.
#: #3151 slice 1 (`full` fixture, `select` items enum, +443 B): 12,398
#: read, 13,841 write.
#: #3209 (`where`/`scope` on `search`; `filter` is not served there,
#: +495 B): 12,893 read, 14,336 write; typed node-lookup instruction
#: (+103 B): 12,996 read, 14,439 write.
#: #3214 (`handle` in the `select` enum, +9 B): 13,005 read, 14,448 write.
#: read-model-s6-page-next moved both by 114 B (`next` on `inspect` and
#: `expand`, as above): 12,069 read, 13,512 write.
#: Combined with #3214 and dev (+420 B on each): 13,425 read, 14,868 write.
#: lexical-index-scan-cap-3162 (+195 B, `search`'s `completeness` and
#: description) on top of the combined pins.
#: Merged grep stack + chain through #3219: dev 12,398 read, 13,841 write,
#: + chain 1,027 B (#3209 495 + 103, #3214 9, #3217 next 114 + repair 306)
#: + grep stack 649 B + #3219 195 B: 14,269 read, 15,712 write.
#: read-record-windows (+363 B on `search`): 14,632, 16,075 (read, write).
#: read-text-batch (+753 B on `inspect`): 15,385, 16,828 (read, write).
#: #3225 (`search.view`, list description, +182 B alone: 13,187 read,
#: 14,630 write over #3214); merged with the chain, +149 B (as above):
#: 15,534 read, 16,977 write. Measured on lab-host at c1f8829e41.
#: #3222 slice 2 (+222 B on `search`): 15,756 read, 17,199 write. Slice 3
#: (+226 B on `search`, as above): 15,982 read, 17,425 write.
#: Plus lexical-quoted-phrases-3064's phrase_match (+15 B) from train 46c.
#: Token re-base (surface-token-budget): 3,946 read, 4,254 write tokens
#: (16,422 and 17,823 B), measured after the dedent.
#: select-fields-by-method (-2 tokens on `search`, as above): 3,943 read,
#: 4,251 write.
#: dataset-card instructions (-51): 3,892 read, 4,200 write.
#: dataset-card tool prose: read -46 (`inspect` loses the `surface` and
#: `query` clauses, `search` its unserved tool names; this deployment
#: declares a live relation, so `expand` keeps `live=`), write -19
#: (`surface` is served there): 3,846 read, 4,181 write.
#: read-query-guide-3666 (`query` is not served here; the shorter
#: node-lookup instructions, -30): 3,972 read, 4,307 write.
#: read-pages-3b-3666 (`inspect` -5; `filter` and `map` not served): 3,997
#: read, 4,332 write.
#: read-expand-records-3666 (`expand` and the reading sentence, -3): 3,994
#: read, 4,329 write.
#: read-surface-diet-3666 (#3666 M10), from 4,002 and 4,337 on dev
#: 02af8953283: 2,579 read, 2,905 write. Three tools (inspect, search,
#: expand); `expand` keeps `live` (this deployment declares a live
#: relation), and the write launch publishes its write-side inspect kinds.
#: Round 2 (`search.select` text, -14): 2,565 read, 2,891 write.
#: Round 3 (`search.select` field list, no enum, +1): 2,566 read, 2,892 write.
#: read-v2 base (M10, M7, expand fixes on dev; inspect and expand as
#: above, +80): 2,646 read, 2,972 write; then -13 (expand drops
#: target_subtype): 2,633 read, 2,959 write.
#: Merged (lane M2, 2026-10-14): -30 and -8 on both: 3,964 read, 4,299 write.
#: read-v2 with 2c and M12 (-31 on both, as discovery): 2,602 read, 2,928
#: write.
#: Then the retain sentence (+4, no reader served): 2,606 read, 2,932 write.
#: Then the `surface` sentence (-12): 2,594 read, 2,920 write.
#: loose-match-3666 (+12, search served, no filter): 2,606 read, 2,932 write.
#: publish-paging-args-3666 (+163: search, inspect and expand served; no
#: map or aggregate): 2,769 read, 3,095 write.
#: read-passage-rows-3666 merged (segment B: -22, +29 view, +29 ticket
#: reading, -10 prompt redesign with `filter` not served; +26, as
#: discovery): 2,795 read, 3,121 write.
#: read-defects-rd-3666 merged (Format v7: +4 `lines`, +9 `contains` with
#: text, search only; as discovery): 2,808 read, 3,134 write.
#: Then read-reply-size-3666 (+29, search's read-narrow line): 2,837 read,
#: 3,163 write.
#: Then read-search-caps-3666 (+46, search's within="record" sentence and
#: `within` enum): 2,883 read, 3,209 write.
#: Then read-names-list-3666 (+21, search's grep names-list sentence):
#: 2,904 read, 3,230 write.
REFERENCE_READ_SURFACE_BASELINE_TOKENS = 3_021
REFERENCE_WRITE_SURFACE_BASELINE_TOKENS = 3_347

#: The `live-http-ci` test fixture (PACT `laptop-monitor-live-example`, issue
#: #2164, req.lmle.budget). Its `agent_surface.served` names four tools; a
#: READ launch registers three of them and `--allow-writes` registers
#: `list_sources` as well.
#:
#: A FIXTURE, not a shipped deployment: it lives under
#: `tests/substrate/e2e/fixtures/` and exists so hosted CI can drive the
#: live-relation path against a real third-party HTTP service. Its surface is
#: pinned all the same, and pinned HERE rather than in that walk, because
#: this file runs in the fast lane on every pull request while the walk needs
#: a database. A declared `agent_surface` that quietly grew would otherwise
#: be caught only where it is least often looked at.
#: #2792 moved both: 13,142 -> 13,344 read, 14,173 -> 14,299 write.
#: Merged with #2668 (13,322 read, 14,353 write alone), measured: 13,491
#: read, 14,446 write -- both deltas minus the 33 B `text` pointer.
#: #2793 moved both by 71 B (`inspect`'s `{name, type}` columns):
#: 13,562 read, 14,517 write.
#: #3059 (no output schemas; contract and result-text line): 13,562 ->
#: 11,755 read, 14,517 -> 12,611 write.
#: search-result-shape moved both by 177 B (`search`'s `rank_by` and
#: description): 11,932 read, 12,788 write.
#: #3067 moved both by 23 B (`search`'s description): 11,955 read,
#: 12,811 write.
#: read-model-s6-page-next moved both by 114 B (`next` on `inspect` and
#: `expand`, as above): 12,069 read, 12,925 write.

#: mcp-grep-3067 moved both by 255 B: 12,210 read, 13,066 write.
#: mcp-grep-rank moved both by 58 B: 12,268 read, 13,124 write.
#: mcp-grep-page moved both by 92 B: 12,360 read, 13,216 write; then by
#: 39 B (`continuation_token`): 12,399 read, 13,255 write.
#: #3151 slice 1 (`full` fixture, `select` items enum, +443 B): 12,398
#: read, 13,254 write.
#: #3209 (`where`/`scope` on `search`, +495 B): 12,893 read, 13,749 write;
#: typed node-lookup instruction (+103 B): 12,996 read, 13,852 write.
#: #3214 (`handle` in the `select` enum, +9 B): 13,005 read, 13,861 write.
#: read-model-s6-page-next moved both by 114 B (`next` on `inspect` and
#: `expand`, as above): 12,069 read, 12,925 write.
#: Combined with #3214 and dev (+420 B on each): 13,425 read, 14,281 write.
#: lexical-index-scan-cap-3162 (+195 B, `search`'s `completeness` and
#: description) on top of the combined pins.
#: Merged grep stack + chain through #3219: dev 12,398 read, 13,254 write,
#: + chain 1,027 B + grep stack 649 B + #3219 195 B: 14,269 read, 15,125
#: write.
#: read-record-windows (+363 B on `search`): 14,632, 15,488 (read, write).
#: read-text-batch (+753 B on `inspect`): 15,385, 16,241 (read, write).
#: #3225 (`search.view`, list description, +182 B alone: 13,187 read,
#: 14,043 write over #3214); merged with the chain, +149 B (as above):
#: 15,534 read, 16,390 write. Measured on lab-host at c1f8829e41.
#: #3222 slice 2 (+222 B on `search`): 15,756 read, 16,612 write. Slice 3
#: (+226 B on `search`, as above): 15,982 read, 16,838 write.
#: Plus lexical-quoted-phrases-3064's phrase_match (+15 B) from train 46c.
#: Token re-base (surface-token-budget): 3,946 read, 4,129 write tokens
#: (16,422 and 17,236 B), measured after the dedent.
#: select-fields-by-method (-2 tokens on `search`, as above): 3,943 read,
#: 4,126 write.
#: dataset-card instructions (-51): 3,892 read, 4,075 write.
#: dataset-card tool prose (live relation declared; as opensearch-reference):
#: -46 read, -19 write: 3,846 read, 4,056 write.
#: read-query-guide-3666 (as opensearch-reference, -30): 3,972 read,
#: 4,182 write.
#: read-pages-3b-3666 (`inspect` -5, as opensearch-reference): 3,997 read,
#: 4,207 write.
#: read-expand-records-3666 (-3, as opensearch-reference): 3,994 read,
#: 4,204 write.
#: read-surface-diet-3666 (#3666 M10), from 4,002 and 4,212 on dev
#: 02af8953283: 2,579 read, 2,773 write (as opensearch-reference; the write
#: launch serves no `record`, so no record kinds).
#: Round 2 (`search.select` text, -14): 2,565 read, 2,759 write.
#: Round 3 (`search.select` field list, no enum, +1): 2,566 read, 2,760 write.
#: read-v2 base (as opensearch-reference, +80): 2,646 read, 2,840 write;
#: then -13 (expand drops target_subtype): 2,633 read, 2,827 write.
#: Merged (lane M2, 2026-10-14): -30 and -8 on both: 3,964 read, 4,174 write.
#: read-v2 with 2c and M12 (-31 on both): 2,602 read, 2,796 write.
#: Then the retain sentence (+4): 2,606 read, 2,800 write.
#: Then the `surface` sentence (-12): 2,594 read, 2,788 write.
#: loose-match-3666 (+12, as opensearch-reference): 2,606 read, 2,800 write.
#: publish-paging-args-3666 (+163, as opensearch-reference): 2,769 read,
#: 2,963 write.
#: read-passage-rows-3666 merged (segment B, +26 as opensearch-reference):
#: 2,795 read, 2,989 write.
#: read-defects-rd-3666 merged (Format v7: +4 `lines`, +9 `contains` with
#: text, search only; as opensearch-reference): 2,808 read, 3,002 write.
#: Then read-reply-size-3666 (+29, search's read-narrow line): 2,837 read,
#: 3,031 write.
#: Then read-search-caps-3666 (+46, search's within="record" sentence and
#: `within` enum): 2,883 read, 3,077 write.
#: Then read-names-list-3666 (+21, search's grep names-list sentence):
#: 2,904 read, 3,098 write.
LIVE_HTTP_CI_READ_SURFACE_BASELINE_TOKENS = 3_021
LIVE_HTTP_CI_WRITE_SURFACE_BASELINE_TOKENS = 3_215

#: Where that fixture's manifest lives. `measure_surface` resolves a
#: deployment by NAME against the deployments root, and the directory name
#: has to match the manifest's `name` or the lookup silently falls back to
#: the default surface -- 7 tools (19,174 bytes when this was written), which passes no
#: assertion here but would if the numbers were ever copied from a run that
#: had not noticed.
LIVE_HTTP_CI_ROOT = (
    Path(__file__).resolve().parents[2] / "substrate/e2e/fixtures"
)

#: The ceiling the sprint set for the read surface once output schemas were
#: added. The baseline may not exceed it.
#: The eleven write/admin tools, in bytes. Pinned as a DIFFERENCE rather than
#: as a frozen write/admin total: a read parameter on a shared operator moves
#: both surfaces equally and leaves this untouched, so it fails on exactly
#: the asymmetry #2012's frozen number was watching for, without needing to
#: be re-derived every time the read surface legitimately grows. It held at
#: 25,554 - 18,663 before #2013, 26,058 - 19,167 after it, and
#: 26,065 - 19,174 after #2156, 27,205 - 20,314 after #2676, and
#: 27,385 - 20,494 after #2668.
#: #2792 moved it to 27,886 - 21,119 = 6,767 (-124 B), and for a reason the
#: control is meant to surface: inlining changed the write surface's OWN
#: schemas. The write launch publishes all seventeen `inspect` kinds, whose
#: pointer overhead was larger (-291 B there against -111 B on read), and
#: `record` and `propose` each gained a typed `generation_id` (+28 B each).
#: Merged with #2668 it holds at 28,033 - 21,266 = 6,767: both surfaces
#: serve the `text` kind, so its bytes and its dropped pointer cancel.
#: #3059 moved it to 23,567 - 17,893 = 5,674 (-1,093 B): dropping the output
#: schemas saved 4,962 B on the write surface and 3,869 B on the read one,
#: because the write/admin tools published schemas of their own too. The
#: instructions moved both by the same +425 B.
#: tool-description-dedent moved it to 28,651 - 23,255 = 5,396 (-278 B): the
#: write/admin tools' own descriptions lose their source indentation too.
#: Token re-base (surface-token-budget): 6,773 - 5,601 = 1,172 tokens. On
#: dev 446c3b1b5f1, before the dedent, it was 6,896 - 5,697 = 1,199.
#: dataset-card tool prose moved it to 6,670 - 5,470 = 1,200 (+28), as the
#: control should show: `inspect`'s `surface` sentence (27 tokens) was then
#: served only where the `surface` kind was, the write launch, and
#: `add_source`'s setup sentence gained a period (+1).
#: read-surface-diet-3666 (#3666 M10): `surface` is now served on every
#: launch (read-only without a ceiling), so its sentence is on both; the
#: difference is the write-side inspect kinds the write launch publishes and
#: the write/admin tools: 4,618 - 3,427 = 1,191.
WRITE_MINUS_READ_TOKENS = 1_191

#: The read surface's hard ceiling, in tokens (surface-token-budget, Jason
#: 2026-10-07 `tokenbudget A`). It replaces the 24,000 B ceiling. Set to the
#: full read surface measured on this branch, dedent included (5,601
#: tokens), plus a stated margin of 150 tokens.
SURFACE_CEILING_MARGIN_TOKENS = 150
READ_SURFACE_CEILING_TOKENS = 5_601 + SURFACE_CEILING_MARGIN_TOKENS

#: The write/admin surface's hard ceiling: 6,773 tokens measured on this
#: branch plus the same 150-token margin. The byte budget never had one.
WRITE_ADMIN_SURFACE_CEILING_TOKENS = 6_773 + SURFACE_CEILING_MARGIN_TOKENS

#: Raised from 2,600 B by #2676, which added the reading paragraph (how
#: to read a whole document, `select`, excerpts, when to stop) and
#: corrected the path sentence: the full read surface's instructions went
#: from 2,353 B to 3,493 B. #2668 replaced the whole-document sentence with
#: the `inspect` kind "text" read and the `file:`-to-`doc:` step: 3,355 B.
#: #3059 raised it to 4,000 B: the untrusted-data contract now says how the
#: text form tags an untrusted row (597 -> 834 B) and one line explains the
#: escape and typed values (188 B), so the read instructions are 3,780 B.
#: A ceiling 20 B above that would red the next one-sentence edit; 4,000 B
#: leaves room for one while still catching a paragraph.
#: Token re-base (surface-token-budget): okg's own (authored) instructions
#: are 965 tokens (3,835 B) on the full read surface under every capability
#: fixture; the ceiling is that plus the same 150-token margin.
INSTRUCTIONS_CEILING_TOKENS = 965 + SURFACE_CEILING_MARGIN_TOKENS

#: The generated "not available here" block (#3151, decision 12), gated in
#: tokens here. The largest one these fixtures produce is the unprobed
#: hermetic build's, 87 tokens; the ceiling adds the same 150-token margin.
#: The server's own 2,000 B refusal (`GENERATED_INSTRUCTIONS_BUDGET_BYTES`)
#: is a runtime guard in `capability_probe` and is not changed here.
GENERATED_INSTRUCTIONS_CEILING_TOKENS = 87 + SURFACE_CEILING_MARGIN_TOKENS


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"


@pytest.fixture(autouse=True)
def _hermetic_probe(monkeypatch: pytest.MonkeyPatch) -> None:
    """Probe mode on, every ambient narrowing off.

    An inherited OKG_MCP_TOOL_ALLOWLIST from the developer's shell would
    make the budget assertions measure a surface nobody asked for and pass
    vacuously, so they are cleared rather than assumed absent.
    """
    monkeypatch.setenv("OKG_MCP_SURFACE_PROBE", "1")
    monkeypatch.setenv("OKG_MCP_TRACE_ENABLED", "0")
    # The capability profile every exact pin in this file is measured under
    # (#3151 slice 1): `full`, so a pin moves only when the code moves. The
    # `none` profile is pinned explicitly in its own section.
    monkeypatch.setenv("OKG_MCP_CAPABILITY_FIXTURE", "full")
    monkeypatch.delenv("OKG_MCP_TOOL_ALLOWLIST", raising=False)
    monkeypatch.delenv("OKG_MCP_ALLOW_WRITES", raising=False)
    # The access-rule mode decides whether `query` is served (issue #3666),
    # so it is read from the real manifests: no ambient override, and no
    # deployment directory memoized by an earlier test in this process (one
    # builds an `okg-workspace` from a temporary directory).
    from okg.substrate.nomos.runtime_enforcement import (
        _clear_nomos_enforcement_mode_cache,
    )

    monkeypatch.delenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", raising=False)
    _clear_nomos_enforcement_mode_cache()


# ── the budget ────────────────────────────────────────────────────────

@pytest.mark.parametrize("label,kwargs,expected", [
    ("full read", {}, FULL_READ_SURFACE_BASELINE_TOKENS),
    ("discovery", {"tool_profile": "discovery"}, DISCOVERY_SURFACE_BASELINE_TOKENS),
    ("reader", {"tool_profile": "reader"}, READER_SURFACE_BASELINE_TOKENS),
    ("write/admin", {"allow_writes": True}, WRITE_ADMIN_SURFACE_BASELINE_TOKENS),
])
async def test_each_surface_matches_its_exact_baseline(
    label: str, kwargs: dict, expected: int,
) -> None:
    """Every launch configuration's startup cost is pinned to the token."""
    measured = await measure_surface(**kwargs)
    assert measured.total_tokens == expected, (
        f"the {label} MCP surface now serves {measured.total_tokens} tokens, "
        f"not the recorded {expected}. Every session pays this before asking "
        f"anything. If the change is intended, put the new number in this "
        f"file and in the review; if it is not, this is the schema or "
        f"docstring edit that caused it.\n{measured.breakdown()}"
    )


async def test_the_two_tool_surface_matches_its_exact_baseline(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Pin the explicit allowlist profile promised by req.ooc.baselines."""
    monkeypatch.setenv("OKG_MCP_TOOL_ALLOWLIST", "inspect,search")
    measured = await measure_surface()
    assert measured.tool_names == ("inspect", "search")
    assert measured.total_tokens == TWO_TOOL_SURFACE_BASELINE_TOKENS, (
        f"the inspect+search MCP surface now serves {measured.total_tokens} "
        f"tokens, not the recorded {TWO_TOOL_SURFACE_BASELINE_TOKENS}. Every session "
        "pays this before asking anything. If the change is intended, put "
        "the new number in this file and in the review; otherwise inspect "
        f"the measured breakdown.\n{measured.breakdown()}"
    )


# ── the token counter (PACT surface-token-budget) ─────────────────────

def test_tokens_are_counted_with_cl100k_from_the_local_ranks(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The count is cl100k_base, built once per process, with no network:
    a download path that is reached at all fails this test."""
    import tiktoken
    import tiktoken.load

    from okg.substrate.mcp import surface_budget

    def _no_network(*_args, **_kwargs):
        raise AssertionError("the surface token count reached the network path")

    monkeypatch.setattr(tiktoken, "get_encoding", _no_network)
    monkeypatch.setattr(tiktoken.load, "read_file", _no_network)
    monkeypatch.setattr(tiktoken.load, "read_file_cached", _no_network)
    monkeypatch.setattr(surface_budget, "_ENCODING", None)

    encoding = surface_budget.cl100k_encoding()
    assert encoding.name == "cl100k_base"
    assert surface_budget.cl100k_encoding() is encoding
    # Known cl100k_base encodings, independent of this module.
    assert encoding.encode("hello world") == [15339, 1917]
    assert count_tokens("hello world") == 2
    # A special-token spelling is counted as the text a client receives.
    assert count_tokens("<|endoftext|>") > 1


@pytest.mark.parametrize("attribute,value,match", [
    ("CL100K_RANKS_FILE", "no-such-ranks-file", "missing"),
    ("CL100K_RANKS_SHA256", "0" * 64, "sha256"),
])
def test_the_token_count_refuses_missing_or_altered_ranks(
    attribute: str, value: str, match: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """No download and no bytes-per-token estimate: a named refusal."""
    from okg.substrate.mcp import surface_budget

    monkeypatch.setattr(surface_budget, "_ENCODING", None)
    monkeypatch.setattr(surface_budget, attribute, value)
    with pytest.raises(surface_budget.SurfaceTokenizerUnavailable, match=match):
        count_tokens("hello world")


def test_clean_descriptions_drop_only_the_source_indent() -> None:
    """tool-description-dedent: the common indent and trailing blank lines
    go; every word, the line breaks and relative indentation stay."""
    from types import SimpleNamespace

    from okg.substrate.mcp.operator_models import publish_clean_descriptions

    raw = (
        "Summary line.\n\n        Body `a`, word b.\n"
        "            indented example\n        last.\n        "
    )
    tool = SimpleNamespace(description=raw)
    mcp = SimpleNamespace(_tool_manager=SimpleNamespace(_tools={"t": tool}))
    removed = publish_clean_descriptions(mcp)
    assert tool.description == (
        "Summary line.\n\nBody `a`, word b.\n    indented example\nlast."
    )
    assert tool.description.split() == raw.split()
    assert removed == len(raw.encode()) - len(tool.description.encode())


def test_clean_descriptions_refuse_when_the_sdk_internals_moved() -> None:
    from types import SimpleNamespace

    from okg.substrate.mcp.operator_models import (
        ToolSchemaUnpublishable,
        publish_clean_descriptions,
    )

    with pytest.raises(ToolSchemaUnpublishable):
        publish_clean_descriptions(SimpleNamespace())


async def test_every_served_description_keeps_each_word_of_its_original(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Every tool the write/admin surface serves (the widest) publishes
    exactly `inspect.cleandoc` of the description it published before the
    pass: the same words in the same order, no source indent left."""
    import inspect as pyinspect

    from okg.substrate.mcp import server as server_mod
    from okg.substrate.mcp.surface_budget import PROBE_DSN

    def served() -> dict[str, str]:
        import asyncio

        mcp = server_mod.build_server(
            dsn=PROBE_DSN, deployment_name="default", allow_writes=True,
        )
        return {t.name: t.description or "" for t in asyncio.run(mcp.list_tools())}

    cleaned = await asyncio_to_thread(served)
    monkeypatch.setattr(server_mod, "publish_clean_descriptions", lambda mcp: 0)
    original = await asyncio_to_thread(served)
    assert set(cleaned) == set(original)
    for name, text in original.items():
        assert cleaned[name] == pyinspect.cleandoc(text), name
        assert cleaned[name].split() == text.split(), name


async def test_the_read_surface_baseline_stays_under_its_ceiling() -> None:
    """Non-vacuity partner: an exact assertion passes at any value once it is
    edited to match. This one says which values are acceptable at all."""
    assert FULL_READ_SURFACE_BASELINE_TOKENS <= READ_SURFACE_CEILING_TOKENS
    assert WRITE_ADMIN_SURFACE_BASELINE_TOKENS <= WRITE_ADMIN_SURFACE_CEILING_TOKENS


@pytest.mark.parametrize("label,kwargs,ceiling", [
    ("full read", {}, READ_SURFACE_CEILING_TOKENS),
    ("write/admin", {"allow_writes": True}, WRITE_ADMIN_SURFACE_CEILING_TOKENS),
])
async def test_each_surface_stays_under_its_token_ceiling(
    label: str, kwargs: dict, ceiling: int,
) -> None:
    """The hard bound, measured rather than compared constant to constant."""
    measured = await measure_surface(**kwargs)
    assert measured.total_tokens <= ceiling, (
        f"the {label} MCP surface serves {measured.total_tokens} tokens, past "
        f"its {ceiling}-token ceiling.\n{measured.breakdown()}"
    )


@pytest.mark.parametrize("fixture", ["full", "none", None])
async def test_the_instructions_stay_within_their_budget(
    fixture: str | None, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Decision 12 (b) of #3151: okg's own text keeps its own ceiling and the
    generated "not available here" block gets a separate budget, both in
    tokens since surface-token-budget. Measured under every capability
    profile, including the unprobed one a hermetic build serves when no
    fixture is named."""
    if fixture is None:
        monkeypatch.delenv("OKG_MCP_CAPABILITY_FIXTURE", raising=False)
    else:
        monkeypatch.setenv("OKG_MCP_CAPABILITY_FIXTURE", fixture)
    measured = await measure_surface()
    okg_text = measured.authored_instructions_tokens
    generated = measured.generated_instructions_tokens
    assert okg_text <= INSTRUCTIONS_CEILING_TOKENS, (
        f"okg's own instructions grew to {okg_text} tokens, past the pinned "
        f"{INSTRUCTIONS_CEILING_TOKENS}-token ceiling. They are sent on every "
        f"initialize."
    )
    assert generated <= GENERATED_INSTRUCTIONS_CEILING_TOKENS, (
        f"the generated capability lines are {generated} tokens, past their "
        f"{GENERATED_INSTRUCTIONS_CEILING_TOKENS}-token budget"
    )


async def test_the_discovery_profile_actually_costs_less() -> None:
    """Non-vacuity partner. A ceiling both surfaces satisfy would pass even
    if the profile stopped narrowing anything."""
    full = await measure_surface()
    discovery = await measure_surface(tool_profile="discovery")
    assert discovery.total_tokens < full.total_tokens * 0.75, (
        f"discovery saves only {full.total_tokens - discovery.total_tokens} "
        f"of {full.total_tokens} tokens; the profile is no longer worth naming.\n"
        f"{discovery.breakdown()}"
    )


async def test_the_measurement_refuses_without_the_hermetic_probe(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A measurement helper that silently enabled probe mode would let a
    caller believe it measured a normal server."""
    monkeypatch.delenv("OKG_MCP_SURFACE_PROBE", raising=False)
    with pytest.raises(RuntimeError, match="OKG_MCP_SURFACE_PROBE"):
        await measure_surface()


# ── the served set is what the profile says ───────────────────────────

async def test_the_discovery_profile_serves_exactly_three_tools() -> None:
    measured = await measure_surface(tool_profile="discovery")
    assert measured.tool_names == ("aggregate", "inspect", "search")


async def test_the_reader_profile_serves_the_whole_read_algebra() -> None:
    measured = await measure_surface(tool_profile="reader")
    assert set(measured.tool_names) == set(OPERATOR_TOOLS)


# ── instructions follow the surface ───────────────────────────────────

def _served_sets() -> list[tuple[str, tuple[str, ...]]]:
    """Every built-in profile, each single tool, and the WRITE surfaces.

    Written as a GENERAL sweep rather than a hardcoded string comparison: a
    test that pinned one profile's expected sentence would pass while a
    sibling profile shipped a sentence naming a tool it does not serve.

    The write cases exist because the first version of this sweep covered
    only read subsets, and a `--allow-writes` launch therefore shipped
    "serves exactly these tools: <the seven read tools>" while serving
    eighteen — a false sentence the read-only sweep could not see.
    """
    cases: list[tuple[str, tuple[str, ...]]] = [
        (name, tuple(tools)) for name, tools in sorted(BUILTIN_TOOL_PROFILES.items())
    ]
    cases.extend((f"only-{tool}", (tool,)) for tool in OPERATOR_TOOLS)
    cases.append(("read-surface", tuple(OPERATOR_TOOLS)))
    cases.append(
        ("write-admin", tuple(OPERATOR_TOOLS) + tuple(WRITE_ADMIN_TOOLS))
    )
    cases.append(
        (
            "write-admin-with-upload",
            tuple(OPERATOR_TOOLS) + tuple(WRITE_ADMIN_TOOLS) + ("upload",),
        )
    )
    cases.append(("narrowed-write", ("inspect", "search", "record")))
    return cases


@pytest.mark.parametrize("label,served", _served_sets(), ids=lambda v: v if isinstance(v, str) else "")
def test_no_rendered_instructions_name_an_unserved_tool(
    label: str, served: tuple[str, ...],
) -> None:
    rendered = operator_instructions_for(served)
    named = set(tools_named_in(rendered, candidates=OPERATOR_TOOLS))
    unserved = sorted(named - set(served))
    assert not unserved, (
        f"instructions rendered for profile {label!r} (serving "
        f"{sorted(served)}) name pruned tool(s) {unserved}. A narrowed "
        f"server must not tell a model to call something it just removed."
    )


@pytest.mark.parametrize("label,served", _served_sets(), ids=lambda v: v if isinstance(v, str) else "")
def test_the_rendered_instructions_enumerate_exactly_the_served_set(
    label: str, served: tuple[str, ...],
) -> None:
    from okg.substrate.mcp.tool_profiles import render_tool_list

    rendered = operator_instructions_for(served)
    assert f"serves exactly these tools: {render_tool_list(served)}." in rendered


@pytest.mark.parametrize("label,served", _served_sets(), ids=lambda v: v if isinstance(v, str) else "")
def test_the_trust_boundary_survives_every_narrowing(
    label: str, served: tuple[str, ...],
) -> None:
    """Not a convenience the narrowing may drop: a reader that loses it
    treats attacker-influenced node text as instructions."""
    from okg.substrate.mcp.provenance import UNTRUSTED_DATA_CONTRACT

    assert UNTRUSTED_DATA_CONTRACT in operator_instructions_for(served)


def test_the_unnarrowed_rendering_is_the_served_operator_constant() -> None:
    assert operator_instructions_for(OPERATOR_TOOLS) == OPERATOR_INSTRUCTIONS


async def test_a_write_server_enumerates_its_write_tools() -> None:
    """End-to-end guard for the write path.

    The rendering is driven by the FULL admitted set, not its read half. An
    earlier version intersected with the read operators, so a
    `--allow-writes` launch told the model it served exactly seven tools
    while serving eighteen.
    """
    measured = await measure_surface(allow_writes=True)
    served = set(measured.tool_names)
    assert {"record", "propose", "add_source"} <= served
    expected = operator_instructions_for(measured.tool_names)
    assert measured.instructions_tokens == count_tokens(
        _probe_card(measured.tool_names) + "\n\n" + expected,
    )
    for tool in ("record", "propose"):
        assert tool in expected


async def test_a_narrowed_server_serves_the_narrowed_instructions() -> None:
    """The end-to-end claim: not just that the renderer can drop a sentence,
    but that the SERVER serves the dropped version."""
    measured = await measure_surface(tool_profile="discovery")
    served = ("inspect", "search", "aggregate")
    expected = operator_instructions_for(served)
    assert measured.instructions_tokens == count_tokens(
        _probe_card(served) + "\n\n" + expected,
    )
    assert count_tokens(expected) < count_tokens(OPERATOR_INSTRUCTIONS)


def _probe_card(served: tuple[str, ...]) -> str:
    """The dataset card a server built without a database serves above
    okg's text (PACT dataset-card)."""
    from okg.substrate.mcp.dataset_card import (
        facts_without_database,
        render_dataset_card,
    )

    return render_dataset_card(
        facts_without_database(
            deployment_name="default",
            reason="this server was built without a database (OKG_MCP_SURFACE_PROBE)",
        ),
        served_tools=served,
    )


async def test_a_tool_that_fails_to_register_refuses_the_launch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Pins the DELIBERATE hardening in the build-time post-condition.

    `_okg_missing_admitted_tools` used to be a diagnostic readback only. It
    now refuses, because the instructions were already rendered naming that
    tool, and a server telling a model to call an unregistered tool is the
    exact failure this change exists to prevent. Asserted rather than left
    implicit, so the behavior change is visible to a reviewer.
    """
    from okg.substrate.mcp import server as server_mod
    from okg.substrate.mcp.server import build_server
    from okg.substrate.mcp.surface_budget import PROBE_DSN
    from okg.substrate.mcp.tool_profiles import InstructionSurfaceMismatch

    real_apply = server_mod.apply_admitted_tool_surface

    def _drop_one(mcp, **kwargs):
        real_apply(mcp, **kwargs)
        mcp._tool_manager._tools.pop("map", None)

    monkeypatch.setattr(
        server_mod,
        "_server_dsn_resolution",
        lambda _dsn, _name: {
            "dsn": PROBE_DSN, "authority": {"source": "surface_probe_stub"},
        },
    )
    monkeypatch.setattr(server_mod, "apply_admitted_tool_surface", _drop_one)

    with pytest.raises(InstructionSurfaceMismatch) as excinfo:
        build_server(dsn=PROBE_DSN)
    assert "map" in str(excinfo.value)
    assert "map" in excinfo.value.data["rendered_for"]
    assert "map" not in excinfo.value.data["served"]


# ── restart, not in-session expansion (issue #1458) ───────────────────

async def test_a_profiled_server_still_advertises_listchanged_false(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Profiles are STATIC with restart semantics. Issue #1458 records why
    in-session expansion is not available: the SDK advertises
    `tools.listChanged: false`, `ToolManager._tools` is process-wide across
    concurrent streamable-http sessions, and no client has been observed
    re-listing. Advertising `listChanged: true` here would promise dynamics
    this server does not have, so it must stay false under every profile.
    """
    from okg.substrate.mcp import server as server_mod
    from okg.substrate.mcp.server import build_server
    from okg.substrate.mcp.surface_budget import PROBE_DSN

    monkeypatch.setattr(
        server_mod,
        "_server_dsn_resolution",
        lambda _dsn, _name: {
            "dsn": PROBE_DSN, "authority": {"source": "surface_probe_stub"},
        },
    )
    mcp = build_server(dsn=PROBE_DSN, tool_profile="discovery")
    try:
        options = mcp._mcp_server.create_initialization_options()
        assert options.capabilities.tools is not None
        assert options.capabilities.tools.listChanged is False
        # The profile is disclosed, so an operator can see which surface a
        # running server was started with without re-reading argv.
        assert mcp._okg_tool_profile == "discovery"
        assert mcp._okg_tool_profile_source == "cli"
        assert mcp._okg_served_tools == ("aggregate", "inspect", "search")
        # And the caller can observe it too. A readback only reachable from
        # inside the process cannot make the restart contract actionable for
        # an agent that has to decide whether to ask for a different server.
        disclosed = mcp._okg_surface_freshness["tool_surface"]
        assert disclosed["profile"] == "discovery"
        assert disclosed["profile_source"] == "cli"
        assert disclosed["served_tools"] == ["inspect", "search", "aggregate"]
        assert disclosed["instruction_source"] == "rendered"
        assert disclosed["instructions_name_unserved_tools"] == []
        assert disclosed["profile_change_requires_restart"] is True
        assert disclosed["unhonored_manifest_default"] is False
    finally:
        mcp._okg_pool.close()


# ── the declared agent surface (PACT effective-surface-declaration) ────

@pytest.mark.parametrize(
    "label,deployment,allow_writes,expected,n_tools", [
        ("okg-workspace read, access rules off", "okg-workspace", False,
         WORKSPACE_READ_SURFACE_BASELINE_TOKENS, 7),
        ("okg-workspace write, access rules off", "okg-workspace", True,
         WORKSPACE_WRITE_SURFACE_BASELINE_TOKENS, 18),
        ("okg-workspace read", "okg-workspace", False,
         WORKSPACE_ENFORCE_READ_SURFACE_BASELINE_TOKENS, 6),
        ("okg-workspace write", "okg-workspace", True,
         WORKSPACE_ENFORCE_WRITE_SURFACE_BASELINE_TOKENS, 17),
        ("opensearch-reference read", "opensearch-reference", False,
         REFERENCE_READ_SURFACE_BASELINE_TOKENS, 3),
        ("opensearch-reference write", "opensearch-reference", True,
         REFERENCE_WRITE_SURFACE_BASELINE_TOKENS, 5),
    ],
)
async def test_a_declared_surface_matches_its_exact_baseline(
    label: str, deployment: str, allow_writes: bool, expected: int,
    n_tools: int, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """req.esd.workspace and req.ori.budget, pinned to the token.

    The tool COUNTS are asserted alongside the tokens because they are the
    operator-visible half of decision (a): `agent_surface.served` bounds
    every launch, a read launch registers the read operators among them, and
    `--allow-writes` registers the rest. For okg-workspace both numbers are
    exactly what it registered before the block existed, so no launch lost a
    tool.

    Every row is a DECLARED surface, hermetic, and under the ceiling
    asserted below. `opensearch-reference` is here because the change that
    added it proved its budget only in a `live_opensearch`-marked walk that
    no lane runs.

    okg-workspace is measured in both access-rule modes (issue #3666):
    its manifest's `enforce` withholds `query`; with the rules off it
    serves it.
    """
    if label.endswith("access rules off"):
        monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "off")
    measured = await measure_surface(
        deployment=deployment, allow_writes=allow_writes,
    )
    assert ("query" in measured.tool_names) is (n_tools in (7, 18)), label
    assert len(measured.tools) == n_tools, (
        f"the {label} surface registers {len(measured.tools)} tools, not "
        f"{n_tools}: {measured.tool_names}"
    )
    assert measured.total_tokens == expected, (
        f"the {label} surface now serves {measured.total_tokens} tokens, not "
        f"the recorded {expected}.\n{measured.breakdown()}"
    )
    if not allow_writes:
        # The charter's exit bound (#2008, amended 2026-09-20), in tokens
        # since surface-token-budget. Stated on the READ surface, which is
        # what a session pays before asking anything.
        assert measured.total_tokens <= READ_SURFACE_CEILING_TOKENS, (
            f"the {label} read surface is {measured.total_tokens} tokens, "
            f"past the {READ_SURFACE_CEILING_TOKENS}-token ceiling"
        )


@pytest.mark.parametrize("label,allow_writes,expected,n_tools", [
    ("live-http-ci read", False,
     LIVE_HTTP_CI_READ_SURFACE_BASELINE_TOKENS, 3),
    ("live-http-ci write", True,
     LIVE_HTTP_CI_WRITE_SURFACE_BASELINE_TOKENS, 4),
])
async def test_the_live_http_ci_surface_matches_its_exact_baseline(
    label: str, allow_writes: bool, expected: int, n_tools: int,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """req.lmle.budget (issue #2164), pinned to the token.

    The `live-http-ci` fixture serves four tools and no more: an agent
    searches for the host, inspects it to learn what it can be ASKED, and
    expands along a live relation. It records nothing, so `record` is not in
    `agent_surface.served` and no launch can add it.

    The tool COUNT is asserted beside the tokens because the two fail
    differently: a lost tool makes the surface smaller, which a token ceiling
    alone would read as an improvement. The count also catches the fallback
    described above -- an unresolved name returns the seven-tool default.
    """
    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(LIVE_HTTP_CI_ROOT))
    measured = await measure_surface(
        deployment="live-http-ci", allow_writes=allow_writes,
    )
    assert len(measured.tools) == n_tools, (
        f"the {label} surface registers {len(measured.tools)} tools, not "
        f"{n_tools}: {measured.tool_names}"
    )
    assert measured.total_tokens == expected, (
        f"the {label} surface now serves {measured.total_tokens} tokens, not "
        f"the recorded {expected}.\n{measured.breakdown()}"
    )
    if not allow_writes:
        # The charter's exit bound (#2008, amended 2026-09-20), in tokens
        # since surface-token-budget. Stated on the READ surface, which is
        # what a session pays before it has asked anything.
        assert measured.total_tokens <= READ_SURFACE_CEILING_TOKENS, (
            f"the {label} read surface is {measured.total_tokens} tokens, "
            f"past the {READ_SURFACE_CEILING_TOKENS}-token ceiling"
        )


async def test_the_declared_surface_registers_what_dev_registered() -> None:
    """No launch loses a tool (operator decision (a), 2026-09-20).

    `agent_surface.served` bounds every launch mode, so a tool missing from
    it is a tool no launch can ever serve. This asserts the two launches
    against the substrate's own tool tuples rather than against a copied
    list, so adding an operator cannot silently drop it from the deployment.
    """
    from okg.substrate.mcp.operators import OPERATOR_TOOLS, WRITE_ADMIN_TOOLS

    # okg-workspace runs its access rules (`nomos.runtime_enforcement:
    # enforce`), which take `query` off every surface (Jason, via
    # orchestrator chat 2026-10-13, query-by-default; issue #3666). Every
    # other tool stays.
    # `run` (run-composed-3666) is new, not lost: a deployment that declares
    # `served` offers it when it names it, and okg-workspace does not (its
    # access rules would refuse it anyway).
    from okg.substrate.mcp.operators import COMPOSITION_TOOLS

    read = await measure_surface(deployment="okg-workspace")
    assert set(read.tool_names) == set(OPERATOR_TOOLS) - {"query"} - set(COMPOSITION_TOOLS)

    write = await measure_surface(
        deployment="okg-workspace", allow_writes=True,
    )
    assert set(write.tool_names) == (
        set(OPERATOR_TOOLS) | set(WRITE_ADMIN_TOOLS)
    ) - {"query"} - set(COMPOSITION_TOOLS)


async def test_the_read_split_does_not_touch_the_write_surface() -> None:
    """The control, stated as a test and not only as a comment.

    A read-surface shrink and a write-surface loss look identical in a
    single number. This one says the write surface still serves every
    `inspect` kind.
    """
    from okg.substrate.mcp.effective_surface import (
        IMAGE_INSPECT_KINDS,
        served_inspect_kinds,
    )
    from okg.substrate.mcp.operator_models import INSPECT_TARGET_KINDS
    from okg.substrate.mcp.operators import OPERATOR_TOOLS, WRITE_ADMIN_TOOLS

    # Every kind but `image`, which needs a declared image source (#3046)
    # and is therefore absent from every launch these baselines measure.
    assert served_inspect_kinds(
        set(OPERATOR_TOOLS) | set(WRITE_ADMIN_TOOLS), allow_writes=True,
    ) == set(INSPECT_TARGET_KINDS) - IMAGE_INSPECT_KINDS
    # MEASURED, not a constant compared against a constant: the assertion
    # below used to read `WRITE_ADMIN_SURFACE_BASELINE_BYTES == 25_554`,
    # which is true of the file and says nothing about the server.
    #
    # #2013 moved the write total, legitimately: `expand` is served on both
    # surfaces and gained a `live` parameter there. What is asserted now is
    # the part of the control that #2012's frozen number was really carrying
    # -- that the write/admin TOOLS did not move. `WRITE_MINUS_READ_TOKENS` is
    # that quantity (in tokens since surface-token-budget), and unlike the
    # total it survives a read-side addition to a shared operator.
    read = await measure_surface()
    measured = await measure_surface(allow_writes=True)
    assert measured.total_tokens - read.total_tokens == WRITE_MINUS_READ_TOKENS, (
        f"the write/admin tools now cost "
        f"{measured.total_tokens - read.total_tokens} tokens beyond the read "
        f"surface, not the recorded {WRITE_MINUS_READ_TOKENS}. This is the "
        f"control for the read-surface narrowing and must not move.\n"
        f"{measured.breakdown()}"
    )
    assert measured.total_tokens == WRITE_ADMIN_SURFACE_BASELINE_TOKENS


async def test_the_read_surface_actually_shrank(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Non-vacuity partner for the re-pin.

    An exact assertion passes at any value once it is edited to match. This
    one says the direction the change was supposed to move in.

    Since the lean read surface (#3666, M10) both launches publish the same
    six `inspect` kinds, so the served-kind narrowing is measured with that
    pass off; the lean surface itself must be smaller still.
    """
    lean = await measure_surface()
    monkeypatch.setattr(
        "okg.substrate.mcp.lean_surface.publish_lean_read_surface", lambda _mcp: 0,
    )
    measured = await measure_surface()
    assert lean.tools_tokens < measured.tools_tokens
    # Compared against the WRITE surface's `inspect`, which serves every
    # kind, rather than against a frozen byte count. The frozen form
    # (`tools_bytes < 19,498 - 2,353`) stopped measuring the narrowing once
    # #2792 legitimately grew other read tools by inlining their predicates;
    # this form measures only `inspect`, and only the kinds it withholds.
    # It compares two schemas' sizes, not a budget, so bytes are enough.
    write = await measure_surface(allow_writes=True)
    read_inspect = next(t for t in measured.tools if t.name == "inspect")
    write_inspect = next(t for t in write.tools if t.name == "inspect")
    assert read_inspect.input_schema_bytes < write_inspect.input_schema_bytes, (
        "the read surface's inspect schema is no smaller than the write "
        "surface's; the kind narrowing is not reaching the published "
        f"schema.\n{measured.breakdown()}"
    )
    assert lean.total_tokens <= READ_SURFACE_CEILING_TOKENS


async def test_the_write_surface_exceeds_the_read_surface_by_the_admin_tools(
) -> None:
    """`write - read` is the invariant; #2013 re-pinned both totals."""
    read = await measure_surface()
    write = await measure_surface(allow_writes=True)
    assert write.total_tokens - read.total_tokens == WRITE_MINUS_READ_TOKENS, (
        f"the write surface is {write.total_tokens} tokens and the read "
        f"surface {read.total_tokens}, a difference of "
        f"{write.total_tokens - read.total_tokens} rather than the recorded "
        f"{WRITE_MINUS_READ_TOKENS}. The write/admin tools are what this "
        f"difference measures, so a change here means a read-side edit "
        f"reached the write surface only, or vice versa."
    )


async def test_read_and_write_moved_by_the_same_amount_in_this_change() -> None:
    """The equal-movement half of the control #2012's frozen number gave.

    #2013 added one parameter to `expand`, which both surfaces serve, one
    output field to each of `inspect` and `expand`, and one sentence to
    `expand`'s docstring. Both surfaces therefore had to move by the same
    504 B. Asserting the deltas against the #2012 baselines keeps that
    visible after the numbers above are re-pinned.

    #2156 then moved both by another 7 B: the seven read tools, served on
    both surfaces, each publish `max_bytes` as a plain optional integer.

    #2676 moved both by 1,140 B: the reading paragraph and the corrected
    node-lookup sentence, served to every surface that serves `search`
    and `expand`.

    #2668 moved both by 318 B: `inspect` gained the `text` target kind,
    which every surface serving `inspect` publishes; and by -246 B: the
    whole-document sentence now teaches that read instead of a path expand;
    and by 108 B: the `file:`-to-`doc:` sentence (issue #2669).

    #2793 moved both by 71 B: `inspect`'s output schema admits `{name,
    type}` column objects, and both surfaces serve `inspect`.

    search-result-shape moved both by 177 B: `search`'s `rank_by` gained
    `records` and its description explains the record default.

    #3067 moved both by 198 B: `filter`'s and `search`'s descriptions stop
    promising substring matches, and both surfaces serve both tools.

    mcp-grep-3067 moved both by 255 B: `search` gained `method="grep"`, the
    `path_prefix` parameter and one description sentence.

    mcp-grep-rank moved both by 58 B and mcp-grep-page by 92 B, all of it
    `search`'s description, and mcp-grep-page by 39 B more: `search`'s
    `continuation_token` parameter (decision 260a).
    The integrated read-model-s3-where-scope contract (where/scope in place
    of the legacy filters, typed dates) moved both by 540 B.
    read-model-s6-page-next moved both by 114 B: `inspect` and `expand`
    publish `next` (`target`/`input` stay required), `inspect`'s
    description gains its `next` sentence, and the whole-document
    instruction names `inspect(next=<token>)`.
    #3151 slice 1 moved both by 443 B under the `full` capability fixture:
    `search.select` publishes an items enum of its selectable fields.
    Merged with the grep stack, grep's own selectable fields join that
    enum: +27 B on both. Merged with #3214's final head, #3209's typed
    node-lookup instruction adds 103 B to both.

    Merged with the read-model chain through #3219,
    lexical-index-scan-cap-3162 moved both by 195 B more: `search` gained
    the `completeness` parameter and one description sentence.

    read-model-s4-list moved both by 182 B alone: `search` publishes
    `view`, its `limit` drops the published default, and the description
    names the list view. Merged with the chain, `view` already exists
    (`windows`) and its enum gains `cards` and `list`: +149 B on both.

    Result references moved both by 803 B in slice 2 (`retain` and the
    `{"ref"}` inputs) and by 226 B in slice 3 (`view="metadata"` and
    `summary` on `search`).
    lexical-quoted-phrases-3064 adds `phrase_match` to the `search.select`
    items enum: +15 B on both.

    tool-description-dedent moves them by different amounts, deliberately:
    every description loses its source indentation, -740 B on read and
    -1,018 B on write (the write/admin tools' own descriptions).

    Token re-base (surface-token-budget): the byte sums above ended at
    23,255 B read and 28,651 B write and are no longer asserted. The token
    control starts from dev 446c3b1b5f1, measured before the dedent: 5,697
    read and 6,896 write tokens. The dedent moves them by -96 and -123
    tokens. Add each later change's measured token delta to both sums.
    """
    read = await measure_surface()
    write = await measure_surface(allow_writes=True)
    # search-children-of-3363, merged after the dedent: -1 token on both.
    # select-fields-by-method: -2 tokens on both (`search`'s text).
    # dataset-card: -51 tokens on both (the served instructions).
    # dataset-card tool prose: -77 read, -49 write (descriptions rendered
    # for the served surface; the write launch keeps `surface`).
    # live-result-sets: +6 tokens on both (`retain`).
    # mcp-schema-no-top-level-anyof: -30 tokens on both (inspect -13, expand -17).
    # mcp-refusals-3470: -2 tokens on both (grep serves `handle`).
    # mcp-window-scope-3605: +12 on both (windows without a scope).
    # mcp-read-whole-3634: +55 on both (text page sizes, what is left, resize).
    # search-options-by-method (#3646): +3 on both (exact's methods, matching='record').
    # mcp-inspect-shape-3647 (call shape, partial node): +75 on both.
    # inspect-node-batch-3657: +119 on both (the `nodes` target).
    # aggregate-date-periods-3658: +14 on both (date periods in `by`).
    # grep-list-view-3659: +1 on both (a list is served on grep too).
    # read-text-lines-3666: -1 on both (`lines` added bare and named in the
    # description, +12; two redundant clauses cut from `inspect`, -13).
    # read-select-vocabulary-3666: -2 on both (`search` no longer says
    # windows take no select).
    # read-select-vocabulary-3666: -89 on both (the instructions no longer
    # say node ids are deterministic and can go straight to inspect).
    # read-inspect-start-3666: +9 on both (`inspect` names the graph
    # overview first and keeps the clause that `target.kind` picks the subject).
    # read-query-guide-3666: +55 on both (`query`'s guide +114, paid in part
    # by the instructions -51 and `inspect` -8), then +18 (three examples),
    # then +9 (a regex word boundary is \y, not \b).
    # read-pages-3b-3666: -1 on both (`inspect` path and source paid for by
    # its wording, -5; `next` on `filter` and `map`, +4).
    # read-text-matches-3666: +1 on both (`matches` in the operator enum of
    # four tools, and the filter and aggregate descriptions).
    # read-expand-records-3666: -1 on both (`expand`'s description, rewritten
    # for record rows, is one token shorter), then -2 (the reading sentence
    # says a one-hop expand returns the linked records, not edges).
    # read-surface-diet-3666 (M10): -2,213 read, -2,222 write. The six read
    # tools publish only their shown forms and shorter descriptions, and the
    # instructions teach one spelling (-60); `query` gains a pointer to the
    # `view` kind for a view's columns (+21). The write launch keeps
    # publishing its write-side inspect kinds, so WRITE_MINUS_READ_TOKENS
    # moves from 1,200 to 1,191.
    # read-surface-diet-3666 (round 2): -14 on both. `search.select` names
    # the fields holding `where`'s title, path and source instead of how
    # grep's rows differ.
    # read-surface-diet-3666 (round 3): +1 on both. `search.select` lists
    # the lexical fields and grep's difference again and drops its items
    # enum.
    # read-v2 base (M10, M7, expand fixes merged on dev): +151 on both. The
    # dev and M7 terms above were measured on the full surface (-3 in all);
    # on the lean surface the same text and schema cost +148 (inspect +43,
    # expand +39, filter +44, map +19, aggregate +5). Then -13: expand no
    # longer names target_subtype, which the lean surface hides.
    # read-v2 with M12: -13 on both. M12's terms (+55 +18 +9) were
    # measured on the full surface; on the lean one they cost +69.
    # read-v2: -1 on both (search's retain sentence names no hidden `input`).
    # read-v2: -12 on both (inspect drops the `surface` sentence).
    # loose-match-3666: +27 on both (search and filter name `like`).
    # publish-paging-args-3666: +200 on both. offset published again on
    # search, expand, filter, map and aggregate (+15 each, expand +7 more
    # for "One hop only."), search's continuation_token (+8) and max_chars
    # with its line (+36): +126; the instructions name max_chars again and
    # the node id schemes (+74). Then +63: search's expand_concepts (+8),
    # map's input (+19) and aggregate's input (+36), untyped with one line.
    # read-passage-rows-3666: -22 on both. search hides max_bytes and its
    # select line gains the attrs.<key> form (read tools -8); the reading
    # sentence no longer teaches select of attrs (instructions -14). Then
    # search publishes `view` again (+29 on both; D1), and the reading
    # sentence says how a ticket reads (+29 on both).
    # Prompt redesign, server half (#3666): -14 on both. The instructions
    # drop "Use `search`/`filter` to fetch specific nodes" and say "an id the
    # graph lacks" for "an id absent at the pin" (D8).
    # read-defects-rd-3666 merged (Format v7): +4 on both. inspect says
    # `lines` reads "a list of" [first,last] line ranges, the form a range
    # read takes. `contains` with text on attrs.<key> served (#3666): search
    # names it in place of "filter's like matches substrings" (+7 on both)
    # and filter names the text form beside its `like` sentence (+11 on
    # both), measured per tool on the lab.
    # Format v10 item C (#3666): +30 on both. aggregate shows by=["match"]
    # with a where text regex, and its `where` lists `text`.
    # read-search-caps-3666: +46 on both. search's description gains the
    # within="record" sentence and its schema the `within` enum.
    # read-aggregate-variants-3666: +32 on both. aggregate's description
    # names norm(match), words(match) and the capture group.
    # read-names-list-3666: +21 on both. search's description gains the
    # grep names-list sentence.
    # run-composed-3666: +147 on both (`run` and the instructions' run
    # sentence), as measured on lane RB's branch.
    assert read.total_tokens - 5_697 == -96 - 1 - 2 - 51 - 77 + 6 - 30 - 2 + 12 + 55 + 3 + 75 + 119 + 14 + 1 - 1 - 2 - 89 + 9 + 55 + 18 + 9 - 1 + 1 - 1 - 2 - 2213 - 14 + 1 + 151 - 13 - 13 - 1 - 12 + 27 + 200 + 63 - 22 + 29 + 29 - 14 + 4 + 7 + 11 + 30 + 29 + 46 + 32 + 21 + 82 + 147, measured_delta_message(read)
    assert write.total_tokens - 6_896 == -123 - 1 - 2 - 51 - 49 + 6 - 30 - 2 + 12 + 55 + 3 + 75 + 119 + 14 + 1 - 1 - 2 - 89 + 9 + 55 + 18 + 9 - 1 + 1 - 1 - 2 - 2222 - 14 + 1 + 151 - 13 - 13 - 1 - 12 + 27 + 200 + 63 - 22 + 29 + 29 - 14 + 4 + 7 + 11 + 30 + 29 + 46 + 32 + 21 + 82 + 147, measured_delta_message(write)


def measured_delta_message(measured) -> str:
    return (
        f"the surface serves {measured.total_tokens} tokens; record the "
        f"change's token delta in the sums above.\n{measured.breakdown()}"
    )


# ── the `none` capability profile (#3151 slice 1) ─────────────────────
#
# No embedder, no aliases, no passage rollups: a private deployment's shape, and
# what a fresh deployment serves. Against the `full` pins above, `search`'s
# schema loses `semantic` and `alias` from the method enum, `passages` from
# `rank_by`, and the alias-only and semantic-only names from the `select`
# enum (-99 B), and the instructions gain the generated block (+252 B: 250 B
# of lines plus the blank line before them). Net +153 B on every surface
# that serves `search`. Merged with the grep stack (lane B, 10-03) these
# pins move with the `full` ones: grep stays published under `none`.
# Merged grep stack + chain through #3219 (lane B, 10-04): dev 18,864 /
# 24,538 / 9,807 + chain (1,063 / 1,063 / 841) + grep stack 649 + #3219
# 195 = 20,771 full read, 26,445 write/admin, 11,492 inspect+search.
# read-record-windows (+363 B on every surface serving `search`):
# 20,771 + 363 = 21,134 full read, 26,445 + 363 = 26,808 write/admin,
# 11,492 + 363 = 11,855 inspect+search.
# read-text-batch (+753 B on every surface serving `inspect`; its schema
# does not depend on the capability fixture): 21,134 + 753 = 21,887 full
# read, 26,808 + 753 = 27,561 write/admin, 11,855 + 753 = 12,608
# inspect+search. Derived (the batch PR never measured the `none`
# profile), then confirmed: these pins pass on lab-host at 11406e66d.
# read-model-s4-list (#3225, +182 B alone on each `none` pin, the same as
# on the `full` pins); merged with the chain, +149 B (see the `full` pins:
# the `none` profile publishes the same `view` enum, cards, list, windows):
# 21,887 + 149 = 22,036 full read, 27,561 + 149 = 27,710 write/admin,
# 12,608 + 149 = 12,757 inspect+search. Measured on lab-host at c1f8829e41.
# lexical-quoted-phrases-3064 (`phrase_match` in `search.select`, +15 B;
# `none` keeps the field): 22,051 full read, 27,725 write/admin, 12,772
# inspect+search.
# Result references, slice 2 (#3222; see the `full` pins, the same deltas):
# 22,036 + 803 = 22,839 full read, 27,710 + 803 = 28,513 write/admin,
# 12,757 + 222 = 12,979 inspect+search.
# Result references, slice 3 (+226 B on `search`; `metadata` is published
# here too, on lexical records): 23,065 full read, 28,739 write/admin,
# 13,205 inspect+search.
# Plus lexical-quoted-phrases-3064's phrase_match (+15 B) from train 46c.
# Token re-base (surface-token-budget), measured after the dedent: 5,629
# full read, 6,801 write/admin, 3,227 inspect+search tokens (23,391, 28,787
# and 13,491 B); the generated block is 54 tokens (250 B).
# select-fields-by-method (-2 tokens on `search`, see the `full` pins):
# 5,626 full read, 6,798 write/admin, 3,224 inspect+search.
# dataset-card instructions (-51 on each): 5,575, 6,747, 3,173.
# dataset-card tool prose (see the `full` pins): -77, -49, -47:
# 5,498 full read, 6,698 write/admin, 3,126 inspect+search.
# read-query-guide-3666 (see the `full` pins): +73, +73, -30: 5,741 full
# read, 6,941 write/admin, 3,263 inspect+search; then +9, +9, 0 (the
# regex word boundary): 5,750 full read, 6,950 write/admin.
# read-pages-3b-3666: -1, -1 and -5 (as the `full` pins).
# read-expand-records-3666: -3, -3 and 0 (as the `full` pins).
# read-surface-diet-3666 round 2 (`search.select` text, -14, as the `full`
# pins): 3,452 full read, 4,643 write/admin, 2,158 inspect+search.
# Round 3 (`search.select` field list, no items enum): +16 on each, not +1
# as on `full`: this profile's enum had no semantic or alias fields, so
# dropping it saves less. 3,468 full read, 4,659 write/admin, 2,174
# inspect+search.
# read-v2 base (as the `full` pins): +148, +148 and +43: 3,616 full read,
# 4,807 write/admin, 2,217 inspect+search; then -13, -13 and 0 (expand
# drops target_subtype): 3,603, 4,794, 2,217.
# Merged (lane M2, 2026-10-14): +82, +82, -30 and -4, -4, -5: 5,746 full
# read, 6,946 write/admin, 3,258 inspect+search.
# read-v2 with 2c and M12 (as the `full` pins): +69, +69, -31: 3,672 full
# read, 4,863 write/admin, 2,186 inspect+search.
# Then the retain sentence: -1, -1, +4: 3,671, 4,862, 2,190.
# Then the `surface` sentence: -12 on each: 3,659, 4,850, 2,178.
# loose-match-3666 (as the `full` pins): +27, +27, +12: 3,686, 4,877, 2,190.
# publish-paging-args-3666 (as the `full` pins): +263, +263, +141: 3,949,
# 5,140, 2,331.
# read-passage-rows-3666 merged (segment B, as the `full` pins): -22, then
# search `view` published (+29), then the ticket reading sentence (+29),
# then the prompt redesign, server half: the authored instructions as the
# `full` pins (-14, -14, -10), and the generated block no longer names the
# methods the schema hides ("semantic", "alias"; lead decision 2026-10-16),
# 54 to 31 tokens (-23 on each): -1, -1, +3: 3,948, 5,139, 2,334.
# read-defects-rd-3666 merged (Format v7, as the `full` pins): inspect's
# `lines` sentence (+4 on each) and `contains` with text served: filter's
# text form (+11 where filter is served) and search's sentence (+7 where
# filter is served, +9 where only search is), measured on the lab:
# +22, +22, +13: 3,970, 5,161, 2,347.
# Then Format v10 item C (+30 on full read and write/admin, 0 on
# inspect+search; aggregate's description and `where`): 4,000, 5,191.
# Then read-reply-size-3666 (+29 on each, search's read-narrow line): 4,029,
# 5,220, 2,376.
# Then read-search-caps-3666 (+46 on each, search's within="record"
# sentence and `within` enum): 4,075, 5,266, 2,422.
# Then read-aggregate-variants-3666 (+32 where aggregate is served,
# aggregate's merged-key clause): 4,107, 5,298; inspect+search unchanged.
# Then read-names-list-3666 (+21 on each, search's grep names-list
# sentence): 4,128, 5,319, 2,443.
# Then run-composed-3666 (as the `full` pins): +147, +147, 0: 4,357 full
# read, 5,548 write/admin.
NONE_FULL_READ_SURFACE_BASELINE_TOKENS = 4_357
NONE_WRITE_ADMIN_SURFACE_BASELINE_TOKENS = 5_548
NONE_TWO_TOOL_SURFACE_BASELINE_TOKENS = 2_479
NONE_GENERATED_INSTRUCTIONS_TOKENS = 31


@pytest.mark.parametrize("label,kwargs,allowlist,expected", [
    ("full read", {}, None, NONE_FULL_READ_SURFACE_BASELINE_TOKENS),
    ("write/admin", {"allow_writes": True}, None,
     NONE_WRITE_ADMIN_SURFACE_BASELINE_TOKENS),
    ("inspect+search", {}, "inspect,search",
     NONE_TWO_TOOL_SURFACE_BASELINE_TOKENS),
])
async def test_the_none_profile_matches_its_exact_baseline(
    label: str, kwargs: dict, allowlist: str | None, expected: int,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("OKG_MCP_CAPABILITY_FIXTURE", "none")
    if allowlist is not None:
        monkeypatch.setenv("OKG_MCP_TOOL_ALLOWLIST", allowlist)
    measured = await measure_surface(**kwargs)
    assert measured.generated_instructions_tokens == (
        NONE_GENERATED_INSTRUCTIONS_TOKENS
    )
    assert measured.total_tokens == expected, (
        f"the {label} surface under the `none` capability profile now "
        f"serves {measured.total_tokens} tokens, not the recorded {expected}.\n"
        f"{measured.breakdown()}"
    )


async def test_a_bare_server_publishes_only_what_it_can_run(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The published `search` schema and the instructions agree with the
    probe record, end to end through the SDK's own `tools/list`."""
    from okg.substrate.mcp import server as server_mod
    from okg.substrate.mcp.server import build_server
    from okg.substrate.mcp.surface_budget import PROBE_DSN

    monkeypatch.setenv("OKG_MCP_CAPABILITY_FIXTURE", "none")
    monkeypatch.setattr(
        server_mod,
        "_server_dsn_resolution",
        lambda _dsn, _name: {
            "dsn": PROBE_DSN, "authority": {"source": "surface_probe_stub"},
        },
    )
    mcp = build_server(dsn=PROBE_DSN)
    try:
        search = next(t for t in await mcp.list_tools() if t.name == "search")
        props = search.inputSchema["properties"]
        # The lean read surface (#3666, M10) shows lexical and grep and hides
        # `rank_by`; the guard still serves exactly the probe's methods.
        assert props["method"]["description"] == "lexical or grep."
        assert "enum" not in props["method"] and "rank_by" not in props
        # The select fields the server serves; the published `select` lists
        # the shown methods' fields in words, with no items enum (#3666).
        assert props["select"]["items"] == {"type": "string"}
        fields = mcp._okg_capabilities.published_select_fields()
        assert {"node_id", "attrs", "excerpt", "score", "source_lineage"} <= set(fields)
        # `distance` only semantic returns; `canonical` only alias does.
        assert "distance" not in fields and "canonical" not in fields
        # Prompt redesign (#3666): the hidden methods (semantic, alias) are
        # not named in the generated block; it keeps the rank_by line.
        assert mcp.instructions.endswith(
            NOT_AVAILABLE_HEADER
            + '\n- search rank_by "passages": no subtype declares a passage '
            + "rollup."
        )
        assert "semantic" not in mcp.instructions
        report = mcp._okg_surface_freshness
        assert report["capabilities"]["mode"] == "fixture:none"
        assert report["capabilities"]["search"]["published_methods"] == [
            "lexical", "identifier_mentions", "attrs", "grep",
        ]
        assert report["lean_read_surface"]["shown_search_methods"] == ["lexical", "grep"]
        # The server's report states the generated block in bytes; it must
        # describe the text actually served, which is pinned in tokens.
        generated = split_generated_instructions(mcp.instructions)[1]
        assert count_tokens(generated) == NONE_GENERATED_INSTRUCTIONS_TOKENS
        assert report["generated_instructions_bytes"] == len(
            generated.encode("utf-8")
        )
        assert report["authored_instructions_hash"] != report["instructions_hash"]
    finally:
        mcp._okg_pool.close()
