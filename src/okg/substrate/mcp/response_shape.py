"""Compact MCP read responses on the wire (issue #2674, PACT mcp-response-metadata-diet).

A default MCP search of ten chunks was 17% source text: the rest repeated the
same values on every row, sent the generation id three times, and carried
each row's Nomos source lineage. `compact_for_wire` removes those repeats
from what the MCP server SENDS for `search`, `expand` and `inspect(node)`. The in-process Python API (`mcp.search.search`,
`mcp.tools.inspect_node`, `expand_neighbors`) and the operator console keep
the full shape; only the registered MCP tools call this function.

Every removal is reversible from the same response, by the rules published in
`docs/reference/mcp.md` ("Compact response shape"):

1. a row field missing from a search row takes its value from `row_defaults`;
2. a missing `canonical_node_id` equals `node_id`;
3. a missing row `method` equals the top-level `method`;
4. row `recency.attribute` and `.derivation` are the page `recency` block's;
5. missing `parent.subtype`, `.edge_type` and `.relation_source` come from
   `parent_relation.child_subtypes[row.subtype]` and `parent_relation.source`;
6. missing `evidence_kind_provenance.subtype` and `.source` are the row's
   `subtype` and `provenance.source`;
7. missing `generation_id` and `session_default_generation_id` equal
   `pinned_generation_id`;
8. a missing key from `NULL_OMITTED_SEARCH_KEYS` was null;
9. a missing `rollup_best_chunk_id` or `matching_passages` was null (every
   `rank_by`);
10. a search row's `parent` (or a candidate in its `parents` list) that has a
    `node_id` but no summary takes `PARENT_SUMMARY_FIELDS` from the page's
    `parent_summaries[node_id]`.

What never moves: identity-floor fields, per-row trust (`provenance`,
`read_provenance`), `source_record_id`, and every fenced value except a
parent summary, which is the parent's own text fenced by the parent's own
trust and moves whole, fence included, to the one entry for that parent.
`source_revision` stays on its row unless every row on the page is trusted
and carries the same value; then it is stated once in `row_defaults`
(PACT mcp-search-compact-results). A field the caller names in `select` is
sent on every row and never hoisted or omitted.

The function is PURE. The byte bound measures a shallow copy of the payload
(`response_bounds.bound_read_tool`), so a pass that edited rows in place would
strip hoisted fields from the served rows while `row_defaults` landed only on
the copy. Every container this function changes is copied first.
"""
from __future__ import annotations

from typing import Any, Iterable, Mapping

from okg.substrate.mcp.provenance import FENCE_CLOSE, is_fenced
from okg.substrate.nomos.source_lineage import LINEAGE_ATTR
from okg.substrate.searchable_text import PRODUCER_TEXT_ATTR, without_producer_text

__all__ = [
    "LINEAGE_SELECT_TOKEN",
    "NULL_OMITTED_SEARCH_KEYS",
    "RESPONSE_SHAPE",
    "compact_for_wire",
    "lineage_select_plan",
    "move_lineage_to_rows",
]

#: Marks a compacted response, so a code client can tell it from the full one.
RESPONSE_SHAPE = 2

#: `select` token that asks for each row's source lineage (option B).
LINEAGE_SELECT_TOKEN = "source_lineage"

#: Search disclosure keys left out when null: each is null exactly when the
#: scope or diversity rule it describes was not applied. No other null is
#: omitted (`next_offset`, `logical_session_id` and the counts-unavailable
#: `inspect(graph)` keys are promised as explicit nulls).
NULL_OMITTED_SEARCH_KEYS: tuple[str, ...] = (
    "max_per_parent",
    "total_count_before_diversity",
    "duplicates_collapsed",
    "diversity_parents_resolved",
    "diversity_rows_without_parent",
    "search_profile",
    "scoped_subtypes",
    "source_families",
    "scoped_source_ids",
    "scoped_source_ids_truncated",
    "scoped_source_id_count",
    "source_family_validation",
    "source_family_application",
    "excluded_path_globs",
    "excluded_path_globs_application",
)

#: A readable parent's summary on a search card (issue #2675). Moved whole,
#: fence included, into the page's `parent_summaries` under the parent's
#: `node_id`: chunks on one page usually share a parent, and the summary is
#: the parent's own (its trust fences it), so it is the same on every card
#: that names that parent.
PARENT_SUMMARY_FIELDS: tuple[str, ...] = (
    "citation",
    "citation_source",
    "title",
    "title_attr",
    "status",
    "passage_count",
)

#: Search row fields hoisted into `row_defaults` when identical on every row.
#: Only server-computed values: never producer text, never trust, never the
#: identity floor.
_HOISTABLE_SEARCH_FIELDS: tuple[str, ...] = ("match_kind", "source_family")

#: Producer-supplied search row fields hoisted into `row_defaults` only when
#: identical on every row AND every row's `provenance.trust` is `"trusted"`
#: (PACT mcp-search-compact-results). This narrows, for trusted pages only,
#: the #2674 rule that producer values stay beside their row's trust block
#: (PACT mcp-response-metadata-diet, req ...lossless and ...trust-boundary):
#: when every row on the page carries the same trust label, "trusted", the
#: page-level value sits beside that same label. One row that is untrusted or
#: unlabeled keeps the field on every row, exactly as before.
#: `source_revision` names the source run a row came from (run id, root,
#: record count, content hash): identical on every row of a one-source page,
#: and not what a chat cites (it cites `node_id`, `source_record_id` and the
#: generation).
_TRUSTED_HOISTABLE_SEARCH_FIELDS: tuple[str, ...] = ("source_revision",)

_ROW_KEYS: tuple[str, ...] = ("results", "neighbors")


def _get(value: Any, key: str) -> Any:
    if isinstance(value, Mapping):
        return value.get(key)
    return getattr(value, key, None)


def _select_names(select: Any) -> frozenset[str]:
    if isinstance(select, (list, tuple)):
        return frozenset(str(item) for item in select)
    return frozenset()


def _inner_text(value: Any) -> Any:
    """The text inside this module's own fence, or the value unchanged.

    Fenced text has every embedded marker escaped
    (`provenance.escape_embedded_fence_markers`), so the first newline ends the
    open marker and the last `\\n⟦/UNTRUSTED_DATA⟧` is the close.
    """
    if not isinstance(value, str) or not is_fenced(value):
        return value
    body = value.rstrip()
    body = body[body.index("\n") + 1:]
    return body[: -len("\n" + FENCE_CLOSE)]


def _without_producer_text_anywhere(value: Any) -> Any:
    """`value` with `okg_producer_text` removed from every `attrs` mapping.

    Walks dicts and lists; returns the SAME object when nothing inside it
    changed, so an ordinary response is not copied, and never mutates input.
    """
    if isinstance(value, dict):
        changed = False
        out: dict[Any, Any] = {}
        for key, item in value.items():
            if (
                key == "attrs" and isinstance(item, Mapping)
                and PRODUCER_TEXT_ATTR in item
            ):
                new = {k: v for k, v in item.items() if k != PRODUCER_TEXT_ATTR}
                # Keep the read filter's in-memory proof of a redact decision.
                new = type(item)(new) if isinstance(item, dict) else new
            else:
                new = _without_producer_text_anywhere(item)
            changed = changed or new is not item
            out[key] = new
        if not changed:
            return value
        return type(value)(out) if type(value) is not dict and isinstance(value, dict) else out
    if isinstance(value, list):
        items = [_without_producer_text_anywhere(item) for item in value]
        if all(new is old for new, old in zip(items, value)):
            return value
        return items
    return value


def _lineage_is_ok(lineage: Any) -> bool:
    """Whether a lineage envelope says `status: ok`, read without its fence."""
    return isinstance(lineage, Mapping) and _inner_text(lineage.get("status")) == "ok"


def _without_ok_lineage(attrs: Any) -> Any:
    if isinstance(attrs, Mapping) and _lineage_is_ok(attrs.get(LINEAGE_ATTR)):
        return {key: value for key, value in attrs.items() if key != LINEAGE_ATTR}
    return attrs


def _compact_generation(out: dict[str, Any]) -> None:
    pinned = out.get("pinned_generation_id")
    if pinned is None:
        return
    for key in ("generation_id", "session_default_generation_id"):
        if key in out and out[key] == pinned:
            del out[key]


def _compact_search_row(
    row: dict[str, Any],
    *,
    page: Mapping[str, Any],
    selected: frozenset[str],
) -> dict[str, Any]:
    out = dict(row)
    if (
        "canonical_node_id" not in selected
        and "canonical_node_id" in out
        and out["canonical_node_id"] == out.get("node_id")
    ):
        del out["canonical_node_id"]
    if (
        "method" not in selected
        and "method" in out
        and out["method"] == page.get("method")
    ):
        del out["method"]
    # Under `records` most rows carry a winning passage and some do not, so a
    # null winner or count is dropped in every mode (PACT search-result-shape).
    for key in ("rollup_best_chunk_id", "matching_passages"):
        if key not in selected and key in out and out[key] is None:
            del out[key]
    recency = out.get("recency")
    page_recency = page.get("recency")
    if isinstance(recency, dict) and isinstance(page_recency, Mapping):
        slim = dict(recency)
        for key in ("attribute", "derivation"):
            if key in slim and slim[key] == page_recency.get(key):
                del slim[key]
        out["recency"] = slim
    parent = out.get("parent")
    relation = page.get("parent_relation")
    if isinstance(parent, dict) and isinstance(relation, Mapping):
        slim = dict(parent)
        if slim.get("relation_source") == relation.get("source"):
            slim.pop("relation_source", None)
        declared = (relation.get("child_subtypes") or {}).get(out.get("subtype"))
        if isinstance(declared, Mapping):
            if "subtype" in slim and slim["subtype"] == declared.get("parent_subtype"):
                del slim["subtype"]
            if "edge_type" in slim and slim["edge_type"] == declared.get("edge_type"):
                del slim["edge_type"]
        out["parent"] = slim
    evidence = out.get("evidence_kind_provenance")
    if isinstance(evidence, dict):
        slim = dict(evidence)
        if "subtype" in slim and slim["subtype"] == out.get("subtype"):
            del slim["subtype"]
        row_source = _get(out.get("provenance"), "source")
        if "source" in slim and row_source is not None and slim["source"] == row_source:
            del slim["source"]
        out["evidence_kind_provenance"] = slim
    if "attrs" in out:
        out["attrs"] = _without_ok_lineage(out["attrs"])
    return out


def _hoist(
    rows: list[dict[str, Any]],
    fields: Iterable[str],
    *,
    selected: frozenset[str],
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    if len(rows) < 2:
        return rows, {}
    defaults: dict[str, Any] = {}
    for key in fields:
        if key in selected or not all(key in row for row in rows):
            continue
        first = rows[0][key]
        if all(row[key] == first for row in rows[1:]):
            defaults[key] = first
    if not defaults:
        return rows, {}
    return [
        {key: value for key, value in row.items() if key not in defaults}
        for row in rows
    ], defaults


def _hoist_parent_summaries(
    rows: list[dict[str, Any]],
    *,
    selected: frozenset[str],
) -> tuple[list[dict[str, Any]], dict[str, dict[str, Any]]]:
    """Move each parent's summary to one page-level entry per parent.

    Applies to a `resolved` parent and to each `candidate` inside a
    `multiple_parents` block: the blocks that carry every summary field and a
    `node_id`. Only a block carrying all six `PARENT_SUMMARY_FIELDS` is
    hoisted; a block with some of them stays whole on its card and is never
    merged with, or recorded in, the page entry. The card keeps `state` and `node_id`, so which parent a card
    names (and whether it resolved) never moves. A summary that differs from
    the one already recorded for that `node_id` stays on its card, so the
    rebuild is exact whatever the input.
    """
    if "parent" in selected:
        return rows, {}
    summaries: dict[str, dict[str, Any]] = {}

    def take(block: Any) -> Any:
        if not isinstance(block, dict):
            return block
        node_id = block.get("node_id")
        if not isinstance(node_id, str) or not all(
            key in block for key in PARENT_SUMMARY_FIELDS
        ):
            return block
        summary = {key: block[key] for key in PARENT_SUMMARY_FIELDS}
        prior = summaries.setdefault(node_id, summary)
        if prior != summary:
            return block
        return {
            key: value for key, value in block.items()
            if key not in PARENT_SUMMARY_FIELDS
        }

    out: list[dict[str, Any]] = []
    for row in rows:
        parent = row.get("parent")
        if not isinstance(parent, dict):
            out.append(row)
            continue
        slim = take(parent)
        candidates = slim.get("parents")
        if isinstance(candidates, list):
            slim = {**slim, "parents": [take(item) for item in candidates]}
        out.append({**row, "parent": slim} if slim is not parent else row)
    return out, summaries


def _all_rows_trusted(rows: list[dict[str, Any]]) -> bool:
    """Every row states `provenance.trust == "trusted"`; a missing label is not trust."""
    return bool(rows) and all(
        _get(row.get("provenance"), "trust") == "trusted" for row in rows
    )


def _compact_search(out: dict[str, Any], *, selected: frozenset[str]) -> None:
    for key in NULL_OMITTED_SEARCH_KEYS:
        if key in out and out[key] is None:
            del out[key]
    rows = out.get("results")
    if not isinstance(rows, list) or not all(isinstance(row, dict) for row in rows):
        return
    relation = out.get("parent_relation")
    if isinstance(relation, dict) and "absent_reason" in relation:
        if rows and all(row.get("parent") is not None for row in rows if "parent" in row):
            out["parent_relation"] = {
                key: value for key, value in relation.items() if key != "absent_reason"
            }
    compacted = [_compact_search_row(row, page=out, selected=selected) for row in rows]
    compacted, parent_summaries = _hoist_parent_summaries(compacted, selected=selected)
    if parent_summaries:
        out["parent_summaries"] = parent_summaries
    # Alias rows carry `match_kind` in their identity floor.
    hoistable = tuple(
        key for key in _HOISTABLE_SEARCH_FIELDS
        if not (key == "match_kind" and out.get("method") == "alias")
    )
    if _all_rows_trusted(compacted):
        hoistable = (*hoistable, *_TRUSTED_HOISTABLE_SEARCH_FIELDS)
    compacted, defaults = _hoist(compacted, hoistable, selected=selected)
    out["results"] = compacted
    # `identifier_mentions` serves the same page twice, as `mentions` and
    # `results`; keep the two identical.
    if out.get("mentions") is rows:
        out["mentions"] = compacted
    if defaults:
        out["row_defaults"] = defaults


def _row_without_ok_lineage(row: Any) -> Any:
    """One row with an `ok` source lineage left out of its `attrs`, and of
    the `attrs` of its `link` (issue #3666: a 1-hop record row carries the
    link's own attributes there, and they are cleaned as a link row's are)."""
    if not isinstance(row, dict):
        return row
    out = row
    if "attrs" in row:
        out = {**out, "attrs": _without_ok_lineage(row["attrs"])}
    link = row.get("link")
    if isinstance(link, dict) and "attrs" in link:
        out = {**out, "link": {**link, "attrs": _without_ok_lineage(link["attrs"])}}
    return out


def _compact_rows_lineage(out: dict[str, Any]) -> None:
    for key in _ROW_KEYS:
        rows = out.get(key)
        if not isinstance(rows, list):
            continue
        out[key] = [_row_without_ok_lineage(row) for row in rows]


def _target_kind(arguments: Mapping[str, Any]) -> Any:
    return _get(arguments.get("target"), "kind")


def compact_for_wire(
    payload: Any,
    arguments: Mapping[str, Any],
) -> Any:
    """Return the compact wire form of one read response; never mutates input.

    `arguments` are the tool call's arguments (only `select` and, for
    `inspect`, `target` are read). The operator is read from the payload's
    `operator_name`, which every read operator stamps. Refusals, other
    operators and other `inspect` kinds come back unchanged.
    """
    if not isinstance(payload, dict):
        return payload
    if payload.get("error") is not None or payload.get("status") == "refused":
        return payload
    # Every operator's response passes here (the logical response envelope),
    # so this is the one place the searchable-text copy (issue #2991) is left
    # out of any `attrs` mapping a read returns: query, filter, map,
    # aggregate, inspect of any kind, run_sql rows named `attrs`.
    payload = _without_producer_text_anywhere(payload)
    operator = payload.get("operator_name")
    kind = _target_kind(arguments) if operator == "inspect" else None
    # `inspect(graph)` keeps its full shape: its duplicate count maps are a
    # separate follow-up (issue #2674 decision, 2026-09-27).
    if operator not in {"search", "expand"} and kind != "node":
        return payload
    if payload.get("response_shape") == RESPONSE_SHAPE:
        return payload
    out = dict(payload)
    out.pop("pagination", None)
    _compact_generation(out)
    selected = _select_names(arguments.get("select"))
    if operator == "search":
        _compact_search(out, selected=selected)
    elif operator == "expand":
        _compact_rows_lineage(out)
    elif "attrs" in out:
        out["attrs"] = _without_ok_lineage(out["attrs"])
    out["response_shape"] = RESPONSE_SHAPE
    return out


def compact_node_record(record: Any) -> Any:
    """One record of a batch read (#3657) with the attributes a single
    `inspect(node)` leaves out of its reply: the searchable-text copy and an
    `ok` source lineage. Never mutates input."""
    if not isinstance(record, dict) or not isinstance(record.get("attrs"), Mapping):
        return record
    return {**record, "attrs": _without_ok_lineage(without_producer_text(record["attrs"]))}


# ── the `source_lineage` select token ─────────────────────────────────────


def lineage_select_plan(
    select: Any,
    *,
    default_without_attrs: tuple[str, ...] | None = None,
) -> tuple[Any, bool, bool, bool]:
    """Rewrite a caller's `select` that names `source_lineage`.

    Returns `(operator_select, requested, drop_attrs, preview_as_default)`:

    * `operator_select` is what the in-process operator receives: the
      caller's other fields plus `attrs` (the lineage lives in `attrs`), or
      the operator's default projection when the caller named nothing else.
      `default_without_attrs` is that default when it lacks `attrs` (lexical
      search); `None` means the default already includes `attrs`.
    * `requested` says whether the token was present.
    * `drop_attrs` says `attrs` was added only to reach the lineage and must
      not be sent.
    * `preview_as_default` says the caller named nothing but the token, so
      the default excerpt preview still applies.
    """
    if not isinstance(select, (list, tuple)) or LINEAGE_SELECT_TOKEN not in select:
        return select, False, False, select is None
    rest = [str(item) for item in select if item != LINEAGE_SELECT_TOKEN]
    if not rest:
        if default_without_attrs is None:
            return None, True, False, True
        return [*default_without_attrs, "attrs"], True, True, True
    if "attrs" in rest:
        return rest, True, False, False
    return [*rest, "attrs"], True, True, False


def _row_with_lineage(row: Any, *, drop_attrs: bool) -> Any:
    if not isinstance(row, dict):
        return row
    out = dict(row)
    attrs = without_producer_text(out.get("attrs"))
    if isinstance(attrs, dict):
        out["attrs"] = attrs
    lineage = None
    if isinstance(attrs, dict) and LINEAGE_ATTR in attrs:
        lineage = attrs[LINEAGE_ATTR]
        out["attrs"] = {key: value for key, value in attrs.items() if key != LINEAGE_ATTR}
    out[LINEAGE_SELECT_TOKEN] = lineage
    if drop_attrs:
        out.pop("attrs", None)
    return out


def move_lineage_to_rows(
    payload: Any,
    *,
    drop_attrs: bool,
    single_row: bool = False,
) -> Any:
    """Move each row's lineage from `attrs` to a row-level `source_lineage`.

    The value is moved unchanged: on an untrusted row it is already fenced.
    A row with no lineage gets `source_lineage: null`, so the caller can tell
    "asked and absent" from "not asked". `single_row` handles `inspect(node)`,
    whose payload is the row.
    """
    if not isinstance(payload, dict) or payload.get("error") is not None:
        return payload
    if single_row:
        return _row_with_lineage(payload, drop_attrs=drop_attrs)
    out = dict(payload)
    for key in (*_ROW_KEYS, "mentions"):
        rows = out.get(key)
        if isinstance(rows, list):
            out[key] = [_row_with_lineage(row, drop_attrs=drop_attrs) for row in rows]
    return out
