"""The read tools' published surface: the shown forms only (issue #3666, M10).

An okg agent's first prompt carried about 4,700 cl100k tokens of read-tool
schemas and descriptions, re-sent on every model turn. Most of it described
arguments and enum values that the shorter forms already cover. This pass
removes them from what `tools/list` PUBLISHES and from nothing else:

* calls are validated against each tool's argument model, not the published
  schema (FastMCP registers its call handler with `validate_input=False`),
  and the unknown-argument check reads that model too, so every hidden
  argument, enum value and `inspect` kind is still accepted exactly as
  before;
* a client that validates its arguments against the published schema is
  not told a served call is invalid either: the top-level objects carry no
  `additionalProperties: false`, `inspect.target` ends with an open member,
  `expand.input` is still typed string or object, `search.method` names its
  shown methods in words rather than as a closed enum, and `search.select`
  lists its fields in words (no items enum).

What stays shown (design `read-interface` section 7): the shared arguments
(`where`, `select`, `order_by`, `limit`, `next`, `max_bytes`, `retain`;
`search` hides `max_bytes` since PACT read-passage-rows-3666),
the arguments `PUBLISHED_AGAIN` names (`offset`, which every cut page's
`next:` names, and `search`'s `max_chars`), the
`search` methods `lexical` and `grep`, and the `inspect` kinds graph, node,
nodes, text, texts and subtype. `where` is published as one object whose
description states the condition language, on every tool that takes it; the
top-level `where` of `filter`, `map`, `aggregate` and `expand` and `map`'s
`select` are spellings the read grammar (`read_grammar.rewrite_call`)
already serves.

Published copy only, after `publish_client_readable_schemas`. Raises
`ToolSchemaUnpublishable` when a tool's schema is not the shape this pass
expects, rather than publishing a half-narrowed surface.
"""
from __future__ import annotations

from typing import Any

from okg.substrate.mcp.operator_models import ToolSchemaUnpublishable

__all__ = [
    "HIDDEN_ARGUMENTS",
    "PUBLISHED_AGAIN",
    "SHOWN_INSPECT_KINDS",
    "SHOWN_SEARCH_METHODS",
    "WHERE_ADDED",
    "publish_lean_read_surface",
    "where_schema",
]

#: Arguments the M10 diet took out of each read tool's published schema. The
#: server accepted every one of them throughout. `search`'s `subtype_filter`,
#: `attr_filters` and `filter` were already hidden
#: (`UNPUBLISHED_SEARCH_ARGUMENTS`).
_DIET_HIDDEN: dict[str, tuple[str, ...]] = {
    "search": (
        "profile", "source_family", "max_per_parent", "rank_by", "offset",
        "generation_id", "authority", "allow_unverified_embedding_provenance",
        "max_chars", "expand_concepts", "continuation_token", "view",
        "matching", "context_chars", "summary",
        # PACT read-passage-rows-3666 (D3, D4): a search page over its byte
        # budget keeps every requested row and shortens passages and
        # selected values instead, so `max_chars` is search's one size
        # control. `max_bytes` is still accepted; no hint names it.
        "max_bytes",
    ),
    "inspect": ("generation_id", "authority"),
    "expand": (
        "depth", "target_subtype", "continuation_token", "filter", "offset",
        "generation_id", "include_siblings", "live", "authority",
        # The values of a live edge's arguments (PACT live-graph-rucio-sites);
        # hidden so the published schema keeps its measured size.
        "args",
    ),
    "filter": ("input", "predicate", "offset", "generation_id", "authority"),
    # aggregate's `predicate` reads a kept live answer; the agent writes the
    # condition as `where`, which the grammar lowers to `predicate` (PACT
    # live-graph-rucio-sites). Hidden so the published schema keeps its
    # measured size.
    "map": ("input", "projection", "offset", "generation_id", "authority", "predicate"),
    "aggregate": ("input", "offset", "generation_id", "authority", "predicate"),
}

#: Arguments published again (PACT publish-paging-args-3666), each with the
#: short description it is published with ("" publishes the bare type and
#: bounds; the reply that names it says what it does). One line each: adding
#: an argument here takes it out of `HIDDEN_ARGUMENTS`. The published copy
#: drops the model's `default`, as the other lean arguments do not carry one.
#:
#: * `offset`: every cut page of these tools ends `next: offset=N` (and a
#:   search page cut at `max_bytes` says "page with offset"). A hint must
#:   name a published argument.
#: * `search.continuation_token`: a grep page stopped at its scan bound,
#:   and every later page of that chain, ends `next: continuation_token=T`.
#: * `search.max_chars`: the excerpt length. In the comp-ops runs agents
#:   used it on 66% of searches before the diet and selected whole `attrs`
#:   under `max_bytes` after it, so a cut page held a few rows.
#: * `search.expand_concepts`: an exact-order notice on a concept-expanded
#:   search says "rerun with expand_concepts=false".
#: * `map.input` and `aggregate.input`: a retained search page ends
#:   `next: map(input={"ref": R}, offset=N)`, a summary cut short
#:   `next: aggregate(input={"ref": R}, ...)`, and the refusal for counting
#:   links names `aggregate(input={"kind": "edges", ...})`. Published
#:   untyped (`UNTYPED`), with one line, not their ~200-token declared shape.
#: * `filter.input` (PACT read-live-handles): every live set reply names a
#:   handle and the descriptions teach `filter(input="<handle>")`; a cut
#:   kept page ends `next: filter(input={"ref": H}, ...)`. Published untyped
#:   with one line, as `map.input` is.
PUBLISHED_AGAIN: dict[str, dict[str, str]] = {
    "search": {
        "offset": "",
        "continuation_token": "",
        # read-passage-rows-3666 (D2): the one size control on search; the
        # default passage length depends on the record type.
        "max_chars": "Passage length in characters.",
        "expand_concepts": "",
        # read-passage-rows-3666 (D1, D6): the short row shape. A hint and
        # the reading guide name it, so it is published.
        "view": '"list": head lines only, no passages.',
    },
    "expand": {"offset": "One hop only."},
    "filter": {"offset": "", "input": "A handle a reply printed."},
    "map": {
        "offset": "",
        "input": 'A handle; {"ref": id} reads a retained result.',
    },
    "aggregate": {
        "offset": "",
        "input": 'A handle, {"kind": "edges", "edge_type": "<type>"}, or one record id.',
    },
}

#: Published-again arguments whose declared shape is not published: the
#: published copy is the description alone, so it accepts any value and the
#: server's own argument model still validates the call. Neither is listed
#: as required.
UNTYPED: frozenset[tuple[str, str]] = frozenset({
    ("filter", "input"), ("map", "input"), ("aggregate", "input"),
})


def check_published_again(
    published: dict[str, dict[str, str]], diet: dict[str, tuple[str, ...]],
) -> None:
    """Refuse an entry the diet never hid: it would publish nothing new and
    could hide a typo. Run at import on the two lists above."""
    outside = {
        tool: sorted(set(names) - set(diet.get(tool, ())))
        for tool, names in published.items()
    }
    if any(outside.values()):
        raise ToolSchemaUnpublishable(
            "PUBLISHED_AGAIN names arguments the diet never hid",
            data={tool: names for tool, names in outside.items() if names},
        )


check_published_again(PUBLISHED_AGAIN, _DIET_HIDDEN)

#: Arguments each read tool still accepts but does not publish.
HIDDEN_ARGUMENTS: dict[str, tuple[str, ...]] = {
    tool: tuple(n for n in names if n not in PUBLISHED_AGAIN.get(tool, {}))
    for tool, names in _DIET_HIDDEN.items()
}

#: Tools that gain a published top-level `where` (the grammar's spelling of
#: `predicate`, `input.filter` and `expand.filter`). `search` already has one;
#: its schema is replaced by the same short one.
WHERE_ADDED: tuple[str, ...] = ("filter", "map", "aggregate", "expand")

SHOWN_SEARCH_METHODS: tuple[str, ...] = ("lexical", "grep")
SHOWN_INSPECT_KINDS: tuple[str, ...] = ("graph", "node", "nodes", "text", "texts", "subtype")
#: Fields of a shown `inspect` kind that are hidden in the same way.
#: A shown kind keeps all its fields (`hydrate`, a nested
#: `continuation_token`): its member is closed (`additionalProperties: false`),
#: so hiding one would make a validating client refuse a call the server
#: serves.

_CONDITION = (
    'Condition: {"<field>": value} (a list means any of; several keys all '
    'hold), or a test {"<field>": {"op": "prefix", "value": "x"}}. ops: eq, '
    'in, exists, contains, prefix, suffix, lt, gt, between (in and between '
    'take "values"), like, like_exact (substrings; not in search); '
    '{"op": "or", "clauses": [...]}, {"op": "not", '
    '"clause": {...}}.'
)
#: The short form, on the tools whose own description shows a condition.
_CONDITION_SHORT = (
    'Condition: {"<field>": value}, a list means any of; or {"<field>": '
    '{"op": "prefix", "value": "x"}}.'
)
_FULL_CONDITION_TOOLS = frozenset({"search", "filter"})

#: The fields each tool's `where` compares (`read_grammar`: `SEARCH_FIELDS`,
#: `NODE_FIELDS`, `DATE_CONDITION_TOOLS`).
_WHERE_FIELDS: dict[str, str] = {
    "search": "Fields: id, type, source, title, path, attrs.<key>, date.<name>.",
    "aggregate": "Fields: node_id, type, text, attrs.<key>, date.<name>.",
    "filter": "Fields: node_id, type, attrs.<key>, date.<name>.",
    "map": "Fields: node_id, type, attrs.<key>, date.<name>.",
    "expand": "Fields of the far record: node_id, type, attrs.<key>, date.<name>.",
}


def where_schema(tool: str) -> dict[str, Any]:
    """The published `where` of `tool`: an object, described once."""
    condition = _CONDITION if tool in _FULL_CONDITION_TOOLS else _CONDITION_SHORT
    return {"type": "object", "description": f"{condition} {_WHERE_FIELDS[tool]}"}


def _kinds_of(member: dict[str, Any]) -> list[str]:
    kind = (member.get("properties") or {}).get("kind")
    if not isinstance(kind, dict):
        return []
    if "const" in kind:
        return [kind["const"]]
    values = kind.get("enum")
    return list(values) if isinstance(values, list) else []


def _narrow_inspect_target(
    schema: dict[str, Any], shown_kinds: frozenset[str], hidden: list[str],
) -> list[str]:
    """Keep the members of the shown kinds; return the kinds kept.

    `hidden` gains one line per hidden kind, for the read-only `surface`
    listing.
    """
    import json

    target = schema.get("properties", {}).get("target")
    members = target.get("oneOf") if isinstance(target, dict) else None
    if not isinstance(members, list):
        raise ToolSchemaUnpublishable(
            "inspect.target is not a published oneOf of kinds; cannot narrow it",
        )
    kept: list[dict[str, Any]] = []
    for member in members:
        all_kinds = _kinds_of(member)
        kinds = [k for k in all_kinds if k in shown_kinds]
        fields = {
            k: v for k, v in (member.get("properties") or {}).items() if k != "kind"
        }
        for kind in all_kinds:
            if kind not in kinds:
                hidden.append(_line(f'inspect target kind "{kind}"', json.dumps(
                    sorted(fields), separators=(",", ":"),
                )))
        if not kinds:
            continue
        member = dict(member)
        properties = dict(member.get("properties") or {})
        kind = dict(properties["kind"])
        if "enum" in kind:
            kind["enum"] = kinds
        properties["kind"] = kind
        member["properties"] = properties
        kept.append(member)
    if not kept:
        raise ToolSchemaUnpublishable(
            "no shown inspect kind is served; refusing to publish an empty target",
        )
    # One open member keeps every hidden kind valid for a client that checks
    # its arguments against the schema: such a client must never be told a
    # call the server accepts is invalid. With it the members can overlap,
    # so they are an `anyOf`, and no longer a union discriminated on `kind`.
    del target["oneOf"]
    target.pop("discriminator", None)
    # The open member takes only kinds no shown member names, so a malformed
    # shown kind (`lines` as a string, say) is still refused by such a client.
    shown = [k for member in kept for k in _kinds_of(member)]
    target["anyOf"] = [*kept, {
        "type": "object", "required": ["kind"],
        "properties": {"kind": {"not": {"enum": shown}}},
    }]
    return shown


#: What each hidden form does, for the read-only `surface` listing. The
#: text is what the tool descriptions said before #3666 M10 shortened them,
#: kept where it is still true of the code; the listing is not sent at
#: connect, so its detail costs no startup tokens. Keyed by the line's name.
HIDDEN_NOTES: dict[str, str] = {
    # PACT live-graph-rucio-sites (#3666).
    "expand(args=...)": (
        "the values a live edge takes, e.g. expand(input=\"site:A\", "
        "edge_type=\"link\", args={\"to\": \"site:B\"}); a node id is "
        "passed as its name"
    ),
    "search(profile=...)": (
        "applies a declared subtype scope (`all` disables it); the "
        "deployment's default applies when neither profile nor a `where` type "
        "is given"
    ),
    "search(source_family=...)": "picks declared producer families before ranking",
    "search(max_per_parent=...)": "a per-parent diversity bound on ranked rows",
    "search(rank_by=...)": (
        "lexical defaults to `records`: one card per record, passages folded "
        "in, `limit` counting records; `self` ranks each row by its own text; "
        "`passages` needs a declared child relation"
    ),
    "search(generation_id=...)": "reads this one call at another published generation",
    "search(view=...)": (
        '"list": one line per record (id, type, handle), no excerpt, on '
        'lexical records or grep, limit default and maximum 100 (cards: 8); '
        '"windows" (grep only; no where, path_prefix, profile, offset, '
        "source_family, max_per_parent or rank_by other than records): the "
        "literal matching windows of the `scope` records, else of this grep's "
        'top `limit` records; "metadata": a retained result reference without '
        "rows"
    ),
    "search(matching=...)": (
        "windows view only: `record` decides each term across the record's "
        "chained text, `passage` within each text alone"
    ),
    "search(context_chars=...)": "windows view only: the characters around each match",
    "search(max_bytes=...)": (
        "the reply's byte budget; a page over it keeps every row and "
        "shortens passages and selected values to fit"
    ),
    "search(summary=...)": (
        '1-4 fields, with view="metadata" only: adds the counts per value of '
        "the referenced records, top 50 groups"
    ),
    "expand(depth=...)": "repeats one edge_type and direction up to 3 hops; resumes by `next` alone",
    "expand(target_subtype=...)": (
        "on a path or depth, filters only the terminal record, so a "
        "terminal-empty path stays distinguishable from no path"
    ),
    "expand(filter=...)": "the old spelling of `where` (one hop only)",
    "expand(continuation_token=...)": "the old paging token; `next` replaces it",
    "expand(include_siblings=...)": (
        "adds policy-filtered same-type neighbours through catalog-declared "
        "self-relations, telling undeclared structure apart from a declared "
        "empty relation"
    ),
    "expand(live=...)": (
        "{relation, args}: also asks one live relation for this record's "
        "type; its rows return in `external_live`, never merged with the "
        "graph rows"
    ),
    "filter(input=...)": (
        'the set: {"kind": "nodes"|"edges", "subtype", "edge_type", '
        '"filter"} or {"ref": id}; `where` with a type stands for it'
    ),
    "filter(predicate=...)": "the old spelling of `where`",
    "map(projection=...)": "the old spelling of `select`; also {alias: field} to rename",
    'inspect target kind "coverage"': (
        'tells "search found nothing" apart from "the authoritative source '
        'is not in this graph"'
    ),
    'inspect target kind "view"': (
        "a view's columns: the schema path, since `query` refuses "
        "information_schema"
    ),
    "dates": (
        "a date field is date.<name> (created, modified, published, "
        "resolved, effective) or date.attrs.<key>; a bare `date` is refused; "
        "a value with no time zone is read as UTC"
    ),
}


def _line(name: str, detail: str) -> str:
    """One listing line: the form, its shape in full, and what it does."""
    note = HIDDEN_NOTES.get(name)
    return f"{name}: {detail}" + (f" -- {note}" if note else "")


def _narrow_search(
    schema: dict[str, Any], capabilities: Any, hidden: list[str],
) -> list[str]:
    """Show `lexical` and `grep`; return the methods shown.

    The method `enum` (the methods this server runs) becomes words: an enum
    of two would make a validating client refuse the hidden methods the
    server still runs. `select` loses its items enum for the same reason;
    its description lists the shown methods' fields. `hidden` gains one
    line per hidden method with the fields only it returns.
    """
    properties = schema["properties"]
    method = properties.get("method")
    if not isinstance(method, dict) or not isinstance(method.get("enum"), list):
        raise ToolSchemaUnpublishable(
            "search.method is not a published enum; cannot narrow it",
        )
    served = list(method["enum"])
    shown = [m for m in served if m in SHOWN_SEARCH_METHODS]
    del method["enum"]
    method["description"] = " or ".join(shown) + "."
    select = properties.get("select")
    if capabilities is None or not isinstance(select, dict):
        return shown
    # The description lists the shown methods' fields (the names an agent
    # may select) and which row field holds each `where`-only name. With
    # the list cut, agents selected `where`'s title, path and source in 84
    # of 126 tries (#3666, after M10). The items enum (the union over every
    # served method) is not published: the description is the list, and an
    # enum made a validating client refuse a `where` name the server serves
    # from the field holding it. Fields only a hidden method returns are in
    # the `surface` listing. A `select` naming a field one shape lacks is
    # served with the rest and a note (`select_vocabulary`).
    from okg.substrate.mcp.select_vocabulary import WHERE_NAMES_IN_ROWS

    fields = dict(capabilities.select_fields)
    listed = capabilities.with_select_fields(
        {method: fields[method] for method in shown if method in fields},
    ).select_fields_description()
    items = select.get("items")
    if isinstance(items, dict):
        items.pop("enum", None)
    in_shown = {name for m in shown for name in fields.get(m, ())}
    for other in served:
        extra = sorted(set(fields.get(other, ())) - in_shown)
        if other not in shown and extra:
            hidden.append(_line(f'search(method="{other}", select=...)', ", ".join(extra)))
    lexical = set(fields.get("lexical", ())) if "lexical" in shown else set()
    holders: dict[str, list[str]] = {}
    for name, target in WHERE_NAMES_IN_ROWS.items():
        if target in lexical:
            holders.setdefault(target, []).append(name)
    held = "; ".join(
        f"{' and '.join(names)} {'are' if len(names) > 1 else 'is'} in {target}"
        for target, names in holders.items()
    )
    # PACT read-passage-rows-3666 (D6): one record attribute is selected in
    # the `where` form; agents sent dotted names 83 times and were refused.
    dotted = 'One attribute: attrs.<key>, e.g. "attrs.status".' if listed else ""
    text = " ".join(
        part for part in (listed, f"{held}." if held else "", dotted) if part
    )
    if text:
        select["description"] = text
    else:
        select.pop("description", None)
    return shown


def publish_lean_read_surface(mcp: Any) -> int:
    """Narrow every registered read tool's published schema to its shown forms.

    `HIDDEN_NOTES` and every hidden shape are listed in full (no line is cut).

    The shown `inspect` kinds are `SHOWN_INSPECT_KINDS`, and on a write/admin
    launch also every write-side kind it serves (`admission`, `record`,
    `record_templates`, `surface`, ...): a write agent needs them. `expand`
    keeps `live` where the deployment declares a live relation. Everything
    hidden is listed, one line each, on `mcp._okg_lean_hidden_lines`, which
    the read-only `inspect(target={"kind": "surface"})` serves.

    Returns the bytes removed across the tools (as compact JSON).
    """
    import json

    from okg.substrate.mcp.effective_surface import (
        CEILING_INSPECT_KINDS,
        RECORD_INSPECT_KINDS,
        WRITE_ADMIN_INSPECT_KINDS,
    )
    from okg.substrate.mcp.operator_models import _unpublish

    manager = getattr(mcp, "_tool_manager", None)
    tools = getattr(manager, "_tools", None)
    if not isinstance(tools, dict):
        raise ToolSchemaUnpublishable(
            "could not reach the registered tools through the MCP SDK "
            "internals (_tool_manager._tools); refusing to publish the read "
            "tools' full surface unannounced",
            data={"has_tool_manager": manager is not None},
        )
    capabilities = getattr(mcp, "_okg_capabilities", None)
    shown_kinds = frozenset(SHOWN_INSPECT_KINDS)
    if getattr(mcp, "_okg_allow_writes", False):
        shown_kinds |= WRITE_ADMIN_INSPECT_KINDS | RECORD_INSPECT_KINDS | CEILING_INSPECT_KINDS
    live = bool(getattr(mcp, "_okg_live_bindings", None))
    shown_methods: list[str] = []
    published_kinds: list[str] = []
    hidden_lines: list[str] = [_line(
        "where",
        'also the long form {"op": "eq", "field": "<field>", "value": ...} and '
        'the old spellings predicate (filter), input.filter (filter, map, '
        'aggregate), filter (expand)',
    ), "dates: " + HIDDEN_NOTES["dates"]]
    removed = 0
    for name, hidden_names in HIDDEN_ARGUMENTS.items():
        tool = tools.get(name)
        if tool is None:
            continue
        schema = getattr(tool, "parameters", None)
        if not isinstance(schema, dict) or not isinstance(schema.get("properties"), dict):
            raise ToolSchemaUnpublishable(
                f"{name}: published schema has no properties object",
                data={"tool": name},
            )
        if name == "expand" and live:
            hidden_names = tuple(n for n in hidden_names if n != "live")
        before = len(json.dumps(schema, separators=(",", ":")))
        for argument in hidden_names:
            body = schema["properties"].get(argument)
            if isinstance(body, dict):
                shape = {k: v for k, v in body.items() if k != "description"}
                hidden_lines.append(_line(
                    f"{name}({argument}=...)", json.dumps(shape, separators=(",", ":")),
                ))
        _unpublish(schema, hidden_names)
        properties = schema["properties"]
        for argument, description in PUBLISHED_AGAIN.get(name, {}).items():
            body = properties.get(argument)
            if not isinstance(body, dict):
                raise ToolSchemaUnpublishable(
                    f"{name}.{argument} is to be published but the tool's "
                    "schema does not declare it",
                    data={"tool": name, "argument": argument},
                )
            if (name, argument) in UNTYPED:
                # Never required: `where` with a type stands for the set, so
                # a published call may leave `input` out.
                properties[argument] = {"description": description}
                if isinstance(schema.get("required"), list):
                    schema["required"] = [n for n in schema["required"] if n != argument]
                continue
            body = {k: v for k, v in body.items() if k not in ("description", "default")}
            properties[argument] = {**body, "description": description} if description else body
        if name == "search":
            method = properties.get("method") or {}
            extra = [m for m in method.get("enum") or () if m not in SHOWN_SEARCH_METHODS]
            if extra:
                hidden_lines.append(_line("search(method=...)", "also " + ", ".join(extra)))
            shown_methods = _narrow_search(schema, capabilities, hidden_lines)
            if "where" in properties:
                properties["where"] = where_schema("search")
        if name == "inspect":
            published_kinds = _narrow_inspect_target(schema, shown_kinds, hidden_lines)
        if name in WHERE_ADDED:
            # First among the arguments: it is the one every call names.
            schema["properties"] = {"where": where_schema(name), **properties}
            properties = schema["properties"]
        if name == "expand" and isinstance(properties.get("input"), dict):
            # A record id (the description says so). The `{"node_id": ...}`
            # object is the old spelling: still typed as an object, with its
            # fields unlisted, so a validating client does not refuse it.
            properties["input"] = {
                "type": ["string", "object"],
                "description": properties["input"].get("description"),
            }
            if properties["input"]["description"] is None:
                del properties["input"]["description"]
            hidden_lines.append(_line(
                "expand(input=...)", 'also {"node_id": "<id>"}, {"kind": "node", "node_id": ...}',
            ))
        if name == "map":
            properties["select"] = {"type": "array", "items": {"type": "string"}}
        if not schema.get("required"):
            schema.pop("required", None)
        removed += before - len(json.dumps(schema, separators=(",", ":")))
    mcp._okg_lean_hidden_lines = tuple(hidden_lines)
    _report(mcp, tools, shown_methods, published_kinds)
    return removed


def _report(
    mcp: Any, tools: dict[str, Any], shown_methods: list[str], shown_kinds: list[str],
) -> None:
    """Say in the surface report what `tools/list` shows and what it hides.

    The capability block's `published_methods` names what the server runs
    and accepts; the schema now shows fewer. Without this block a readback
    would claim the schema lists methods it no longer lists.
    """
    freshness = getattr(mcp, "_okg_surface_freshness", None)
    if not isinstance(freshness, dict):
        return
    freshness["lean_read_surface"] = {
        "shown_search_methods": list(shown_methods),
        "shown_inspect_kinds": list(shown_kinds),
        "hidden_arguments": {
            name: list(hidden) for name, hidden in HIDDEN_ARGUMENTS.items() if name in tools
        },
        "accepted_not_shown": list(getattr(mcp, "_okg_lean_hidden_lines", ())),
        "hidden_still_accepted": True,
    }
