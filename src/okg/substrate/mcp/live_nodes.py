"""One live node and its live edges: `inspect` and `expand` (#3666).

PACT ``live-graph-rucio-sites``.

The resolver (`live_resolver.resolve`) sits before every tool body at the
asynchronous call layer. For `inspect` and `expand` it hands the call here
when the call names a live node or a live type:

* ``inspect(target={"kind": "node", "node_id": "site:X"})`` makes the type's
  `exists` call, then one call per field group the call needs (the selected
  fields, or every declared field), and returns the node in the stored-node
  layout (`node_id`, `subtype`, `attrs`) with each field's state under
  `live`, the type's fields with what each costs, and its edges. An unknown
  name is `node_not_visible`, as is a gate deny (never "checked live"); an
  `exists` call that failed or was cut is "<System> unavailable".
* ``inspect(target={"kind": "node", "node_id": "system:example"})`` reads the
  declared node's fields (one call each) and lists its edges.
* ``inspect(target={"kind": "subtype", "subtype": "site"})`` describes the
  declaration with no call.
* ``expand(input="site:X", edge_type="<edge>", args={...})`` follows a
  declared live edge: one call, through the same gate and call samples, and
  returns the far-end typed nodes, a page at a time (`limit`, `offset`). A
  required `$arg` missing is refused with no call; a node id given for an
  `$arg` reaches the relation as its name. A condition (`filter`) is pushed
  to the system where the edge declares it, and the rest is applied in
  memory on columns the rows carry: the full listing is joined when the
  pushed rows lack one. The keep rule applies: a cut reply is kept, and
  page 2 is read from the kept set.

Nothing here touches the stored graph: the tool body only serves the reply
built here (:func:`served_node_reply`), inside its own byte budget.
"""
from __future__ import annotations

import difflib
import time
from collections import Counter
from dataclasses import dataclass
from typing import Any, Mapping, Optional

from okg.substrate.mcp import live_calls as _calls
from okg.substrate.mcp import live_resolver as _lr
from okg.substrate.mcp.live_calls import (
    as_mapping as _as_mapping,
    refused as _refused,
    system_word as _system_word,
)


@dataclass(frozen=True)
class NodeTarget:
    kind: str  # "node", "subtype" or "edge"
    live_type: Any
    node_id: Optional[str] = None
    parts: tuple[str, ...] = ()


def _node_id_of(value: Any) -> Optional[str]:
    value = _as_mapping(value)
    if isinstance(value, Mapping):
        value = value.get("node_id")
    return value if isinstance(value, str) and ":" in value else None


def detect_node(registry: Any, tool: str, arguments: Mapping[str, Any]) -> Optional[NodeTarget]:
    """The live node or type an `inspect`/`expand` call names, or None.

    A live prefix with a malformed name raises `live_node_id_invalid` here,
    before any call.
    """

    if tool == "inspect":
        target = _as_mapping(arguments.get("target"))
        if not isinstance(target, Mapping) or arguments.get("next") is not None:
            return None
        kind = target.get("kind")
        if kind == "subtype":
            live_type = registry.by_subtype(str(target.get("subtype") or ""))
            return None if live_type is None else NodeTarget("subtype", live_type)
        if kind != "node":
            return None
        node_id = _node_id_of(target.get("node_id"))
    elif tool == "expand":
        if arguments.get("next") is not None:
            return None
        node_id = _node_id_of(arguments.get("input"))
    else:
        return None
    if node_id is None or registry.by_prefix(node_id.split(":", 1)[0]) is None:
        return None
    live_type, parts = registry.parse_node_id(node_id)
    return NodeTarget("edge" if tool == "expand" else "node", live_type, node_id, tuple(parts))


async def resolve_node(server: Any, name: str, arguments: dict[str, Any]) -> Any:
    """Resolve an `inspect` or `expand` of a live node; None for a stored one."""

    registry = server._okg_live_types
    target = detect_node(registry, name, arguments)
    if target is None:
        return None
    live_type = target.live_type
    subtype = str(live_type.subtype)
    if target.kind == "subtype":
        surface = getattr(server, "_okg_agent_surface", None)
        served = getattr(surface, "live_types", None) if surface is not None else None
        if not served or subtype not in served:
            return None
        frame = declaration(registry, live_type)
        return arguments, _no_call(name, frame)

    arguments = _lr.without_run_generation(name, arguments)
    await _lr.check_caller(server, name, subtype)
    refusal = await _lr.check_arguments(server, name, subtype, arguments)
    if refusal is not None:
        return refusal
    holder = _calls.HOLDER.get()
    holder_key = _calls.canonical({
        "tool": name, "node": target.node_id, "edge": arguments.get("edge_type"),
        "args": _as_mapping(arguments.get("args")), "where": _as_mapping(arguments.get("filter")),
        "select": arguments.get("select"),
    })
    if holder is not None and holder_key in holder:
        held = holder[holder_key]
        return arguments, held
    if name == "expand":
        edge, caller, call_args = _edge_call(registry, target, arguments)
    prepared = await _lr.prepare_read(server, name, arguments, [subtype])
    reader = _calls.Reader(server, name, prepared, time.monotonic() + _calls.DEADLINE_SECONDS)
    try:
        if name == "expand":
            resolved = await _expand(
                reader, registry, target, edge, caller, call_args, arguments,
            )
        else:
            resolved = await _inspect(reader, registry, target, arguments)
    finally:
        reader.close()
    if holder is not None:
        holder[holder_key] = resolved
    return arguments, resolved


def _no_call(name: str, frame: dict[str, Any]) -> _calls.Resolved:
    import uuid

    return _calls.Resolved(
        tool=name, ref=f"{_calls.REF_PREFIX}{uuid.uuid4().hex[:20]}", rows=[],
        fields=frozenset(), state={"frame": frame, "keepable": False}, calls_made=0,
    )


# -- what the declaration says, with no call ---------------------------------------


def _noun(registry: Any, live_type: Any) -> str:
    found = registry.edge_set(live_type.subtype) if live_type.system else None
    name = str(found[1].name) if found else str(live_type.subtype)
    return name[:-1] if name.endswith("s") and len(name) > 1 else name


def _field_costs(registry: Any, live_type: Any, declared_id: Optional[str]) -> dict[str, str]:
    """What reading each field costs (`static_total: 1 upstream call per site`)."""

    if live_type.declared_only:
        system = _system_word(declared_id)
        calls = live_type.declared_fields.get(str(declared_id), {})
        return {name: f"1 {system} call ({call.relation})" for name, call in sorted(calls.items())}
    system = _system_word(live_type.system)
    noun = _noun(registry, live_type)
    by_call: dict[Any, list[str]] = {}
    for name, fld in sorted((live_type.fields or {}).items()):
        by_call.setdefault(fld.call, []).append(name)
    costs: dict[str, str] = {}
    for call, names in by_call.items():
        for name in names:
            shared = [other for other in names if other != name]
            costs[name] = f"1 {system} call per {noun} ({call.relation})" + (
                f"; the same call fills {', '.join(shared)}" if shared else ""
            )
    if live_type.open_fields is not None:
        costs["attrs.<any other key>"] = (
            f"1 {system} call per {noun} for all of them "
            f"({live_type.open_fields.relation})"
        )
    if live_type.exists is not None:
        costs["exists"] = (
            f"1 {system} call per {noun} ({live_type.exists.relation}); its "
            "columns come with it"
        )
    return costs


def _edges(live_type: Any, node_id: Optional[str], declared_id: Optional[str]) -> list[dict[str, Any]]:
    system = _system_word(declared_id if live_type.declared_only else live_type.system)
    edges = live_type.edges_of(str(declared_id)) if live_type.declared_only else live_type.edges
    out: list[dict[str, Any]] = []
    for name, edge in sorted(edges.items()):
        caller = {arg.value: ("required" if arg.required else "optional")
                  for arg in edge.call.caller_args()}
        entry: dict[str, Any] = {
            "edge_type": name, "to": edge.to, "cost": f"1 {system} call ({edge.call.relation})",
        }
        if caller:
            entry["args"] = caller
        if edge.push_down is not None:
            entry["conditions"] = f"eq / in on the listing's columns are applied by {system}"
        if node_id is not None:
            call: dict[str, Any] = {"input": node_id, "edge_type": name}
            if caller:
                call["args"] = {key: f"<{edge.to} id or name>" for key in caller}
            entry["call"] = {"tool": "expand", "arguments": call}
        out.append(entry)
    return out


def declaration(registry: Any, live_type: Any) -> dict[str, Any]:
    """`inspect(target={"kind": "subtype", ...})` of a live type: no call."""

    frame: dict[str, Any] = {
        "status": "ok",
        "subtype": live_type.subtype,
        "live": True,
        "id_prefix": live_type.id_prefix,
    }
    if live_type.declared_only:
        frame["nodes"] = [live_type.node_id(d) for d in live_type.declared_ids]
        frame["fields"] = {
            live_type.node_id(d): _field_costs(registry, live_type, d)
            for d in live_type.declared_ids
        }
        frame["edges"] = {
            live_type.node_id(d): _edges(live_type, None, d) for d in live_type.declared_ids
        }
    else:
        frame["system"] = live_type.system
        frame["name_parts"] = list(live_type.id_from)
        if live_type.name_pattern:
            frame["name_pattern"] = live_type.name_pattern
        found = registry.edge_set(live_type.subtype)
        if found is not None:
            owner, edge = found
            frame["set"] = (
                f"{owner.id_prefix}:{edge.of} -> {edge.name}: a set read on type "
                f"{live_type.subtype!r} reads this listing"
            )
            if edge.member_columns:
                frame["listing_columns"] = list(edge.member_columns)
        frame["fields"] = _field_costs(registry, live_type, None)
        frame["edges"] = _edges(live_type, None, None)
    frame["calls_made"] = 0
    return frame


# -- inspect one live node ------------------------------------------------------------


def _select_keys(select: Any) -> tuple[Optional[set[str]], bool]:
    """(the `attrs.<key>` names selected, or None for the default; whether
    `attrs` as a whole was selected)."""

    if not isinstance(select, (list, tuple)):
        return None, False
    whole = "attrs" in select
    keys = {key for key in (_calls.field_key(name) for name in select) if key is not None}
    return (keys or None), whole


async def _inspect(reader: Any, registry: Any, target: NodeTarget, arguments: Mapping[str, Any]) -> Any:
    from okg.substrate.external_backed import live_dispatch as _ld
    from okg.substrate.mcp import live_result_sets as _lrs
    from okg.substrate.mcp.tools import node_not_visible

    live_type, node_id = target.live_type, str(target.node_id)
    declared_id = target.parts[0] if live_type.declared_only else None
    system = declared_id if live_type.declared_only else live_type.system
    started = _calls.now_text()
    keys, whole = _select_keys(arguments.get("select"))
    attrs: dict[str, Any] = {}
    states: dict[str, str] = {}
    relation_label = live_type.subtype
    if live_type.declared_only:
        calls = live_type.declared_fields.get(str(declared_id), {})
        wanted = {k: c for k, c in calls.items() if keys is None or whole or k in keys}
        unknown = sorted((keys or set()) - set(calls))
        if unknown and not whole:
            raise _unknown_field(node_id, unknown[0], sorted(calls))
        for fname, call in sorted(wanted.items()):
            outcome = await reader.gate(call.relation, node_id)
            if not outcome.allowed:
                states[fname] = "denied"
                continue
            made = await reader.calls(call.relation, [call.bind()], None, [node_id])
            reader.fetch.per_node[fname] = reader.fetch.per_node.get(fname, 0) + made.started
            result = made.outcomes[0]
            if result in (_ld.VALUE_OK, _ld.VALUE_CUT):
                rows = list(made.rows)
                attrs[fname] = rows[0] if len(rows) == 1 else rows
                states[fname] = "live"
            elif result in (_ld.VALUE_EMPTY, _ld.VALUE_NOT_FOUND):
                states[fname] = "absent"
            else:
                states[fname] = "unavailable"
            relation_label = call.relation
    else:
        name_parts = live_type.name_parts(target.parts)
        if live_type.exists is not None:
            relation = live_type.exists.relation
            relation_label = relation
            outcome = await reader.gate(relation, node_id)
            if not outcome.allowed:
                # The same answer as absence: a deny never says "checked live".
                raise node_not_visible(node_id, reader.prepared.generation)
            made = await reader.calls(
                relation, [live_type.exists.bind(name_parts)], None, [node_id],
            )
            reader.fetch.per_node["exists"] = made.started
            result = made.outcomes[0]
            if result in (_ld.VALUE_NOT_FOUND, _ld.VALUE_EMPTY):
                raise node_not_visible(node_id, reader.prepared.generation)
            if result != _ld.VALUE_OK:
                raise _refused(
                    f"inspect: {node_id}: {_system_word(system)} unavailable; its "
                    f"existence check gave no whole answer "
                    f"({made.reasons[0] or result})",
                    "system_unavailable",
                    "send the call again later; this is not an answer that the "
                    "node does not exist",
                    node_id=node_id, relation=relation, outcome=result,
                )
            for row in made.rows[:1]:
                for key, value in row.items():
                    attrs[key] = value
                    states[key] = "live"
        declared = dict(live_type.fields or {})
        if keys is None and not whole:
            wanted = set(declared)
        else:
            wanted = set(declared) if whole else set()
            wanted |= {k for k in (keys or set()) if k not in attrs}
            if whole and live_type.open_fields is not None:
                wanted.add("\x00open")
        open_key = "\x00open" in wanted
        wanted.discard("\x00open")
        groups = _calls.plan_groups("inspect", live_type, wanted, set(attrs))
        if open_key and live_type.open_fields is not None:
            group_key = _calls.open_group_key(live_type.open_fields, "inspect")
            if group_key not in groups:
                groups[group_key] = _calls.group_for(
                    "inspect", live_type.open_fields, "attributes", None,
                )
        row = {
            "node_id": node_id, "subtype": live_type.subtype, "attrs": attrs,
            "live": {"system": system, "observed_at": started, "fields": states},
        }
        await _calls.fetch_groups(reader, groups, [row], node_id, declared)
        attrs, states = row["attrs"], row["live"]["fields"]
    finished = _calls.now_text()
    unread = [k for k, s in states.items() if s in ("unavailable", "denied")]
    coverage = {
        "listed": 1, "read": 0 if unread else 1, "not_read": [node_id] if unread else [],
        "calls": reader.fetch.calls, "listing_calls": 0,
        "per_node_calls": dict(reader.fetch.per_node),
        "waited_seconds": round(reader.fetch.waited_seconds, 1), "kept": False,
    }
    line = _calls.live_line(
        system=system, started=started, finished=finished,
        label=_noun(registry, live_type) if not live_type.declared_only else "node",
        coverage=coverage,
    )
    line = _calls.with_tail(line, "one node, not kept")
    fenced = _lrs.fence_rows([attrs], relation_label)[0] if attrs else {}
    # Upstream keys are served neutralised, in the field states as in attrs.
    served_keys = dict(zip((str(k) for k in attrs), fenced))
    frame: dict[str, Any] = {
        "status": "ok",
        "node_id": node_id,
        "subtype": live_type.subtype,
        "attrs": fenced,
        "live": _lrs.served_live(
            {"system": system, "observed_at": finished, "fields": dict(states)}, served_keys,
        ),
        "fields": _field_costs(registry, live_type, declared_id),
        "edges": _edges(live_type, node_id, declared_id),
        "trust": "untrusted",
        "calls_made": reader.fetch.calls,
    }
    return _calls.Resolved(
        tool="inspect", ref=f"{_calls.REF_PREFIX}{node_id}", rows=[],
        fields=frozenset(f"attrs.{k}" for k in attrs),
        state={"frame": frame, "keepable": False, "live_line": line, "coverage": coverage},
        calls_made=reader.fetch.calls,
    )


def _unknown_field(node_id: str, key: str, known: list[str]) -> Exception:
    close = difflib.get_close_matches(key, known, n=3)
    return _refused(
        f"inspect: {node_id} has no field {key!r}"
        + (f"; did you mean {', '.join('attrs.' + c for c in close)}?" if close else ""),
        "unknown_field", f"its fields: {', '.join('attrs.' + k for k in known)}",
        field=f"attrs.{key}", closest=close,
    )


# -- expand a live edge -----------------------------------------------------------------


def _edge_call(registry: Any, target: NodeTarget, arguments: Mapping[str, Any]) -> tuple[Any, dict[str, Any], dict[str, Any]]:
    """The declared edge, the caller's `$arg` values and the relation's
    arguments; every refusal here comes before any call."""

    live_type, node_id = target.live_type, str(target.node_id)
    declared_id = target.parts[0] if live_type.declared_only else None
    edges = live_type.edges_of(str(declared_id)) if live_type.declared_only else live_type.edges
    edge_type = arguments.get("edge_type")
    names = sorted(edges)
    for argument, refused_value in (
        ("path", None), ("target_subtype", None), ("continuation_token", None),
    ):
        if arguments.get(argument) not in (refused_value, [], ""):
            raise _refused(
                f"expand: {node_id} is a live node; `{argument}` reads the stored graph",
                "argument_not_live", f"follow one live edge: edge_type one of {names}",
                argument=argument,
            )
    if arguments.get("direction") not in (None, "out") or int(arguments.get("depth") or 1) != 1:
        raise _refused(
            f"expand: {node_id} is a live node; its edges are followed out, one hop",
            "argument_not_live", "drop direction and depth", edges=names,
        )
    if not isinstance(edge_type, str) or not edge_type:
        raise _refused(
            f"expand: {node_id} is a live node; name the edge to follow",
            "edge_type_required",
            f"pass edge_type, one of {names}" if names else "this node has no live edges",
            edges=names,
        )
    edge = edges.get(edge_type)
    if edge is None:
        close = difflib.get_close_matches(edge_type, names, n=3)
        raise _refused(
            f"expand: {node_id} has no live edge {edge_type!r}"
            + (f"; did you mean {', '.join(close)}?" if close else ""),
            "unknown_edge", f"its edges: {', '.join(names) or 'none'}",
            edge_type=edge_type, edges=names, closest=close,
        )
    given = _as_mapping(arguments.get("args")) or {}
    if not isinstance(given, Mapping):
        raise _refused(
            "expand: `args` is an object of the edge's arguments",
            "args_not_an_object", 'pass args={"<name>": <value>}',
        )
    takes = {arg.value for arg in edge.call.caller_args()}
    unknown = sorted(set(given) - takes)
    if unknown:
        raise _refused(
            f"expand: edge {edge_type!r} takes no argument {unknown[0]!r}"
            + (f"; it takes {sorted(takes)}" if takes else "; it takes none"),
            "unknown_edge_argument", "drop it, or name one the edge takes",
            argument=unknown[0], takes=sorted(takes),
        )
    caller = {key: registry.caller_arg_value(edge, value) for key, value in given.items()}
    call_args = edge.call.bind(
        {} if live_type.declared_only else live_type.name_parts(target.parts), caller,
    )
    return edge, {key: given[key] for key in given}, call_args


def _far_id(
    far: Any, row: Mapping[str, Any], caller: Mapping[str, Any], edge: Any,
) -> tuple[Optional[str], Optional[str]]:
    """(the far-end node id, or None; the upstream name refused, or None).

    A name built from the row's columns must pass the far type's
    ``name_pattern``; one that does not is never made a node."""

    if far.id_from and all(row.get(col) not in (None, "") for col in far.id_from):
        parts = [str(row[col]) for col in far.id_from]
        name = ":".join(parts)
        if not _calls.name_checked(far, name):
            return None, name
        return far.node_id(*parts), None
    # The far end named by the caller (`link`: args={"to": "site:B"}),
    # already checked as a node id before any call.
    named = [
        value for value in caller.values()
        if isinstance(value, str) and value.startswith(f"{far.id_prefix}:")
    ]
    return (named[0] if len(named) == 1 else None), None


def _join_key(far: Any, row: Mapping[str, Any]) -> Optional[tuple[str, ...]]:
    if not far.id_from or any(row.get(col) in (None, "") for col in far.id_from):
        return None
    return tuple(str(row[col]) for col in far.id_from)


async def _expand(
    reader: Any, registry: Any, target: NodeTarget, edge: Any,
    caller: Mapping[str, Any], call_args: dict[str, Any], arguments: Mapping[str, Any],
) -> Any:
    from okg.substrate.mcp import live_reader

    live_type, node_id = target.live_type, str(target.node_id)
    declared_id = target.parts[0] if live_type.declared_only else None
    system = edge.of or declared_id or live_type.system
    far = registry.by_subtype(edge.to)
    started = _calls.now_text()
    condition = _as_mapping(arguments.get("filter"))
    relation, args, applied = edge.call.relation, dict(call_args), []
    empty_error: tuple[str, ...] = ()
    join_full = False
    rest_keys: set[str] = set()
    if condition not in (None, {}, True):
        _lr.refuse_unsupported_ops("expand", condition)
        push = _lr.plan_push("expand", edge, far, condition)
        columns = set(edge.member_columns or ()) | set(far.id_from or ())
        rest_keys = _lr.condition_keys(push.rest)
        if push.rest is not None and (not edge.member_columns or not rest_keys <= columns):
            raise _refused(
                f"expand: a condition on the far end of {edge.name!r} names a field "
                "its listing does not carry; it would be read as absent",
                "condition_needs_fields",
                f'read the set with filter(where={{"type": "{edge.to}", ...}}), '
                "which fetches the fields a condition names",
                fields=sorted(f"attrs.{k}" for k in rest_keys - columns),
            )
        if push.expression is not None:
            pd = edge.push_down
            relation = pd.relation
            declared_args = {a.name for a in reader.prepared.bound(pd.relation).relation.args}
            args = {pd.arg: push.expression, **{k: v for k, v in call_args.items() if k in declared_args}}
            applied = list(push.applied)
            # The set read passes these too (#3666 review minor 1): an empty
            # pushed expression is zero rows, not an unavailable system.
            empty_error = tuple(pd.empty_error or ())
            # The pushed rows carry only the push-down's own columns: a
            # remaining condition on any other listing column joins the full
            # listing (one call), as a set read does.
            pushed_columns = set(pd.member_columns or ()) | set(far.id_from or ())
            join_full = push.rest is not None and not rest_keys <= pushed_columns
        condition = push.rest
    rows, cut = await _calls.listing(
        reader, relation, args, node_id, edge.name, system=system, empty_error=empty_error,
    )
    full_cut = False
    by_key: dict[tuple[str, ...], Mapping[str, Any]] = {}
    if join_full:
        full_rows, full_cut = await _calls.listing(
            reader, edge.call.relation, dict(call_args), node_id, edge.name, system=system,
        )
        cut = cut or full_cut
        for full in full_rows:
            key = _join_key(far, full)
            if key is not None:
                by_key.setdefault(key, full)
    out: list[dict[str, Any]] = []
    unnamed = 0
    names_refused: list[str] = []
    for row in rows:
        far_id, bad_name = _far_id(far, row, caller, edge)
        if bad_name is not None:
            names_refused.append(bad_name)
            continue
        if far_id is None:
            unnamed += 1
            continue
        attrs = dict(row)
        states = {key: "live" for key in row}
        if join_full:
            found = by_key.get(_join_key(far, row) or ())
            if found is not None:
                attrs = {**dict(found), **attrs}
                states = {key: "live" for key in attrs}
            for key in rest_keys - set(attrs):
                # A node the cut full listing did not reach was not read.
                states[key] = "unavailable" if (found is None and full_cut) else "absent"
        out.append({
            "node_id": far_id, "subtype": far.subtype, "attrs": attrs,
            "live": {"system": system, "observed_at": started, "fields": states},
        })
    listed = len(out)
    not_comparable: Counter[str] = Counter()
    condition_not_read = live_reader.NotRead()
    unread: list[str] = []
    if condition not in (None, {}, True):
        out, unread = live_reader.select_rows(  # type: ignore[assignment]
            out, condition, condition_not_read, not_comparable,
        )
    # Whether the kept rows are the edge's whole listing: nothing pushed, no
    # cut, every row a node, no condition dropped one. A later read over
    # them refuses a key no row carries as a probable typo only then.
    whole = not applied and not cut and not names_refused and not unnamed and len(out) == listed
    finished = _calls.now_text()
    coverage = {
        "listed": listed, "read": listed - len(unread), "not_read": unread,
        "not_comparable": sum(not_comparable.values()), "cut": cut,
        "cuts": list(reader.fetch.cut), "applied_by_system": applied,
        "calls": reader.fetch.calls, "listing_calls": reader.fetch.listing_calls,
        "per_node_calls": {}, "outcomes": {k: dict(v) for k, v in reader.fetch.outcomes.items()},
        "waited_seconds": round(reader.fetch.waited_seconds, 1), "kept": False,
        "names_refused": len(names_refused),
        **({"rows_without_a_name": unnamed} if unnamed else {}),
    }
    line = _calls.live_line(
        system=system, started=started, finished=finished, label=str(edge.name),
        coverage=coverage,
    )
    resolved = _lr.resolved_read(
        reader, name="expand", live_type=far, system=system, rows=out,
        coverage=coverage, line=line, finished=finished, listing_relation=relation,
        query=args, node_id=node_id,
        fields=frozenset(f"attrs.{k}" for row in out for k in row["attrs"]),
        narrowing=[f"attrs.{k}" for k in (edge.member_columns or ()) if k not in far.id_from],
        ids_checked=far.name_pattern is not None,
        known=_lr.declared_keys(far, edge), whole=whole,
    )
    _lr.note_conditions(resolved.state, condition_not_read, not_comparable, names_refused)
    names = _lr.names_refused_block(resolved.state)
    resolved.state["frame"] = {
        "status": "ok",
        "node_id": node_id,
        "edge_type": edge.name,
        "direction": "out",
        "to": far.subtype,
        "trust": "untrusted",
        **({"rows_without_a_name": unnamed} if unnamed else {}),
        **({"names_refused": names} if names is not None else {}),
        **({"not_read": _not_read_report(condition_not_read, relation)}
           if condition_not_read.count else {}),
    }
    return resolved


def _not_read_report(not_read: Any, relation: str) -> dict[str, Any]:
    from okg.substrate.mcp import live_result_sets as _lrs

    report = dict(not_read.report() or {})
    # Node ids are built from upstream names: served as untrusted text.
    report["node_ids"] = _lrs.fence_rows(report.get("node_ids") or [], relation)
    return report


# -- the tool body's side -------------------------------------------------------------------


def served_node_reply(operator: str, *, limit: Any = None, offset: Any = 0) -> Optional[dict[str, Any]]:
    """The reply of a live `inspect`/`expand` resolved for this call, or
    None. Called first thing in the tool body, inside its draft scope, so a
    byte-fitting rerun (a smaller `limit`) re-pages the same rows."""

    resolved = _calls.HANDOFF.get()
    if resolved is None or resolved.tool != operator:
        return None
    from okg.substrate.mcp import live_result_sets as _lrs
    from okg.substrate.mcp import result_references

    memo = result_references.call_memo() or {}
    state = memo.get("live-read:" + resolved.ref) or resolved.state
    frame = state.get("frame")
    if not isinstance(frame, Mapping):
        return None
    payload = dict(frame)
    if operator == "expand":
        rows = list(state.get("rows") or [])
        page = int(limit) if isinstance(limit, int) and not isinstance(limit, bool) else _lr.DEFAULT_PAGE
        page = max(1, page)
        off = int(offset) if isinstance(offset, int) and not isinstance(offset, bool) else 0
        page_rows = rows[off: off + page]
        kept_set = state.get("set")
        relation = getattr(kept_set, "relation_name", None) or "live"
        checked = bool(getattr(kept_set, "ids_checked", False))
        raw = [row["attrs"] for row in page_rows]
        served = _lrs.fence_rows(raw, relation)
        payload["neighbors"] = [
            {"node_id": _lrs.served_node_id(row["node_id"], relation, checked=checked),
             "subtype": row["subtype"], "attrs": attrs,
             "live": _lrs.served_live(
                 row["live"], dict(zip((str(k) for k in row["attrs"]), attrs)),
             )}
            for row, attrs in zip(page_rows, served)
        ]
        truncated = off + page < len(rows)
        payload.update({
            "total_count": len(rows), "total_count_basis": "exact",
            "returned": len(page_rows), "limit": page, "offset": off,
            "truncated": truncated,
        })
        fields = _lr.fresh_served_fields(state) or {}
        if fields.get("result_reference"):
            payload["result_reference"] = fields["result_reference"]
            if truncated:
                payload["next_call"] = {
                    "tool": "filter",
                    "arguments": {"input": {"ref": state["kept_id"]}, "offset": off + page},
                }
        elif "kept" in fields:
            payload["kept"] = fields["kept"]
    return _lr.with_live_line(payload, {**state, "fresh": True})


__all__ = [
    "NodeTarget",
    "declaration",
    "detect_node",
    "resolve_node",
    "served_node_reply",
]
