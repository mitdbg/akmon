"""Live types: node types whose members, fields and edges are read live.

Issue #3666, PACT ``live-graph-rucio-sites``.

WHAT THIS MODULE IS
-------------------
A live type is an ordinary node type (``site``, ``system``) that is never
stored. Its declaration says, once per type, which declared live relation
fills each field and follows each edge. This module owns the declaration
model, its parser and the registry the read tools consult. It never calls an
adapter and never opens a database connection.

The declaration is the class annotation ``okg_live_type``, next to the
``okg_live_relations`` it names::

    Site:
      annotations:
        okg_live_type:
          id: {prefix: site, from: [site]}
          system: example
          name_pattern: '^S[0-3]_[A-Z]{2}_[A-Za-z0-9_-]+$'
          mention_pattern: '\\bS[0-3]_[A-Z]{2}_[A-Za-z0-9_-]+\\b'
          fields:
            static_total: {relation: example_get_usage_by_source,
                           args: {site: $self.site, source: static},
                           column: total, type: integer}
          open_fields: {relation: example_list_site_attributes, args: {site: $self.site}}
          exists: {relation: example_get_site, args: {site: $self.site}}
          edges:
            link: {relation: example_get_link,
                   args: {source_site: $self.site,
                          destination_site: {value: $arg.to, required: true}},
                   to: site}

ARGUMENT VALUES
---------------
- ``$self.<col>``: a name part of the node being read; ``<col>`` must be one
  of ``id.from``.
- ``$arg.<name>``: a value the caller passes when following an EDGE. Written
  ``{value: $arg.<name>, required: true}`` when the caller must give it. A
  relation argument that is itself required and is filled from ``$arg`` must
  be marked required here; otherwise a caller who omits it would reach the
  upstream call with no value, and the refusal would come after a decision
  instead of before any call.
- anything else is a fixed value, checked at load against the relation's own
  argument model (type, enum, bounds, pattern). A fixed string starting with
  ``$`` is refused as a probable typo of the two forms above.

Every relation a declaration names must be declared on the SAME subtype, and
every argument name must be a declared argument of that relation.

LISTING COLUMNS
---------------
An edge whose far-end rows carry the far-end nodes' own fields says so with
``member_fields: from_rows``, and may name the columns its relation yields
with ``member_columns``. A ``push_down`` block names its own
``member_columns``: a system's pushed listing (``example_list_sites``) may
yield only ``site``, while its base listing (``example_list_all_sites``)
yields many, so a
planner joins a pushed listing with one base listing call when it needs a
listing column.

PUSH-DOWN KEYS
--------------
A ``push_down`` block names, in ``keys``, the field keys the system's own
expression can test (for example site attributes such as ``kind``,
``level`` and ``flag``). Only a condition on one of these keys, or on ``node_id``, is
sent to the system; every other condition is applied in memory after the
fields it names are fetched. A key that is a declared per-node field of the
far-end type is refused at load: the system's expression cannot read it.
A pushed value may hold only letters, digits, ``_``, ``.`` and ``-``.
The ``join`` block must declare ``and`` and ``or``; they have no default.
A ``push_down`` block may list, in ``empty_error``, substrings of the
system's own error text that mean "the expression selected nothing"; a
failure holding one is read as zero rows. Without it every failure stays a
failure.

Nothing about a live node is stored: the registry is built from the catalog
at server start, and every value comes from an upstream call at read time.
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any, Iterable, Literal, Mapping

from okg.substrate.errors import SubstrateError

#: The class annotation a live type is declared under.
ANNOTATION_KEY = "okg_live_type"

#: The key the compiled declaration is stored under in
#: ``okg.subtypes.metadata``.
METADATA_KEY = "live_type"

#: Value types a declared field may take. Upstream values are coerced to the
#: declared type before any comparison.
FIELD_TYPES = frozenset({"string", "integer", "number", "boolean"})

#: Push-down atom shapes and the placeholders each template must carry.
PUSH_DOWN_ATOMS: Mapping[str, tuple[str, ...]] = {
    "eq": ("{key}", "{value}"),
    "in": ("{key}",),
    "node_id_eq": ("{name}",),
}

#: Push-down joins a template set may name.
PUSH_DOWN_JOINS = frozenset({"and", "or", "and_not"})

#: The only characters a pushed value or node name may hold. An allowlist:
#: anything else could be an operator of the system's expression grammar
#: (``& | \\ ( ) = * < > !``, whitespace) and widen the set read.
PUSH_VALUE_CHARS = re.compile(r"[A-Za-z0-9_.\-]+")


def name_matches(pattern: str | None, name: Any) -> bool:
    """Whether `name` may be a node name under a type's ``name_pattern``:
    the one rule for a node id a caller passes and a name a listing returns.

    A full match: an anchored ``$`` alone would also accept a name ending in
    a newline. No pattern accepts any non-empty text.
    """

    if not isinstance(name, str) or not name:
        return False
    return pattern is None or re.fullmatch(pattern, name) is not None

#: The only accepted ``member_fields`` value.
MEMBER_FIELDS_FROM_ROWS = "from_rows"

#: Where one argument of a live call gets its value.
#: - ``self``:  a name part of the node being read (``$self.site``);
#: - ``fixed``: a literal from the declaration (``source: static``);
#: - ``arg``:   a value the caller passes when following an edge (``$arg.to``).
ArgKind = Literal["self", "fixed", "arg"]

_TOKEN = re.compile(r"[a-z][a-z0-9_]{0,63}")
_SELF = "$self."
_ARG = "$arg."

_TYPE_KEYS = frozenset({
    "id", "system", "name_pattern", "mention_pattern", "fields",
    "open_fields", "exists", "edges",
})
_ID_KEYS = frozenset({"prefix", "from", "declared"})
_CALL_KEYS = frozenset({"relation", "args"})
_FIELD_KEYS = frozenset({"relation", "args", "column", "type"})
_DECLARED_FIELD_KEYS = frozenset({"relation", "args", "of"})
_EDGE_KEYS = frozenset({
    "relation", "args", "to", "member_fields", "member_columns", "push_down",
    "of",
})
_PUSH_DOWN_KEYS = frozenset({
    "relation", "arg", "atoms", "join", "member_columns", "keys", "empty_error",
})
_ARG_FORM_KEYS = frozenset({"value", "required"})


class LiveTypeInvalid(SubstrateError):
    """A live-type declaration this substrate cannot honour.

    Every refusal is raised at catalog composition or at server start, never
    at read time, and names the type, the key and the relation involved.
    The two read-time subclasses below are the exception, and say so.
    """

    code = "live_type_invalid"


class LiveNodeIdInvalid(LiveTypeInvalid):
    """READ time: a node id with a live prefix whose name is malformed.

    Also raised for a caller argument that names a node of the wrong type
    (``args={"to": "item:example"}`` on an edge whose far end is ``site``).
    Refused before any upstream call.
    """

    code = "live_node_id_invalid"


class LiveArgMissing(LiveTypeInvalid):
    """READ time: a required ``$arg`` the caller did not give.

    Refused before any upstream call.
    """

    code = "live_arg_missing"


@dataclass(frozen=True)
class LiveArg:
    """One argument of a declared live call."""

    name: str
    kind: ArgKind
    #: ``self``: the id column; ``fixed``: the literal; ``arg``: the caller's key.
    value: Any
    #: Only for ``arg``: the call is refused, with zero upstream calls, without it.
    required: bool = False

    def to_metadata(self) -> Any:
        if self.kind == "self":
            return f"{_SELF}{self.value}"
        if self.kind == "arg":
            ref = f"{_ARG}{self.value}"
            return {"value": ref, "required": True} if self.required else ref
        return self.value


@dataclass(frozen=True)
class LiveCall:
    """One declared live relation and how its arguments are filled.

    Two calls with the same relation and arguments are EQUAL, so fields that
    share a relation and arguments share one call per node: group by the ``LiveCall``.
    """

    relation: str
    args: tuple[LiveArg, ...] = ()

    def caller_args(self) -> tuple[LiveArg, ...]:
        """The ``$arg`` arguments, in name order."""
        return tuple(arg for arg in self.args if arg.kind == "arg")

    def bind(
        self,
        name_parts: Mapping[str, str] | None = None,
        caller_args: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        """The relation arguments for one call.

        ``name_parts`` maps each ``id.from`` column to the node's value;
        ``caller_args`` holds the caller's ``$arg`` values, already stripped
        of any node-id prefix (see :meth:`LiveTypeRegistry.caller_arg_value`).
        An optional ``$arg`` the caller left out is omitted, so the
        relation's declared default applies. A required one raises
        :class:`LiveArgMissing`. The result is validated by the dispatcher's
        own argument model, as every live call is.
        """
        parts = name_parts or {}
        given = caller_args or {}
        out: dict[str, Any] = {}
        for arg in self.args:
            if arg.kind == "fixed":
                out[arg.name] = arg.value
            elif arg.kind == "self":
                if arg.value not in parts:
                    raise LiveNodeIdInvalid(
                        f"live relation {self.relation!r} argument "
                        f"{arg.name!r} takes the node's name part "
                        f"{arg.value!r}, which was not given.",
                        data={"relation": self.relation, "arg": arg.name,
                              "column": arg.value},
                    )
                out[arg.name] = parts[arg.value]
            else:
                if arg.value in given and given[arg.value] is not None:
                    out[arg.name] = given[arg.value]
                elif arg.required:
                    raise LiveArgMissing(
                        f"live relation {self.relation!r} needs the caller "
                        f"argument {arg.value!r} (for {arg.name!r}); it was "
                        f"not given, so no call was made.",
                        data={"relation": self.relation, "arg": arg.value,
                              "relation_arg": arg.name},
                        fix_hint=f"pass args={{{arg.value!r}: ...}}",
                    )
        return out

    def to_metadata(self) -> dict[str, Any]:
        body: dict[str, Any] = {"relation": self.relation}
        if self.args:
            body["args"] = {arg.name: arg.to_metadata() for arg in self.args}
        return body


@dataclass(frozen=True)
class LiveField:
    """A field of a live node, filled by one call per node."""

    name: str
    call: LiveCall
    column: str
    type: str = "string"

    def to_metadata(self) -> dict[str, Any]:
        return {**self.call.to_metadata(), "column": self.column,
                "type": self.type}


@dataclass(frozen=True)
class PushDown:
    """How leaf conditions become one argument of a listing relation.

    ``atoms`` maps a condition shape (``eq``, ``in``, ``node_id_eq``) to a
    template; ``join`` maps ``and``/``or``/``and_not`` to the system's
    operators. ``keys`` names the field keys the expression can test: a
    condition on any other key is never pushed. A pushed value that is not
    made of :data:`PUSH_VALUE_CHARS` is refused.
    """

    relation: str
    arg: str
    atoms: Mapping[str, str]
    join: Mapping[str, str]
    #: The columns the pushed listing's rows carry (``("site",)`` for a
    #: ``example_list_sites`` relation). Empty when the declaration does not say.
    member_columns: tuple[str, ...] = ()
    #: The field keys the system's expression can test. Empty: only a
    #: ``node_id`` condition is pushed.
    keys: tuple[str, ...] = ()
    #: Substrings of the system's error text that mean "the expression
    #: selected nothing" (matched case-insensitively). A failure whose text
    #: holds one is read as zero rows. Empty: no failure is read as empty.
    empty_error: tuple[str, ...] = ()

    def to_metadata(self) -> dict[str, Any]:
        body: dict[str, Any] = {
            "relation": self.relation, "arg": self.arg,
            "atoms": dict(sorted(self.atoms.items())),
            "join": dict(sorted(self.join.items())),
        }
        if self.member_columns:
            body["member_columns"] = list(self.member_columns)
        if self.keys:
            body["keys"] = list(self.keys)
        if self.empty_error:
            body["empty_error"] = list(self.empty_error)
        return body


@dataclass(frozen=True)
class LiveEdge:
    """An edge of a live node whose far end is nodes of type ``to``."""

    name: str
    call: LiveCall
    to: str
    #: The listing's own columns become fields of each far-end node.
    member_fields: bool = False
    push_down: PushDown | None = None
    #: For system nodes: which declared system this edge belongs to.
    of: str | None = None
    #: The columns the edge relation's rows carry. Empty when the declaration
    #: does not say. Only declared with ``member_fields: from_rows``.
    member_columns: tuple[str, ...] = ()

    def to_metadata(self) -> dict[str, Any]:
        body: dict[str, Any] = {**self.call.to_metadata(), "to": self.to}
        if self.member_fields:
            body["member_fields"] = MEMBER_FIELDS_FROM_ROWS
        if self.member_columns:
            body["member_columns"] = list(self.member_columns)
        if self.push_down is not None:
            body["push_down"] = self.push_down.to_metadata()
        if self.of is not None:
            body["of"] = self.of
        return body


@dataclass(frozen=True)
class LiveType:
    """One live type, as declared and checked."""

    subtype: str
    id_prefix: str
    #: Row columns that make a node's name, joined by ``:`` (``did`` is
    #: ``[scope, name]``). Empty for declared-only types (``system``).
    id_from: tuple[str, ...]
    system: str | None = None
    name_pattern: str | None = None
    mention_pattern: str | None = None
    fields: Mapping[str, LiveField] = field(default_factory=dict)
    open_fields: LiveCall | None = None
    exists: LiveCall | None = None
    edges: Mapping[str, LiveEdge] = field(default_factory=dict)
    #: Names of nodes that exist by declaration (``system:example``).
    declared_ids: tuple[str, ...] = ()
    #: Fields of a declared-only node, keyed by the declared id they belong to.
    declared_fields: Mapping[str, Mapping[str, LiveCall]] = field(default_factory=dict)

    @property
    def declared_only(self) -> bool:
        """True for a type whose nodes exist by declaration (``system``)."""
        return not self.id_from

    def relations(self) -> frozenset[str]:
        """Every relation this type names, for the start-time binding check."""
        names: set[str] = {f.call.relation for f in self.fields.values()}
        for call in (self.open_fields, self.exists):
            if call is not None:
                names.add(call.relation)
        for edge in self.edges.values():
            names.add(edge.call.relation)
            if edge.push_down is not None:
                names.add(edge.push_down.relation)
        for by_name in self.declared_fields.values():
            names.update(call.relation for call in by_name.values())
        return frozenset(names)

    def node_id(self, *parts: str) -> str:
        """``site`` + ``("S1_X_ONE",)`` -> ``site:S1_X_ONE``."""
        return f"{self.id_prefix}:{':'.join(parts)}"

    def name_parts(self, parts: tuple[str, ...]) -> dict[str, str]:
        """The ``$self`` values of one node: ``{"site": "S1_X_ONE"}``."""
        return dict(zip(self.id_from, parts))

    def edges_of(self, system: str) -> Mapping[str, LiveEdge]:
        """A declared-only type's edges that belong to one declared id."""
        return {name: e for name, e in self.edges.items() if e.of == system}

    def to_metadata(self) -> dict[str, Any]:
        """The compiled declaration, in the annotation's own shape.

        :func:`parse_live_type` reads it back to an equal ``LiveType``; the
        server builds its registry from this stored form.
        """
        ident: dict[str, Any] = {"prefix": self.id_prefix}
        if self.id_from:
            ident["from"] = list(self.id_from)
        else:
            ident["declared"] = list(self.declared_ids)
        body: dict[str, Any] = {"id": ident}
        if self.system is not None:
            body["system"] = self.system
        if self.name_pattern is not None:
            body["name_pattern"] = self.name_pattern
        if self.mention_pattern is not None:
            body["mention_pattern"] = self.mention_pattern
        fields: dict[str, Any] = {
            name: f.to_metadata() for name, f in sorted(self.fields.items())
        }
        for of, by_name in sorted(self.declared_fields.items()):
            for name, call in sorted(by_name.items()):
                fields[name] = {**call.to_metadata(), "of": of}
        if fields:
            body["fields"] = dict(sorted(fields.items()))
        if self.open_fields is not None:
            body["open_fields"] = self.open_fields.to_metadata()
        if self.exists is not None:
            body["exists"] = self.exists.to_metadata()
        if self.edges:
            body["edges"] = {
                name: e.to_metadata() for name, e in sorted(self.edges.items())
            }
        return body


@dataclass(frozen=True)
class LiveTypeRegistry:
    """Every live type of one deployment, built once at server start."""

    types: Mapping[str, LiveType] = field(default_factory=dict)

    def by_subtype(self, subtype: str) -> LiveType | None:
        """The live type named ``subtype``, or None for a stored type."""
        return self.types.get(subtype)

    def by_prefix(self, prefix: str) -> LiveType | None:
        """The live type whose node ids start ``<prefix>:``, or None."""
        for live_type in self.types.values():
            if live_type.id_prefix == prefix:
                return live_type
        return None

    def parse_node_id(self, node_id: str) -> tuple[LiveType, tuple[str, ...]] | None:
        """``site:S1_X_ONE`` -> (the site type, ("S1_X_ONE",)).

        None when the prefix is not a live type's. A live prefix with a
        malformed name raises :class:`LiveNodeIdInvalid` (a
        :class:`LiveTypeInvalid`): an empty name or name part, a name that
        fails ``name_pattern``, the wrong number of parts, or a declared-only
        id (``system:<x>``) that is not declared.
        """
        if not isinstance(node_id, str) or ":" not in node_id:
            return None
        prefix, rest = node_id.split(":", 1)
        live_type = self.by_prefix(prefix)
        if live_type is None:
            return None
        data = {"node_id": node_id, "subtype": live_type.subtype}
        if live_type.declared_only:
            if rest not in live_type.declared_ids:
                raise LiveNodeIdInvalid(
                    f"{node_id!r} is not a declared {live_type.subtype} node; "
                    f"the declared ones are "
                    f"{[live_type.node_id(d) for d in live_type.declared_ids]}.",
                    data={**data, "declared": list(live_type.declared_ids)},
                    fix_hint="use one of the declared node ids",
                )
            return live_type, (rest,)
        width = len(live_type.id_from)
        parts = tuple(rest.split(":", width - 1)) if width > 1 else (rest,)
        if len(parts) != width or any(not part for part in parts):
            raise LiveNodeIdInvalid(
                f"{node_id!r} is not a well-formed {live_type.subtype} id: it "
                f"needs {width} non-empty part(s) after "
                f"`{live_type.id_prefix}:` ({', '.join(live_type.id_from)}).",
                data={**data, "id_from": list(live_type.id_from)},
                fix_hint=(
                    f"write `{live_type.id_prefix}:"
                    + ":".join(f"<{c}>" for c in live_type.id_from) + "`"
                ),
            )
        if not name_matches(live_type.name_pattern, rest):
            raise LiveNodeIdInvalid(
                f"{node_id!r}: {rest!r} is not a {live_type.subtype} name "
                f"(names match {live_type.name_pattern!r}).",
                data={**data, "name_pattern": live_type.name_pattern},
                fix_hint="check the spelling of the name",
            )
        return live_type, parts

    def edge_set(self, subtype: str) -> tuple[LiveType, LiveEdge] | None:
        """The system node edge a set read on ``subtype`` follows.

        ``site`` -> (the system type, its ``sites`` edge): sets start at a
        system node. The edge is the one on a declared-only type
        whose far end is ``subtype`` and whose ``of`` is the type's own
        ``system``. :meth:`from_types` refuses a registry where that is not
        unique, so the answer here is never a choice.
        """
        live_type = self.by_subtype(subtype)
        if live_type is None or live_type.system is None:
            return None
        found = _set_edges(self.types.values(), live_type)
        return found[0] if found else None

    def relations(self) -> dict[str, str]:
        """Every relation a live type names, relation -> subtype."""
        return {
            relation: live_type.subtype
            for live_type in self.types.values()
            for relation in sorted(live_type.relations())
        }

    def caller_arg_value(self, edge: LiveEdge, value: Any) -> Any:
        """A caller's ``$arg`` value as the relation receives it.

        A node id is passed as its name, without the prefix: ``site:B``
        reaches ``destination_site`` as ``B``. Its prefix must be the edge's
        far-end type, or :class:`LiveNodeIdInvalid` is raised. A value that
        is not a live node id (a bare name, a pattern) is passed unchanged.
        """
        if not isinstance(value, str):
            return value
        parsed = self.parse_node_id(value)
        if parsed is None:
            return value
        live_type, parts = parsed
        if live_type.subtype != edge.to:
            raise LiveNodeIdInvalid(
                f"edge {edge.name!r} takes {edge.to} nodes; {value!r} is a "
                f"{live_type.subtype} node.",
                data={"edge": edge.name, "to": edge.to, "node_id": value,
                      "subtype": live_type.subtype},
                fix_hint=f"pass a `{self._prefix_of(edge.to)}:` id",
            )
        return ":".join(parts)

    def assert_bound(self, bound: Iterable[str]) -> None:
        """Refuse a relation a live type names that no binding serves."""
        bound_set = set(bound)
        for live_type in sorted(self.types.values(), key=lambda t: t.subtype):
            unbound = sorted(live_type.relations() - bound_set)
            if unbound:
                raise LiveTypeInvalid(
                    f"live type {live_type.subtype!r} names relation(s) "
                    f"{unbound} that no `agent_surface.live_bindings` entry "
                    f"binds, so its reads could never be served.",
                    data={"subtype": live_type.subtype, "unbound": unbound,
                          "reason": "relation_unbound"},
                    fix_hint="bind each relation, or remove it from the type",
                )

    def assert_served(self, names: Iterable[str]) -> None:
        """Refuse an ``agent_surface.live_types`` name no live type has."""
        unknown = sorted(set(names) - set(self.types))
        if unknown:
            raise LiveTypeInvalid(
                f"`agent_surface.live_types` names {unknown}, which no "
                f"enrolled subtype declares as a live type. Declared live "
                f"types are {sorted(self.types)}.",
                data={"unknown": unknown, "declared": sorted(self.types),
                      "reason": "served_type_undeclared"},
                fix_hint=(
                    "fix the spelling, or declare `okg_live_type` on the "
                    "subtype"
                ),
            )

    def _prefix_of(self, subtype: str) -> str:
        live_type = self.by_subtype(subtype)
        return live_type.id_prefix if live_type is not None else subtype

    @classmethod
    def from_types(cls, types: Iterable[LiveType]) -> "LiveTypeRegistry":
        """Check the declarations against each other and build the registry.

        Refuses, by name: two types with one subtype or one prefix; an edge
        whose far end is not a live type; ``member_columns`` that omit the
        far-end type's name columns; a ``system`` no declared-only type
        declares; and a type with more than one set edge.
        """
        by_subtype: dict[str, LiveType] = {}
        prefixes: dict[str, str] = {}
        for live_type in types:
            if live_type.subtype in by_subtype:
                raise LiveTypeInvalid(
                    f"live type {live_type.subtype!r} is declared twice.",
                    data={"subtype": live_type.subtype,
                          "reason": "duplicate_type"},
                )
            owner = prefixes.get(live_type.id_prefix)
            if owner is not None:
                raise LiveTypeInvalid(
                    f"live types {owner!r} and {live_type.subtype!r} both "
                    f"declare id prefix {live_type.id_prefix!r}; a node id "
                    f"must name exactly one type.",
                    data={"subtype": live_type.subtype, "other": owner,
                          "prefix": live_type.id_prefix,
                          "reason": "duplicate_prefix"},
                    fix_hint="give each live type its own `id.prefix`",
                )
            by_subtype[live_type.subtype] = live_type
            prefixes[live_type.id_prefix] = live_type.subtype

        # One relation, one live type: `relations()` maps a relation to the
        # type that names it, so a second type naming it would win silently.
        owners: dict[str, str] = {}
        for live_type in sorted(by_subtype.values(), key=lambda t: t.subtype):
            for relation in sorted(live_type.relations()):
                owner = owners.setdefault(relation, live_type.subtype)
                if owner != live_type.subtype:
                    raise LiveTypeInvalid(
                        f"live types {owner!r} and {live_type.subtype!r} both "
                        f"name relation {relation!r}; a relation belongs to "
                        f"one live type.",
                        data={"subtype": live_type.subtype, "other": owner,
                              "relation": relation,
                              "reason": "relation_shared"},
                        fix_hint=(
                            "declare the relation once, on the type whose "
                            "nodes it reads"
                        ),
                    )

        systems = {
            declared: live_type.subtype
            for live_type in by_subtype.values()
            for declared in live_type.declared_ids
        }
        for live_type in sorted(by_subtype.values(), key=lambda t: t.subtype):
            if live_type.system is not None and live_type.system not in systems:
                raise LiveTypeInvalid(
                    f"live type {live_type.subtype!r} declares `system: "
                    f"{live_type.system}`, which no declared-only live type "
                    f"declares; declared systems are {sorted(systems)}.",
                    data={"subtype": live_type.subtype, "key": "system",
                          "system": live_type.system,
                          "reason": "system_undeclared"},
                    fix_hint=(
                        "declare the system under a type with `id.declared`"
                    ),
                )
            for edge in live_type.edges.values():
                far = by_subtype.get(edge.to)
                if far is None:
                    raise LiveTypeInvalid(
                        f"live type {live_type.subtype!r} edge {edge.name!r} "
                        f"leads to {edge.to!r}, which is not a declared live "
                        f"type. Live types are {sorted(by_subtype)}.",
                        data={"subtype": live_type.subtype,
                              "key": f"edges.{edge.name}.to",
                              "relation": edge.call.relation, "to": edge.to,
                              "reason": "edge_target_undeclared"},
                        fix_hint=f"declare `okg_live_type` on {edge.to!r}",
                    )
                pushed_fields = sorted(
                    set(edge.push_down.keys) & set(far.fields)
                    if edge.push_down is not None else ()
                )
                if pushed_fields:
                    raise LiveTypeInvalid(
                        f"live type {live_type.subtype!r} edge {edge.name!r} "
                        f"`push_down.keys` names {pushed_fields}, declared "
                        f"per-node fields of {edge.to!r}; the system's "
                        f"expression cannot read them, so a pushed condition "
                        f"on one would read as an empty set.",
                        data={"subtype": live_type.subtype,
                              "key": f"edges.{edge.name}.push_down.keys",
                              "fields": pushed_fields,
                              "reason": "push_key_is_a_field"},
                        fix_hint="drop those keys from `push_down.keys`",
                    )
                for label, columns in (
                    ("member_columns", edge.member_columns),
                    ("push_down.member_columns",
                     edge.push_down.member_columns
                     if edge.push_down is not None else ()),
                ):
                    missing = [c for c in far.id_from if c not in columns]
                    if columns and missing:
                        raise LiveTypeInvalid(
                            f"live type {live_type.subtype!r} edge "
                            f"{edge.name!r} `{label}` {list(columns)} lacks "
                            f"the name column(s) {missing} of {edge.to!r}, so "
                            f"its rows could not be turned into node ids.",
                            data={"subtype": live_type.subtype,
                                  "key": f"edges.{edge.name}.{label}",
                                  "relation": edge.call.relation,
                                  "missing": missing,
                                  "reason": "member_columns_lack_id"},
                        )
        for live_type in by_subtype.values():
            if live_type.system is None:
                continue
            found = _set_edges(by_subtype.values(), live_type)
            if len(found) > 1:
                raise LiveTypeInvalid(
                    f"live type {live_type.subtype!r} has "
                    f"{len(found)} set edges on system "
                    f"{live_type.system!r} "
                    f"({sorted(e.name for _, e in found)}); a set read must "
                    f"start at exactly one.",
                    data={"subtype": live_type.subtype,
                          "edges": sorted(e.name for _, e in found),
                          "reason": "ambiguous_set_edge"},
                )
        return cls(types=dict(sorted(by_subtype.items())))

    @classmethod
    def from_catalog(
        cls, metadata_by_subtype: Mapping[str, Mapping[str, Any]],
    ) -> "LiveTypeRegistry":
        """Build the registry from ``okg.subtypes.metadata`` rows.

        Each stored ``live_type`` is parsed again against the SAME subtype's
        stored ``live_relations``, so a catalog composed by other code is
        checked exactly as a fresh composition is.
        """
        from okg.substrate.external_backed.live_relations import LiveRelation

        types: list[LiveType] = []
        for subtype, metadata in sorted(metadata_by_subtype.items()):
            if not isinstance(metadata, Mapping):
                continue
            body = metadata.get(METADATA_KEY)
            if body is None:
                continue
            stored = metadata.get("live_relations") or {}
            relations = {
                name: LiveRelation.from_metadata(name, subtype, rel)
                for name, rel in sorted(stored.items())
                if isinstance(rel, Mapping)
            } if isinstance(stored, Mapping) else {}
            types.append(parse_live_type(
                subtype, body, relations, where=" (stored in the catalog)",
            ))
        return cls.from_types(types)


def _set_edges(
    types: Iterable[LiveType], target: LiveType,
) -> list[tuple[LiveType, LiveEdge]]:
    return [
        (live_type, edge)
        for live_type in sorted(types, key=lambda t: t.subtype)
        if live_type.declared_only
        for edge in sorted(live_type.edges.values(), key=lambda e: e.name)
        if edge.to == target.subtype and edge.of == target.system
    ]


# --------------------------------------------------------------------------
# the parser
# --------------------------------------------------------------------------


class _Ctx:
    """Names the type and annotation file in every refusal."""

    def __init__(self, subtype: str, where: str) -> None:
        self.subtype = subtype
        self.where = where

    def refuse(
        self, key: str, detail: str, *, relation: str | None = None,
        fix_hint: str | None = None, **data: Any,
    ) -> LiveTypeInvalid:
        rel = f" (relation {relation!r})" if relation is not None else ""
        return LiveTypeInvalid(
            f"live type {self.subtype!r} `{ANNOTATION_KEY}.{key}`{rel}: "
            f"{detail}{self.where}",
            data={"subtype": self.subtype, "key": key,
                  **({"relation": relation} if relation is not None else {}),
                  **data},
            fix_hint=fix_hint,
        )


def _mapping(ctx: _Ctx, value: Any, key: str, allowed: frozenset[str]) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ctx.refuse(key, f"must be a mapping; got {type(value).__name__}.")
    unknown = sorted(str(k) for k in set(value) - allowed)
    if unknown:
        raise ctx.refuse(
            key, f"has unknown key(s) {unknown}; accepted keys are "
            f"{sorted(allowed)}.", unknown=unknown,
        )
    return value


def _token(ctx: _Ctx, value: Any, key: str) -> str:
    if not isinstance(value, str) or not _TOKEN.fullmatch(value):
        raise ctx.refuse(
            key, f"must be a lower-snake-case token; got {value!r}.",
        )
    return value


def _tokens(ctx: _Ctx, value: Any, key: str) -> tuple[str, ...]:
    if not isinstance(value, (list, tuple)) or not value:
        raise ctx.refuse(key, f"must be a non-empty list; got {value!r}.")
    names = tuple(_token(ctx, v, key) for v in value)
    if len(set(names)) != len(names):
        raise ctx.refuse(key, f"repeats a name: {list(names)}.")
    return names


def _columns(ctx: _Ctx, value: Any, key: str) -> tuple[str, ...]:
    if not isinstance(value, (list, tuple)) or not value:
        raise ctx.refuse(key, f"must be a non-empty list of column names; got {value!r}.")
    names = tuple(value)
    if any(not isinstance(c, str) or not c.strip() for c in names):
        raise ctx.refuse(key, f"must hold non-empty column names; got {list(names)}.")
    if len(set(names)) != len(names):
        raise ctx.refuse(key, f"repeats a column: {list(names)}.")
    return names


def _pattern(ctx: _Ctx, value: Any, key: str, *, anchored: bool) -> str:
    if not isinstance(value, str) or not value:
        raise ctx.refuse(key, f"must be a non-empty regular expression; got {value!r}.")
    try:
        re.compile(value)
    except re.error as exc:
        raise ctx.refuse(key, f"does not compile ({exc}).") from exc
    from okg.substrate.external_backed.live_relations import (
        _pattern_anchor_problem,
    )

    problem = _pattern_anchor_problem(value)
    if anchored and problem is not None:
        raise ctx.refuse(
            key, f"must match a WHOLE name and is not anchored: {problem}.",
            fix_hint="anchor it, e.g. `^(?:...)$`",
        )
    if not anchored and (value.startswith("^") or value.endswith("$")):
        raise ctx.refuse(
            key, "finds names INSIDE stored text, so it must not be "
            "anchored at the start or end of the text.",
            fix_hint="use `\\b` word boundaries instead of `^`/`$`",
        )
    return value


def _check_fixed(
    ctx: _Ctx, key: str, relation: Any, arg_name: str, value: Any,
) -> None:
    """A fixed value must pass the relation's own argument model."""
    from pydantic import ValidationError

    from okg.substrate.external_backed.live_relations import (
        _is_declared_type,
        compile_args_model,
    )

    declared = {a.name: a for a in relation.args}[arg_name]
    if value is None or not _is_declared_type(value, declared.type_name):
        raise ctx.refuse(
            key, f"fixed value {value!r} for argument {arg_name!r} is not a "
            f"{declared.type_name}.", relation=relation.name, arg=arg_name,
        )
    model = compile_args_model(relation)
    try:
        model.model_validate({arg_name: value})
    except ValidationError as exc:
        problems = [
            err.get("msg") for err in exc.errors()
            if tuple(err.get("loc") or ())[:1] == (arg_name,)
        ]
        if problems:
            raise ctx.refuse(
                key, f"fixed value {value!r} for argument {arg_name!r} breaks "
                f"the relation's own bounds: {problems[0]}.",
                relation=relation.name, arg=arg_name,
            ) from exc


def _parse_call(
    ctx: _Ctx,
    body: Mapping[str, Any],
    key: str,
    relations: Mapping[str, Any] | None,
    *,
    id_from: tuple[str, ...],
    caller_args_allowed: bool,
) -> LiveCall:
    relation_name = _token(ctx, body.get("relation"), f"{key}.relation")
    relation = None
    if relations is not None:
        relation = relations.get(relation_name)
        if relation is None:
            raise ctx.refuse(
                f"{key}.relation",
                f"names {relation_name!r}, which this subtype does not "
                f"declare under `okg_live_relations`; it declares "
                f"{sorted(relations)}.",
                relation=relation_name, reason="relation_undeclared",
                fix_hint=(
                    "declare the relation on this same class, or fix the "
                    "spelling"
                ),
            )
    raw_args = body.get("args")
    if raw_args is None:
        raw_args = {}
    if not isinstance(raw_args, Mapping):
        raise ctx.refuse(
            f"{key}.args", f"must be a mapping; got {type(raw_args).__name__}.",
            relation=relation_name,
        )
    declared_args = (
        {a.name: a for a in relation.args} if relation is not None else None
    )
    args: list[LiveArg] = []
    for raw_name, raw_value in raw_args.items():
        arg_key = f"{key}.args.{raw_name}"
        name = _token(ctx, raw_name, arg_key)
        if declared_args is not None and name not in declared_args:
            raise ctx.refuse(
                arg_key,
                f"{name!r} is not a declared argument of the relation; its "
                f"arguments are {sorted(declared_args)}.",
                relation=relation_name, arg=name, reason="arg_undeclared",
            )
        required = False
        value = raw_value
        if isinstance(raw_value, Mapping):
            form = _mapping(ctx, raw_value, arg_key, _ARG_FORM_KEYS)
            value = form.get("value")
            required = form.get("required", False)
            if not isinstance(required, bool):
                raise ctx.refuse(
                    arg_key, f"`required` must be true or false; got {required!r}.",
                    relation=relation_name,
                )
            if not (isinstance(value, str) and value.startswith(_ARG)):
                raise ctx.refuse(
                    arg_key,
                    "the long form `{value: ..., required: ...}` is only for "
                    "`$arg.<name>`.", relation=relation_name,
                )
        if isinstance(value, str) and value.startswith(_SELF):
            column = value[len(_SELF):]
            if column not in id_from:
                raise ctx.refuse(
                    arg_key,
                    f"`{value}` names {column!r}, which is not a name column "
                    f"of this type ({list(id_from)}).",
                    relation=relation_name, reason="self_column_unknown",
                )
            args.append(LiveArg(name=name, kind="self", value=column))
        elif isinstance(value, str) and value.startswith(_ARG):
            if not caller_args_allowed:
                raise ctx.refuse(
                    arg_key,
                    f"`{value}` is a caller argument, which only an edge "
                    f"can take: a field or a check is read with no caller "
                    f"arguments.", relation=relation_name,
                    reason="caller_arg_outside_edge",
                )
            caller = _token(ctx, value[len(_ARG):], arg_key)
            if (
                declared_args is not None and declared_args[name].required
                and not required
            ):
                raise ctx.refuse(
                    arg_key,
                    f"fills the relation's REQUIRED argument {name!r} from "
                    f"`{value}` without marking it required, so a caller "
                    f"who leaves it out would reach the upstream call with "
                    f"no value.", relation=relation_name, arg=name,
                    reason="required_arg_not_marked",
                    fix_hint=f"write `{name}: {{value: {value}, required: true}}`",
                )
            args.append(LiveArg(
                name=name, kind="arg", value=caller, required=required,
            ))
        elif isinstance(value, str) and value.startswith("$"):
            raise ctx.refuse(
                arg_key,
                f"{value!r} starts with `$` but is neither `$self.<column>` "
                f"nor `$arg.<name>`.", relation=relation_name,
            )
        else:
            if relation is not None:
                _check_fixed(ctx, arg_key, relation, name, value)
            elif value is None:
                raise ctx.refuse(
                    arg_key, "a fixed value may not be null.",
                    relation=relation_name,
                )
            args.append(LiveArg(name=name, kind="fixed", value=value))
    if declared_args is not None:
        filled = {a.name for a in args}
        unfilled = sorted(
            n for n, a in declared_args.items() if a.required and n not in filled
        )
        if unfilled:
            raise ctx.refuse(
                f"{key}.args",
                f"leaves the relation's required argument(s) {unfilled} "
                f"unfilled, so every call would be refused.",
                relation=relation_name, unfilled=unfilled,
                reason="required_arg_unfilled",
            )
    return LiveCall(
        relation=relation_name,
        args=tuple(sorted(args, key=lambda a: a.name)),
    )


def _parse_push_down(
    ctx: _Ctx, value: Any, key: str, relations: Mapping[str, Any] | None,
) -> PushDown:
    body = _mapping(ctx, value, key, _PUSH_DOWN_KEYS)
    relation_name = _token(ctx, body.get("relation"), f"{key}.relation")
    arg = _token(ctx, body.get("arg"), f"{key}.arg")
    if relations is not None:
        relation = relations.get(relation_name)
        if relation is None:
            raise ctx.refuse(
                f"{key}.relation",
                f"names {relation_name!r}, which this subtype does not "
                f"declare; it declares {sorted(relations)}.",
                relation=relation_name, reason="relation_undeclared",
            )
        declared = {a.name: a for a in relation.args}
        if arg not in declared:
            raise ctx.refuse(
                f"{key}.arg",
                f"{arg!r} is not a declared argument of the relation; its "
                f"arguments are {sorted(declared)}.",
                relation=relation_name, arg=arg, reason="arg_undeclared",
            )
        if declared[arg].type_name != "string":
            raise ctx.refuse(
                f"{key}.arg",
                f"{arg!r} is a {declared[arg].type_name}; a pushed-down "
                f"expression is text.", relation=relation_name,
            )
    atoms = body.get("atoms")
    if not isinstance(atoms, Mapping) or not atoms:
        raise ctx.refuse(f"{key}.atoms", "must be a non-empty mapping.")
    for shape, template in atoms.items():
        if shape not in PUSH_DOWN_ATOMS:
            raise ctx.refuse(
                f"{key}.atoms",
                f"{shape!r} is not an atom shape; accepted shapes are "
                f"{sorted(PUSH_DOWN_ATOMS)}.",
            )
        if not isinstance(template, str) or not template:
            raise ctx.refuse(f"{key}.atoms.{shape}", "must be a template string.")
        missing = [p for p in PUSH_DOWN_ATOMS[shape] if p not in template]
        if missing:
            raise ctx.refuse(
                f"{key}.atoms.{shape}",
                f"template {template!r} lacks {missing}.",
            )
    join = body.get("join")
    if not isinstance(join, Mapping) or not join:
        raise ctx.refuse(f"{key}.join", "must be a non-empty mapping.")
    for op, symbol in join.items():
        if op not in PUSH_DOWN_JOINS:
            raise ctx.refuse(
                f"{key}.join",
                f"{op!r} is not a join; accepted joins are "
                f"{sorted(PUSH_DOWN_JOINS)}.",
            )
        if not isinstance(symbol, str) or not symbol:
            raise ctx.refuse(f"{key}.join.{op}", "must be a non-empty string.")
    absent_joins = [op for op in ("and", "or") if op not in join]
    if absent_joins:
        raise ctx.refuse(
            f"{key}.join",
            f"must declare {absent_joins}; the join operators are the "
            "system's own and have no default.",
            reason="join_operator_missing",
        )
    empty_error = body.get("empty_error")
    if empty_error is not None and (
        not isinstance(empty_error, (list, tuple)) or not empty_error
        or not all(isinstance(m, str) and m.strip() for m in empty_error)
    ):
        raise ctx.refuse(
            f"{key}.empty_error", "must be a non-empty list of non-empty strings.",
            reason="empty_error_invalid",
        )
    member_columns = (
        _columns(ctx, body["member_columns"], f"{key}.member_columns")
        if body.get("member_columns") is not None else ()
    )
    keys = (
        _columns(ctx, body["keys"], f"{key}.keys")
        if body.get("keys") is not None else ()
    )
    bad_keys = [k for k in keys if not PUSH_VALUE_CHARS.fullmatch(k)]
    if bad_keys:
        raise ctx.refuse(
            f"{key}.keys",
            f"{bad_keys} hold characters a pushed key may not hold "
            "(letters, digits, `_`, `.` and `-` only).",
            reason="push_key_invalid",
        )
    return PushDown(
        relation=relation_name, arg=arg,
        atoms={str(k): str(v) for k, v in sorted(atoms.items())},
        join={str(k): str(v) for k, v in sorted(join.items())},
        member_columns=member_columns,
        keys=keys,
        empty_error=tuple(str(m) for m in empty_error) if empty_error else (),
    )


def parse_live_type(
    subtype: str,
    annotation: Mapping[str, Any],
    relations: Mapping[str, Any] | None,
    *,
    where: str = "",
) -> LiveType:
    """Parse and check one ``okg_live_type`` annotation.

    ``relations`` is the subtype's own parsed ``okg_live_relations``. Every
    relation the annotation names must be among them, and every argument name
    must be a declared argument of that relation.

    ``relations=None`` checks the SHAPE only. Composition passes it for a
    file that adds the annotation to a subtype whose relations another file
    declares; the composed catalog is then checked whole by
    :meth:`LiveTypeRegistry.from_catalog`, so no declaration escapes the
    relation check. The stored form (:meth:`LiveType.to_metadata`) parses
    to an equal ``LiveType``.
    """
    ctx = _Ctx(subtype, where)
    if annotation is None:
        raise ctx.refuse("", "must be a mapping; got None.")
    body = _mapping(ctx, annotation, "", _TYPE_KEYS)

    ident = _mapping(ctx, body.get("id"), "id", _ID_KEYS)
    prefix = _token(ctx, ident.get("prefix"), "id.prefix")
    has_from = ident.get("from") is not None
    has_declared = ident.get("declared") is not None
    if has_from == has_declared:
        raise ctx.refuse(
            "id", "must give exactly one of `from` (name columns) and "
            "`declared` (node names that exist by declaration).",
        )
    id_from = _tokens(ctx, ident["from"], "id.from") if has_from else ()
    declared_ids = (
        _tokens(ctx, ident["declared"], "id.declared") if has_declared else ()
    )
    declared_only = not id_from

    system = body.get("system")
    if system is not None:
        system = _token(ctx, system, "system")
        if declared_only:
            raise ctx.refuse(
                "system", "a declared-only type IS the system; it does not "
                "name one.",
            )
    name_pattern = (
        _pattern(ctx, body["name_pattern"], "name_pattern", anchored=True)
        if body.get("name_pattern") is not None else None
    )
    mention_pattern = (
        _pattern(ctx, body["mention_pattern"], "mention_pattern", anchored=False)
        if body.get("mention_pattern") is not None else None
    )
    if declared_only and (name_pattern or mention_pattern):
        raise ctx.refuse(
            "name_pattern" if name_pattern else "mention_pattern",
            "a declared-only type has fixed names; it takes no pattern.",
        )

    def of_value(key: str, raw: Any) -> str | None:
        if raw is None:
            if declared_only:
                raise ctx.refuse(
                    key, f"a declared-only type must say which declared node "
                    f"this belongs to with `of` (one of {list(declared_ids)}).",
                )
            return None
        if not declared_only:
            raise ctx.refuse(
                key, "`of` is only for declared-only types (`id.declared`).",
            )
        if raw not in declared_ids:
            raise ctx.refuse(
                key, f"`of: {raw}` is not a declared id; declared ids are "
                f"{list(declared_ids)}.",
            )
        return str(raw)

    fields: dict[str, LiveField] = {}
    declared_fields: dict[str, dict[str, LiveCall]] = {}
    raw_fields = body.get("fields") or {}
    if not isinstance(raw_fields, Mapping):
        raise ctx.refuse("fields", "must be a mapping of field name to declaration.")
    for raw_name, raw_field in raw_fields.items():
        key = f"fields.{raw_name}"
        name = _token(ctx, raw_name, key)
        if declared_only:
            spec = _mapping(ctx, raw_field, key, _DECLARED_FIELD_KEYS)
            of = of_value(f"{key}.of", spec.get("of"))
            call = _parse_call(
                ctx, spec, key, relations, id_from=id_from,
                caller_args_allowed=False,
            )
            declared_fields.setdefault(str(of), {})[name] = call
            continue
        spec = _mapping(ctx, raw_field, key, _FIELD_KEYS | {"of"})
        of_value(f"{key}.of", spec.get("of"))
        call = _parse_call(
            ctx, spec, key, relations, id_from=id_from, caller_args_allowed=False,
        )
        column = spec.get("column")
        if not isinstance(column, str) or not column.strip():
            raise ctx.refuse(
                f"{key}.column", f"must name the row column; got {column!r}.",
                relation=call.relation,
            )
        type_name = spec.get("type", "string")
        if type_name not in FIELD_TYPES:
            raise ctx.refuse(
                f"{key}.type",
                f"{type_name!r} is not one of {sorted(FIELD_TYPES)}.",
                relation=call.relation,
            )
        fields[name] = LiveField(
            name=name, call=call, column=column, type=str(type_name),
        )

    def one_call(key: str) -> LiveCall | None:
        if body.get(key) is None:
            return None
        if declared_only:
            raise ctx.refuse(key, "a declared-only type has no per-node calls.")
        spec = _mapping(ctx, body[key], key, _CALL_KEYS)
        return _parse_call(
            ctx, spec, key, relations, id_from=id_from, caller_args_allowed=False,
        )

    open_fields = one_call("open_fields")
    exists = one_call("exists")

    edges: dict[str, LiveEdge] = {}
    raw_edges = body.get("edges") or {}
    if not isinstance(raw_edges, Mapping):
        raise ctx.refuse("edges", "must be a mapping of edge name to declaration.")
    for raw_name, raw_edge in raw_edges.items():
        key = f"edges.{raw_name}"
        name = _token(ctx, raw_name, key)
        if name in fields or any(name in f for f in declared_fields.values()):
            raise ctx.refuse(
                key, f"{name!r} is both a field and an edge; a read could not "
                f"tell which is meant.",
            )
        spec = _mapping(ctx, raw_edge, key, _EDGE_KEYS)
        of = of_value(f"{key}.of", spec.get("of"))
        call = _parse_call(
            ctx, spec, key, relations, id_from=id_from, caller_args_allowed=True,
        )
        to = _token(ctx, spec.get("to"), f"{key}.to")
        member_fields = spec.get("member_fields")
        if member_fields is not None and member_fields != MEMBER_FIELDS_FROM_ROWS:
            raise ctx.refuse(
                f"{key}.member_fields",
                f"the only accepted value is `{MEMBER_FIELDS_FROM_ROWS}`; got "
                f"{member_fields!r}.", relation=call.relation,
            )
        member_columns = (
            _columns(ctx, spec["member_columns"], f"{key}.member_columns")
            if spec.get("member_columns") is not None else ()
        )
        if member_columns and member_fields is None:
            raise ctx.refuse(
                f"{key}.member_columns",
                f"names the listing's columns, which become fields only with "
                f"`member_fields: {MEMBER_FIELDS_FROM_ROWS}`.",
                relation=call.relation,
            )
        push_down = (
            _parse_push_down(ctx, spec["push_down"], f"{key}.push_down", relations)
            if spec.get("push_down") is not None else None
        )
        edges[name] = LiveEdge(
            name=name, call=call, to=to,
            member_fields=member_fields is not None,
            push_down=push_down, of=of, member_columns=member_columns,
        )

    return LiveType(
        subtype=subtype,
        id_prefix=prefix,
        id_from=id_from,
        system=system,
        name_pattern=name_pattern,
        mention_pattern=mention_pattern,
        fields=dict(sorted(fields.items())),
        open_fields=open_fields,
        exists=exists,
        edges=dict(sorted(edges.items())),
        declared_ids=declared_ids,
        declared_fields={
            of: dict(sorted(by_name.items()))
            for of, by_name in sorted(declared_fields.items())
        },
    )


__all__ = [
    "ANNOTATION_KEY",
    "FIELD_TYPES",
    "METADATA_KEY",
    "LiveArg",
    "LiveArgMissing",
    "LiveCall",
    "LiveEdge",
    "LiveField",
    "LiveNodeIdInvalid",
    "LiveType",
    "LiveTypeInvalid",
    "LiveTypeRegistry",
    "PushDown",
    "name_matches",
    "parse_live_type",
]
