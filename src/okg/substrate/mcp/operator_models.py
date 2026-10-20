"""Typed input models for the MCP read algebra (PACT `typed-operator-inputs`).

Issue #2009. The operator inputs `inspect.target`, `expand.input`,
`filter.input`, `filter.predicate`, `map.input`, `map.projection` and
`aggregate.input` used to be bare `dict` or `Any`, so the listed JSON schema
told an agent nothing about the grammar it had to satisfy. The grammar lived
in prose and in runtime `ValueError`s, and an agent discovered it by trial.

These models are the same grammar, declared. The `tools/list` payload now
shows the discriminator, the closed operator set, the enums and every
maximum, and the SDK validates an argument against them BEFORE the tool body
runs, raising a `ToolError` that names the offending field and the values it
would accept. (`ByteBoundedFastMCP.call_tool` in `response_bounds.py` then
flattens any non-`SubstrateError` into one bounded `operator_execution_failed`
envelope, so the public refusal is bounded but unspecific — the same as it was
for the `ValueError`s these models replace.)

Each tool body converts its validated model back to the plain mapping the
implementation already consumed (`as_mapping` below), so a valid input takes
exactly the path it took before.

Three behaviors DO change, deliberately, and each replaces a silent outcome
with a refusal:

* An argument no operator declares is refused, naming the key
  (`UnknownOperatorArgument` in `response_bounds.py`). The SDK drops an
  undeclared keyword, so `search(scope=...)` used to return a normal,
  completely unscoped page with nothing saying the argument was ignored.
* `query.row_cap` and `query.timeout_ms` outside their ranges are refused
  rather than clamped. `run_sql` silently pinned them to its ceilings
  (10,000 rows / 60,000 ms), so a caller asking for 50,000 rows got 10,000
  and no indication that it was not the page it asked for. The ceilings are
  now published in the schema and a value past them is an error, which is
  this repository's standing preference: refuse, never quietly clamp.
* `filter`, `map` and `aggregate` refuse a `limit` or `offset` past their
  bounds instead of clamping them with `max(1, min(int(limit), 1000))`,
  which used to serve a shorter page than the caller asked for without
  saying which page it had served.

Byte discipline: this surface is measured by `surface_budget.py` and pinned in
`tests/substrate/mcp/test_tool_surface_budget.py`. Model and field names are
short because pydantic serializes every one of them into the schema a client
pays for at startup. Field descriptions are deliberately absent: the type IS
the description, and prose would be paid for twice.
"""

from __future__ import annotations

import functools
from collections.abc import Mapping
from typing import Annotated, Any, Literal, Optional, Union

from pydantic import BaseModel, ConfigDict, Field, RootModel, model_validator
from pydantic_core import PydanticCustomError

from okg.substrate.errors import SubstrateError
from okg.substrate.mcp.operator_sql import is_text_contains, text_operand_problem

#: Page ceilings, declared rather than clamped silently. `filter`/`map`/
#: `aggregate` already clamped to 1000 rows and `expand` already refused an
#: offset over 10,000; naming them in the schema means a caller learns the
#: bound from the tool list instead of from a surprising short page.
MAX_PAGE_LIMIT = 1000
MAX_OFFSET = 10_000
MAX_PATH_HOPS = 3
MAX_DEPTH = 3
MAX_AGGREGATE_BY = 4
#: `subtype_summary.MAX_SECTION_PAGE_SIZE`. Re-stated rather than imported so
#: this module keeps no runtime dependency on the operator implementations;
#: `test_typed_operator_inputs.py` asserts the two agree.
MAX_SUBTYPE_PAGE_SIZE = 32
#: `search.MAX_PREVIEW_CHARS`. Restated for the same reason as the page size
#: above, and checked against the real constant by the surface tests.
MAX_PREVIEW_CHARS = 4096

Limit = Annotated[int, Field(ge=1, le=MAX_PAGE_LIMIT)]
Offset = Annotated[int, Field(ge=0, le=MAX_OFFSET)]


def declared(annotation: Any, **constraints: Any) -> Any:
    """Publish a constraint in the schema without enforcing it in the schema.

    `expand` already refuses an out-of-range `depth`, an unknown `direction`,
    a malformed `path` and a zero `limit` — through `path_diagnostics`, with a
    typed envelope that names the hop index, the requested shape and a fix
    hint. A pydantic constraint would win the race and replace all of that
    with "Input should be less than or equal to 3".

    So the bound is DECLARED to the caller and left ENFORCED where the better
    message lives. The published schema is stricter than pydantic's
    validation and exactly as strict as the runtime, so nothing out of range
    is accepted — it is only refused one layer later and more usefully.
    One declared exception: `search.max_chars` above its maximum is lowered
    to it, not refused, and the result says so (`max_chars_lowered`, issue
    #3252); a client that checks the schema first still refuses it itself.
    `tests/substrate/mcp/test_expand_path_diagnostics.py` pins those
    envelopes; `test_typed_operator_inputs.py` pins that the bound is listed.
    """
    return Annotated[annotation, Field(json_schema_extra=constraints)]


def compact_schema(
    schema: Any,
    *,
    keep_descriptions: bool = False,
    keep_nulls: bool = False,
    in_map: bool = False,
) -> Any:
    """Strip schema bytes that carry no information for a caller.

    Every byte of every input schema is sent to every session at startup and
    is pinned by `test_tool_surface_budget.py`. Pydantic's default rendering
    spends most of its bytes on two things a reader already knows:

    * `"title": "Node Id"` — a title mechanically derived from the key it sits
      next to, on all 200-odd fields across these models.
    * `"anyOf": [X, {"type": "null"}], "default": null` for every optional
      field — 47 bytes to say "may be omitted", which the absence of the key
      from `required` already says.

    `keep_nulls` turns the second one off, and OUTPUT schemas must set it. For
    an input schema, dropping the null branch makes the published contract
    STRICTER than the runtime, which is safe. For an output schema the
    direction reverses: a client validating our response against a schema
    that forbids null would reject a field we really do send as null. So the
    saving is taken on the way in and declined on the way out.

    Both are removed here, along with a `$def`'s own title (the `$ref` points
    at its KEY, not its title) and the `discriminator.mapping` block, which
    restates what each `oneOf` member's `const` already says. `discriminator`
    is an OpenAPI keyword that a plain JSON Schema validator ignores, and
    every `oneOf` member keeps the `const` on its own discriminator field, so
    dropping the mapping leaves validation complete. Validation is untouched
    generally: pydantic still accepts an explicit null for an optional field,
    so the published schema ends up stricter than the runtime, never looser.

    `in_map` marks the places where keys are NAMES rather than keywords —
    inside `properties`, `$defs` and friends. Nothing is stripped there. A
    field genuinely called `title` must survive: deleting it while its entry
    in `required` stays behind leaves a schema demanding a property it does
    not define, which no instance can satisfy.
    """
    kwargs = {"keep_descriptions": keep_descriptions, "keep_nulls": keep_nulls}
    if isinstance(schema, list):
        return [compact_schema(item, **kwargs) for item in schema]
    if not isinstance(schema, dict):
        return schema
    if in_map:
        return {name: compact_schema(body, **kwargs) for name, body in schema.items()}
    out: dict[str, Any] = {}
    for key, value in schema.items():
        if key == "title" or (key == "description" and not keep_descriptions):
            # A model docstring is a note to the next maintainer, not guidance
            # an agent pays for on every connect; the tool docstring is where
            # caller-facing prose belongs.
            continue
        if key == "default" and value is None:
            continue
        if key in {"$defs", "definitions", "properties", "patternProperties"}:
            out[key] = compact_schema(value, in_map=True, **kwargs)
            continue
        out[key] = compact_schema(value, **kwargs)
    disc = out.get("discriminator")
    if isinstance(disc, dict) and "mapping" in disc:
        # `mapping` restates, kind by kind, what every `oneOf` member already
        # declares as a `const` on its own discriminator field. Seventeen
        # inspect kinds spell that out twice for 800 B. `propertyName` stays,
        # so the union is still declared as discriminated on `kind`.
        out["discriminator"] = {
            k: v for k, v in disc.items() if k != "mapping"
        }
    if out.get("items") == {} and "type" in out:
        # `"items": {}` constrains nothing; the `type` already says array.
        del out["items"]
    if out.get("additionalProperties") is True:
        # `true` is what JSON Schema already means when the keyword is absent,
        # so spelling it out costs 28 bytes to say nothing. Only the `true`
        # case goes; `false` is a real constraint and stays.
        del out["additionalProperties"]
    branches = out.get("anyOf")
    if isinstance(branches, list) and keep_nulls and len(branches) >= 2:
        # `anyOf: [{"type": X}, {"type": "null"}]` and `type: [X, "null"]` say
        # the same thing to a validator, and the second is much shorter. Two
        # safe rewrites, both requiring that NO branch carries a constraint
        # beyond its type, so nothing can be lost:
        #
        #   every branch bare          -> one `type` array
        #   one null + one typed dict  -> null folded into that dict's `type`
        bare = [
            b.get("type")
            for b in branches
            if isinstance(b, dict) and set(b) == {"type"}
            and isinstance(b.get("type"), str)
        ]
        if len(bare) == len(branches) and "null" in bare:
            merged = {k: v for k, v in out.items() if k != "anyOf"}
            merged["type"] = [t for t in bare if t != "null"] + ["null"]
            return merged
        nulls = [b for b in branches if b == {"type": "null"}]
        rest = [b for b in branches if b != {"type": "null"}]
        if len(nulls) == 1 and len(rest) == 1 and isinstance(rest[0], dict):
            inner = dict(rest[0])
            if isinstance(inner.get("type"), str):
                inner["type"] = [inner["type"], "null"]
                for key, value in out.items():
                    if key != "anyOf":
                        inner.setdefault(key, value)
                return inner
    if isinstance(branches, list) and not keep_nulls:
        rest = [b for b in branches if b != {"type": "null"}]
        if len(rest) < len(branches):
            out["anyOf"] = rest
            branches = rest
        if len(branches) == 1:
            merged = dict(branches[0])
            for key, value in out.items():
                if key != "anyOf":
                    merged.setdefault(key, value)
            return merged
    return out


class _CompactSchema:
    """Mixin that applies `compact_schema` to whatever pydantic renders."""

    @classmethod
    def __get_pydantic_json_schema__(cls, core_schema, handler):  # type: ignore[no-untyped-def]
        return compact_schema(handler(core_schema))


class _Model(_CompactSchema, BaseModel):
    """Strict base: an unknown key is a caller mistake, not a silent no-op.

    `extra="forbid"` is the point of typing these at all. Before this change a
    misspelled key (`subtypes` for `subtype`) was dropped and the operator
    answered a question nobody asked.
    """

    model_config = ConfigDict(extra="forbid")


def as_mapping(value: Any) -> Any:
    """Validated model -> the plain mapping the operator bodies consume.

    `exclude_none=True` matters: the bodies distinguish an absent key from an
    explicit null (`target.get("page_size") is None`, `target.get("to",
    "latest")`), so a dump that materialized every unset field as null would
    change behavior for valid input.
    """
    if isinstance(value, BaseModel):
        return value.model_dump(exclude_none=True)
    if isinstance(value, list):
        return [as_mapping(item) for item in value]
    return value


# ── predicate tree ────────────────────────────────────────────────────
#
# The closed v1 operator set, exactly the ops `_effective_view_predicate_sql`
# and `_operator_predicate_matches` implement. An op outside it used to reach
# the runtime and raise "unsupported predicate op"; it is now unrepresentable.


class Predicate(_Model):
    """The v1 predicate tree: one node type, a closed fifteen-op set.

    Written as ONE recursive model rather than a per-op discriminated union
    on purpose. The union spelling costs about 1.9 KB of `$defs`, `oneOf` and
    discriminator mapping in EVERY tool that takes a predicate — `filter`,
    `map`, `aggregate` and `expand` — which is over 7 KB on a startup surface
    pinned to the byte in `test_tool_surface_budget.py`. The closed `op`
    enum is what an agent could not previously see; which of `value`,
    `values`, `clause` or `clauses` a given op wants is still checked, by the
    same runtime that checked it before, and named in the refusal.

    `and`/`or` take `clauses`; `not` takes `clause`; `eq`/`lt`/`gt`/`suffix`
    take `field` + `value`; `in`/`between` take `field` + `values`; `exists`
    takes `field`; `contains` takes field='attrs' and an object `value`, or
    field='attrs.<key>' and text (a substring, ASCII case ignored).
    Fields are stable view columns or attrs.<key>.

    `op` is optional for ONE reason: the empty predicate `{}` means "no
    WHERE". `_predicate_to_expr` and `_compose_scope_predicate` have always
    treated `{}` like an omitted predicate, and it is the spelling callers
    use for a plain listing (`filter(input={"kind": "nodes"},
    predicate={})`). A type that refused it would have broken every such
    call while claiming to change nothing. Any OTHER key without an `op` is
    refused below, so the exemption covers the empty object only.
    """

    op: Optional[
        Literal[
            "and", "or", "not", "eq", "in", "exists", "contains", "suffix",
            "prefix", "lt", "gt", "between", "matches", "like", "like_exact",
        ]
    ] = None
    field: Optional[str] = None
    value: Any = None
    values: Optional[list[Any]] = None
    clause: Optional["Predicate"] = None
    clauses: Optional[Annotated[list["Predicate"], Field(min_length=1)]] = None

    @model_validator(mode="after")
    def _only_the_empty_predicate_may_omit_op(self) -> "Predicate":
        if self.op is None and self.model_fields_set - {"op"}:
            raise ValueError(
                "a predicate needs an `op` from the v1 set (and, or, not, "
                "eq, in, exists, contains, suffix, prefix, lt, gt, between, "
                "matches, like, like_exact); "
                "only the empty predicate {} may omit it"
            )
        return self

    @model_validator(mode="after")
    def _sub_predicates_sit_where_they_are_read(self) -> "Predicate":
        """Refuse a logical op whose operand sits in a key nothing reads.

        Decision 182a. `not` reads its one sub-predicate from `clause`, and
        `and`/`or` read theirs from `clauses`; no evaluator reads `value`,
        `values` or `field` on them. `{"op": "not", "value": {...}}` used to
        lower to `NOT (TRUE)` (`operator_sql.predicate_sql`) and to
        `{'not': {'lit': True}}` (`transform.operators._predicate_to_expr`):
        an empty page with `isError: false`. `and`/`or` without `clauses`
        were caught only later, by a refusal that named `query` as the
        fallback. A non-logical op carrying `clause`/`clauses` had them
        ignored. The empty predicate `{}` is not a sub-predicate either:
        under `not` it is `NOT (TRUE)` again.

        The error type is the part the MCP refusal publishes
        (`InvalidOperatorArguments.data.validation_kinds`), so each one
        names the key the op reads; the message carries the detail for a
        direct caller. A validator adds nothing to the JSON schema.
        """
        op = self.op
        if op is None:
            return self
        present = [
            key
            for key in ("field", "value", "values", "clause", "clauses")
            if getattr(self, key) is not None
        ]
        if op == "not":
            stray = [key for key in present if key != "clause"]
            if self.clause is None or stray:
                detail = (
                    "a `not` predicate takes exactly one sub-predicate, "
                    "in `clause`"
                ) + _ignored_keys_note(stray)
                raise PydanticCustomError(
                    "not_takes_clause", "{detail}", {"detail": detail}
                )
            _refuse_empty_sub_predicate("clause", [self.clause])
        elif op in ("and", "or"):
            stray = [key for key in present if key != "clauses"]
            if self.clauses is None or stray:
                detail = (
                    f"an `{op}` predicate takes a non-empty list of "
                    "sub-predicates, in `clauses`"
                ) + _ignored_keys_note(stray)
                raise PydanticCustomError(
                    f"{op}_takes_clauses", "{detail}", {"detail": detail}
                )
            _refuse_empty_sub_predicate("clauses", self.clauses)
        else:
            stray = [key for key in present if key in ("clause", "clauses")]
            if stray:
                detail = (
                    f"a `{op}` predicate has no sub-predicates; `clause` "
                    "belongs to `not` and `clauses` to `and`/`or`"
                ) + _ignored_keys_note(stray)
                raise PydanticCustomError(
                    "clause_only_for_and_or_not", "{detail}", {"detail": detail}
                )
        return self


def _ignored_keys_note(keys: list[str]) -> str:
    if not keys:
        return ""
    named = ", ".join(f"`{key}`" for key in keys)
    return f"; it does not read {named}"


def _refuse_empty_sub_predicate(key: str, children: list["Predicate"]) -> None:
    if any(child.op is None for child in children):
        detail = (
            f"every predicate in `{key}` needs an `op`; the empty "
            "predicate {} is allowed only at the top level"
        )
        raise PydanticCustomError(
            "sub_predicate_needs_op", "{detail}", {"detail": detail}
        )


#: What `filter.predicate`, `expand.filter` and a scope's own `filter` take.
#: Named separately from `Predicate` so the call sites read as the grammar
#: they accept rather than as one model that happens to allow `{}`.
PredicateInput = Predicate

Predicate.model_rebuild()


# ── scopes and anchors ────────────────────────────────────────────────


class SetScope(_Model):
    """`filter`/`map`/`aggregate` input: a node or edge set, optionally narrowed."""

    kind: Literal["nodes", "edges"]
    subtype: Optional[str] = None
    edge_type: Optional[str] = None
    filter: Optional[PredicateInput] = None


class AggregateScope(_Model):
    """`aggregate` input: the same scope, with `kind` optional.

    A separate model from `SetScope` on purpose. `aggregate` routes through
    `set_operators`, which defaults a missing kind to nodes
    (`kind = "edges" if input.get("kind") == "edges" else "nodes"`), so
    `aggregate(input={"subtype": "pact_change"}, by=[...])` has always worked
    and the docstring taught it. `filter` and `map` route through
    `operator_sql.scope_kind`, which REFUSES a missing kind with a typed
    fallback to `query`. One shared model would have to pick one of those
    behaviors and silently change the other.
    """

    kind: Literal["nodes", "edges"] = "nodes"
    subtype: Optional[str] = None
    edge_type: Optional[str] = None
    filter: Optional[PredicateInput] = None

    @model_validator(mode="after")
    def _an_empty_scope_is_not_the_whole_graph(self) -> "AggregateScope":
        """`aggregate(input={})` must not quietly become "every node".

        `kind` defaulting to nodes is what makes the legacy short form work,
        but it also means an EMPTY object dumps as `{"kind": "nodes"}`, which
        `is_set_input` reads as a set scope. An input that used to take the
        single-node path would silently start aggregating the entire node
        set — the widest possible answer to a request that named nothing.
        """
        if not self.model_fields_set:
            raise ValueError(
                "aggregate input must name a scope or an anchor: give "
                "subtype, edge_type or filter for a set, or node_id for a "
                "single node. An empty object would otherwise aggregate "
                "every node in the graph"
            )
        return self


# #3222: only `ref`, the id a retained search or `filter(retain=true)`
# returned. A scope key beside it is refused (`extra="forbid"`), and
# `SetScope` refuses `ref` the same way, so a mixed input matches no member
# of the union and is refused at the schema. The docstring is published.
class RefInput(_Model):
    """A result reference's members."""

    ref: str = Field(min_length=1, max_length=128)


class NodeAnchor(_Model):
    """A single-node anchor. `expand` starts here; `aggregate` still accepts it."""

    kind: Literal["node"] = "node"
    node_id: Optional[str] = None
    start_node: Optional[str] = None

    @model_validator(mode="after")
    def _an_anchor_must_name_a_node(self) -> "NodeAnchor":
        """An empty object is refused here as well as on `AggregateScope`.

        `aggregate`'s input is a union, and which member claims a bare `{}`
        is a question about pydantic's union resolution, not about what the
        caller meant. Refusing it on BOTH members makes the answer the same
        either way: an input that names nothing is a mistake, never "every
        node in the graph" and never an anchor on a node with no id.
        """
        if not (self.node_id or self.start_node):
            raise ValueError(
                "a node anchor must carry node_id (or start_node); an input "
                "naming nothing cannot select a node"
            )
        return self


class LiveCall(_Model):
    """Ask ONE subtype-declared live relation beside the pinned graph rows.

    `relation` names a relation `inspect` lists for the target node's
    subtype; `args` is checked against that relation's declared schema and an
    unknown key is refused by name. Results come back in the `external_live`
    envelope, marked untrusted, never merged into `neighbors` and never
    published into a generation.
    """

    relation: str
    args: dict[str, Any] = Field(default_factory=dict)
    retain: Optional[bool] = None


class SearchFilter(_CompactSchema, BaseModel):
    """`search.filter`: the three keys the ranked `search()` path reads (#2793).

    It used to be a bare `dict`, so nothing told a caller its shape, and a
    condition-style filter -- `{"field": ..., "op": "contains", "value":
    ...}` or `{"op": "and", "clauses": [...]}`, which is the `filter` TOOL's
    predicate grammar -- reached `search()` and raised a `ValueError` that
    the public boundary flattens to an opaque `operator_execution_failed`.
    The qwen3.8 probe for #2792 sent exactly that eight times.

    The PUBLISHED schema now lists the three keys and says
    `additionalProperties: false`. The VALIDATOR deliberately still allows
    other keys (`extra="allow"`), because two methods never reach
    `search()`'s key check and did accept them: the query-less `attrs`
    lookup reads `subtypes` as well, and `identifier_mentions` ignores
    `filter`. Refusing there would turn calls that work today into
    refusals. The search tool instead refuses unknown keys by name exactly
    where `search()` refused them (`refuse_unknown_search_filter_keys`),
    before the database is touched. The values are declared, not enforced,
    for the same reason: `search()` and `validate_attr_filters` still check
    them with the messages they always had.
    """

    model_config = ConfigDict(
        extra="allow", json_schema_extra={"additionalProperties": False},
    )

    subtype: declared(Any, type="string") = None
    attr_filters: declared(Any, type="object") = None
    source: declared(Any, type="string") = None


SEARCH_FILTER_KEYS: frozenset[str] = frozenset({"subtype", "attr_filters", "source"})


class SearchScope(_Model):
    """One record (`record`) or up to 100 (`records`), with their passages;
    or the children of one record (`children_of`, by `edge_type`)."""

    record: Optional[str] = None
    records: Optional[Annotated[list[str], Field(min_length=1, max_length=100)]] = None
    children_of: Optional[str] = None
    edge_type: Optional[str] = None


#: `search` arguments still accepted but no longer published (design D9,
#: PACT read-model-s3-where-scope): `where` says what they said, and every
#: one of them is lifted into it, so publishing both would charge every
#: session for two spellings of one idea.
UNPUBLISHED_SEARCH_ARGUMENTS: tuple[str, ...] = (
    "subtype_filter", "attr_filters", "filter",
)

#: The hint a refused `search.filter` carries: where a condition belongs.
SEARCH_FILTER_FIX_HINT = (
    "search.filter takes only subtype, attr_filters and source. Put exact "
    "matches in attr_filters ({key: value}); put conditions (op/field/value, "
    "prefix, suffix, lt/gt, and/or clauses) in search's `where`; text "
    "inside one attribute is contains on attrs.<key>, or filter with like "
    "to ignore accents."
)


def _substring_contains_node(value: Any) -> Any:
    """The first `contains` node in a predicate tree that no read serves.

    Served are JSON containment (field 'attrs', an object value) and text
    inside one attribute (field 'attrs.<key>', a non-empty string with no
    NUL or lone surrogate; issue #3666).

    `value` and `values` are operands, not predicates: an exact match on an
    object that happens to hold an `op` key (`{"op": "eq", "field": "attrs.x",
    "value": {"op": "contains"}}`) is data, so the walk never descends there.
    """
    if isinstance(value, Mapping):
        if value.get("op") == "contains" and not (
            (value.get("field") == "attrs" and isinstance(value.get("value"), Mapping))
            or (is_text_contains(value) and text_operand_problem(value["value"]) is None)
        ):
            return value
        children: Any = [
            child for key, child in value.items() if key not in ("value", "values")
        ]
    elif isinstance(value, list):
        children = value
    else:
        return None
    for child in children:
        found = _substring_contains_node(child)
        if found is not None:
            return found
    return None


def _words_retry(node: Mapping[str, Any], scope: Any) -> Optional[dict[str, Any]]:
    """The search call offered beside a `contains` with a string (#3666, 2c).

    `contains` with a string on one attribute is served (text inside it);
    on any other field (a column, a date, the whole attribute map) it is
    not. The offer is search for those words over the scope's one record type
    (`subtype`): a `guess`, which the refusal's words say. Offered only when
    search's own argument checks take it as written.
    """
    from okg.substrate.mcp.response_bounds import _proven_servable

    value = node.get("value")
    if not isinstance(value, str) or not value.strip() or len(value) > 200:
        return None
    if isinstance(scope, Mapping) and scope.get("kind", "nodes") != "nodes":
        return None
    # Only over one named record type: the search then reads the set the
    # caller named, and a scope with no type (or a saved result, which
    # takes no scope here) keeps the plain refusal it always had.
    subtype = scope.get("subtype") if isinstance(scope, Mapping) else None
    if not isinstance(subtype, str) or not subtype:
        return None
    arguments: dict[str, Any] = {"query": value, "where": {"type": subtype}}
    from okg.substrate.mcp.read_grammar import rewrite_call

    try:
        # The checks a dispatched call meets: the rewrite with no note, then
        # the tool body's own checks on the rewritten call.
        rewritten = rewrite_call("search", arguments)
        if rewritten.adjusted or not _proven_servable(
            "search", rewritten.arguments, ceiling=None,
        ):
            return None
    except Exception:  # noqa: BLE001 - any refusal of the offer means no offer.
        return None
    return {"tool": "search", "mode": "guess", "arguments": arguments}


def refuse_substring_contains(operator_name: str, *trees: Any, scope: Any = None) -> None:
    """Refuse a `contains` no read serves, before any lowering (issue #3067).

    `contains` has two forms: JSON containment on attrs (`field='attrs'`, an
    object `value`) and, since #3666, text inside one attribute
    (`field='attrs.<key>'`, a string `value`: the stored text holds it,
    ASCII case ignored). Any other shape -- a string on `attrs` or a stable
    column, an object on `attrs.<key>` -- used to fall through to a refusal
    that named `query` as the fallback (a tool a default reader cannot use)
    and returned `isError: false`, so a client that checks only `isError`
    read it as an empty result. Raising a typed error here makes it
    `isError: true` on every path, with or without attrs in the predicate,
    and names only what the caller can use. Text that is empty or holds NUL
    or a lone surrogate is refused by name, as `prefix` and `suffix` are.

    With `scope` (the call's `input`), a string `value` that is the call's
    whole condition also gets a `retry_call`: search for those words over
    the scope's type (#3666, 2c).
    """
    from okg.substrate.transform.operators import CONTAINS_FORMS_MESSAGE

    for tree in trees:
        node = _substring_contains_node(tree)
        if node is None:
            continue
        if is_text_contains(node):
            err = SubstrateError(
                text_operand_problem(node["value"]) or CONTAINS_FORMS_MESSAGE,
                data={"tool": operator_name, "predicate": dict(node),
                      "reason": "invalid_contains_text"},
                fix_hint="pass non-empty text with no NUL character or lone surrogate",
            )
            err.code = "unsupported_operator_form"
            raise err
        # Offered only when this test is the call's whole condition: search
        # for the words then drops nothing else the caller asked for, and
        # never stands for a test under `not` or `or`.
        retry = None
        if isinstance(scope, Mapping):
            conditions = [t for t in trees if t is not scope] + [scope.get("filter")]
            given = [c for c in conditions if c not in (None, {})]
            if len(given) == 1 and given[0] is node:
                retry = _words_retry(node, scope)
        err = SubstrateError(
            CONTAINS_FORMS_MESSAGE,
            data={"tool": operator_name, "predicate": dict(node)},
            fix_hint=(
                "search(query=...) ranks records by words; filter with "
                "{op:'contains', field:'attrs.<key>', value:'<text>'} matches "
                "text inside one attribute; "
                "{op:'suffix', field:'attrs.<key>', value:'dir/file'} matches a "
                "path tail; {op:'contains', field:'attrs', value:{<key>: "
                "<exact value>}} matches an exact attribute value"
                + (
                    "; if words in the text are meant, search for them "
                    "(a guess: see retry_call)" if retry is not None else ""
                )
            ),
        )
        err.code = "unsupported_operator_form"
        if retry is not None:
            err.retry_call = retry
        raise err


def search_filter_mapping(value: Any) -> Any:
    """A validated `SearchFilter` -> the plain mapping the search body reads.

    `exclude_unset`, not `exclude_none` (what `as_mapping` uses): an explicit
    null the caller sent stays in the mapping, and an extra key stays too, so
    the body sees exactly the dictionary it saw when `filter` was a `dict`.
    A direct Python caller's plain dict passes through untouched.
    """
    if isinstance(value, SearchFilter):
        return value.model_dump(exclude_unset=True)
    return value


def refuse_unknown_search_filter_keys(filter: Any) -> None:
    """Refuse, by name, the keys `search()` would refuse, before `_open`.

    Called only on the methods that route to `search()`, so it refuses
    exactly the calls `search()` already refused -- with
    `invalid_operator_arguments`, the bad keys and a remedy instead of an
    opaque execution failure.
    """
    if not isinstance(filter, Mapping):
        return
    unknown = sorted(str(key) for key in filter if key not in SEARCH_FILTER_KEYS)
    if not unknown:
        return
    from okg.substrate.mcp.response_bounds import (
        InvalidOperatorArguments,
        _validation_field_path,
    )

    fields = [_validation_field_path(("filter", key)) for key in unknown[:32]]
    raise InvalidOperatorArguments(
        "search input validation failed at "
        + ", ".join(repr(field) for field in fields[:8]) + ".",
        data={
            "tool": "search",
            "invalid_fields": fields,
            "validation_kinds": ["extra_forbidden"],
        },
        fix_hint=SEARCH_FILTER_FIX_HINT,
    )


ExpandInput = Union[str, NodeAnchor]
AggregateInput = Union[str, AggregateScope, NodeAnchor, RefInput]
#: `filter`/`map` input: a scope, or a result reference's members.
SetInput = Union[SetScope, RefInput]

#: `map`/`filter` projection: an ordered field list, or {alias: field}.
Projection = Optional[Union[list[str], dict[str, str]]]


# ── enums the runtime actually accepts ────────────────────────────────

SearchMethod = Literal[
    "lexical", "semantic", "alias", "identifier_mentions", "attrs", "grep"
]
#: Spelled as a tuple, not a Literal: `expand` DECLARES it (see `declared`)
#: and refuses an unknown direction in `path_diagnostics`, where the refusal
#: can name the hop it failed on.
DIRECTIONS: tuple[str, ...] = ("out", "in")
#: v1 computes one metric. `aggregate` raised on anything else; the schema
#: now says so before the call is made.
AggregateMetric = Literal["count"]


# ── inspect targets ───────────────────────────────────────────────────
#
# One member per `kind` the body routes, discriminated on `kind`. The prose
# enumeration in the docstring this replaces was the only place the grammar
# existed, and it was already out of date: it did not list `generation`,
# `generation_delta` or `view`'s `view` spelling.


class TPlain(_Model):
    kind: Literal[
        "graph", "coverage", "ontology", "session_start", "record_prefixes"
    ]


class TSubtype(_Model):
    """`detail` and `page_size` are DECLARED, not enforced — see `declared`.

    `_inspect_subtype` answers a bad one with `invalid_subtype_inspection`
    and `error.data.field`, so a caller is told WHICH field it got wrong.
    Enforcing the bound here would replace that with a generic schema
    message; `test_mcp_actionable_bounds_e2e.py` pins the envelope.
    """

    kind: Literal["subtype"]
    subtype: Optional[str] = None
    continuation_token: Optional[str] = None
    detail: declared(Any, type="string", enum=["summary", "sample_nodes"]) = None
    page_size: declared(
        Any, type="integer", minimum=1, maximum=MAX_SUBTYPE_PAGE_SIZE
    ) = None


class TView(_Model):
    kind: Literal["view"]
    name: Optional[str] = None
    view: Optional[str] = None


class TSurface(_Model):
    kind: Literal["surface"]
    action: Optional[Literal["expand"]] = None
    profile: Optional[str] = None


class TextAt(_Model):
    """A search hit's `location` (PACT read-model-s5-location)."""

    chunk: str
    offset: Annotated[int, Field(ge=0, strict=True)]
    hash: Optional[str] = None


#: `lines` of a text read: 1 to 20 `[first, last]` line ranges (#3666).
#: Only the shape is published, to keep the tool surface small:
#: `record_text.parse_lines` refuses a bad value by name with the accepted
#: form (the count, ascending, not overlapping, first <= last, an open end).
TextLines = declared(Any, type="array", items={"type": "array"})
#: The same argument on a batch item, published as a bare array.
TextItemLines = declared(Any, type="array")


class TText(_Model):
    """A record's chunk text in reading order, paged by bytes (#2668).

    `at` starts the read at a search hit's location; without `node_id` it
    reads that chunk alone (PACT read-model-s5-location). `lines` reads
    chosen line ranges of the text instead of all of it. `path` (and
    `source`) name a file instead of `node_id` (#3666 step 3b)."""

    kind: Literal["text"]
    node_id: Optional[str] = None
    path: Optional[str] = None
    source: Optional[str] = None
    at: Optional[TextAt] = None
    lines: TextLines = None
    continuation_token: Optional[str] = None


class TextBatchItem(_Model):
    model_config = ConfigDict(
        extra="forbid",
        json_schema_extra={"anyOf": [
            {"type": "object", "required": ["node_id"], "properties": {"node_id": {"type": "string", "minLength": 1}}},
            {"type": "object", "required": ["at"], "properties": {"at": {"type": "object"}}},
        ]},
    )
    key: Annotated[str, Field(min_length=1, max_length=128)]
    node_id: Optional[str] = None
    at: Optional[TextAt] = None
    lines: TextItemLines = None

    @model_validator(mode="after")
    def _has_target(self):
        if not self.node_id and self.at is None:
            raise ValueError("a batch item requires node_id or at")
        return self


class TTexts(_Model):
    kind: Literal["texts"]
    items: Annotated[list[TextBatchItem], Field(min_length=1, max_length=8)]

    @model_validator(mode="after")
    def _unique_keys(self):
        if len({item.key for item in self.items}) != len(self.items):
            raise ValueError("batch item keys must be unique")
        return self


class TImage(_Model):
    """A figure a node points at, returned as MCP image content (#3046).

    `panel` is DECLARED, not enforced, like `TSubtype`'s bounds: the body
    answers an out-of-range one by field name. There is no `dpi`: images
    render at the deployment's `agent_surface.images.render.dpi`, and a
    `dpi` key is refused as unknown."""

    kind: Literal["image"]
    node_id: str
    attr: Optional[str] = None
    panel: declared(Any, type="integer", minimum=1) = None


# `path` (and `source`) name a file instead of `node_id` (#3666 step 3b):
# served when exactly one record has that path, else refused with the
# candidates. Exactly one of the two.
class TNode(_Model):
    kind: Literal["node"]
    node_id: Optional[str] = None
    path: Optional[str] = None
    source: Optional[str] = None
    hydrate: Optional[Union[bool, list[str]]] = None

    @model_validator(mode="after")
    def _one_name(self):
        if (self.node_id is None) == (self.path is None):
            raise ValueError("pass exactly one of node_id or path")
        return self


#: Most records one `nodes` page reads (#3657); a reference with more
#: members is read this many at a time through `next`.
MAX_NODE_BATCH = 50


# Several records in one call, within one byte budget (#3657). Exactly one
# of `node_ids` or `ref` (a graph result reference from `search`, `filter` or
# `map` with retain). Top-level `select` and `max_bytes` apply to every
# record, as for `node`. A comment, not a docstring: a docstring would be
# served as the schema's description on every surface.
class TNodes(_Model):
    model_config = ConfigDict(
        extra="forbid",
        json_schema_extra={"oneOf": [
            {"required": ["node_ids"]},
            {"required": ["ref"]},
        ]},
    )
    kind: Literal["nodes"]
    node_ids: Optional[Annotated[
        list[Annotated[str, Field(min_length=1)]],
        Field(min_length=1, max_length=MAX_NODE_BATCH),
    ]] = None
    ref: Optional[Annotated[str, Field(min_length=1)]] = None

    @model_validator(mode="after")
    def _one_source(self):
        if (self.node_ids is None) == (self.ref is None):
            raise ValueError("pass exactly one of node_ids or ref")
        if self.node_ids is not None and len(set(self.node_ids)) != len(self.node_ids):
            raise ValueError("node_ids must be unique")
        return self


class TAttr(_Model):
    kind: Literal["attr", "attribute", "attribute_provenance"]
    target_kind: Optional[Literal["node", "edge"]] = None
    target_id: Optional[str] = None
    node_id: Optional[str] = None
    edge_id: Optional[str] = None
    attr_key: Optional[str] = None
    namespace: Optional[str] = None


class TSession(_Model):
    kind: Literal["session"]
    action: Optional[Literal["refresh"]] = None


class TGen(_Model):
    """`generation` reads one checkpoint; `generation_delta` previews the
    advance from `from_generation` (default: the pin) `to` a reference."""

    kind: Literal["generation", "generation_delta"]
    generation_id: Optional[Union[int, str]] = None
    from_generation: Optional[Union[int, str]] = None
    to: Optional[str] = None


class TLedger(_Model):
    """Admission-ledger reads. `record_id` narrows the `record` kind;
    `alias_suggestions` takes only `limit`."""

    kind: Literal["admission", "record", "alias_suggestions"]
    admission_id: Optional[str] = None
    record_id: Optional[str] = None
    limit: Optional[Limit] = None


class TRecordTemplates(_Model):
    kind: Literal["record_templates"]
    record_kind: Optional[str] = None


#: Every `inspect` member model, in published order, with the kinds it
#: carries. ONE table: the union, the kind list and the per-surface narrowing
#: are all derived from it, so a new kind cannot be routed without appearing
#: in the published schema.
INSPECT_TARGET_MEMBERS: tuple[tuple[type, tuple[str, ...]], ...] = (
    (TPlain, ("graph", "coverage", "ontology", "session_start",
              "record_prefixes")),
    (TSubtype, ("subtype",)),
    (TView, ("view",)),
    (TSurface, ("surface",)),
    (TNode, ("node",)),
    (TNodes, ("nodes",)),
    (TText, ("text",)),
    (TTexts, ("texts",)),
    (TImage, ("image",)),
    (TAttr, ("attr", "attribute", "attribute_provenance")),
    (TSession, ("session",)),
    (TGen, ("generation", "generation_delta")),
    (TLedger, ("admission", "record", "alias_suggestions")),
    (TRecordTemplates, ("record_templates",)),
)

INSPECT_TARGET_KINDS: tuple[str, ...] = (
    "graph", "coverage", "ontology", "session_start", "record_prefixes",
    "subtype", "view", "surface", "node", "nodes", "text", "texts", "image", "attr", "session",
    "generation",
    "generation_delta", "admission", "record", "record_templates",
    "alias_suggestions",
)

#: `attr`'s two spellings are aliases of one kind, not kinds of their own:
#: `INSPECT_TARGET_KINDS` lists `attr` alone while the model accepts all
#: three. A narrowing that names `attr` therefore serves all three.
INSPECT_KIND_ALIASES: Mapping[str, str] = {
    "attribute": "attr",
    "attribute_provenance": "attr",
}


def _kind_key(kind: str) -> str:
    """The name a narrowing uses for `kind` — aliases fold onto their kind."""
    return INSPECT_KIND_ALIASES.get(kind, kind)


@functools.lru_cache(maxsize=None)
def _narrow_member(model: type, kept: tuple[str, ...]) -> type:
    """`model` with its `kind` Literal cut down to `kept`.

    A member model, not a member LIST, is narrowed. Dropping whole members
    instead would force one kind per model, and splitting `TPlain` into two
    members to separate `record_prefixes` from `graph` measured +168 B on the
    UNNARROWED union — a cost paid by the write/admin surface, which serves
    every kind and should not move at all when a narrowing it does not use is
    added. Narrowing the Literal leaves the full union byte-identical.
    """
    from pydantic import create_model

    narrowed = create_model(
        model.__name__,
        __base__=model,
        kind=(Literal[kept], ...),  # type: ignore[valid-type]
    )
    narrowed.__qualname__ = model.__qualname__
    narrowed.__doc__ = model.__doc__
    return narrowed


def _narrowed_member(
    model: type, member_kinds: tuple[str, ...], served: frozenset[str],
) -> type:
    kept = tuple(k for k in member_kinds if _kind_key(k) in served)
    if len(kept) == len(member_kinds):
        return model
    return _narrow_member(model, kept)


@functools.lru_cache(maxsize=None)
def build_inspect_target(kinds: frozenset[str]) -> type:
    """The `inspect` target union for exactly `kinds`.

    Built BEFORE registration, from the same table the router guard reads, so
    the published schema and the runtime answer cannot disagree. Pruning the
    rendered `oneOf` after registration would not do: the SDK validates
    against the pydantic model, not against the rendering, so a pruned schema
    over an unpruned validator would accept an admin kind the surface claims
    not to serve.

    Cached because `build_server` is called per process but the test suite
    builds many servers, and rebuilding a pydantic union is not free.
    """
    members: tuple[type, ...] = tuple(
        _narrowed_member(model, member_kinds, kinds)
        for model, member_kinds in INSPECT_TARGET_MEMBERS
        if any(_kind_key(k) in kinds for k in member_kinds)
    )
    if not members:
        raise ValueError(
            "an inspect target union needs at least one member; "
            f"{sorted(kinds)} selects none of "
            f"{[m.__name__ for m, _ in INSPECT_TARGET_MEMBERS]}"
        )
    if len(members) == 1:
        # A one-member Union is not a union, and pydantic refuses a
        # discriminator on it. Publish the member itself.
        annotation: Any = members[0]
    else:
        annotation = Annotated[
            Union[members], Field(discriminator="kind"),
        ]

    # Named `InspectTarget` AT CREATION, not renamed afterwards: pydantic
    # derives the `$defs` key from the class name when it builds the core
    # schema, so a later rename leaves `_InspectTarget` on the wire. That
    # measured +2 B on every surface — one byte in the `$defs` key and one in
    # the `$ref` — which is a byte baseline moving for a reason that has
    # nothing to do with what the surface serves.
    class InspectTarget(_CompactSchema, RootModel[annotation]):  # type: ignore[valid-type]
        """What `inspect` describes, discriminated on `kind`.

        A RootModel rather than a bare `Annotated` alias so the compaction
        hook reaches the union node itself; spelled as an alias, pydantic
        renders the union outside any model and the hook never sees it.
        """

    InspectTarget.__qualname__ = "InspectTarget"
    return InspectTarget


#: The unnarrowed union: every kind. Kept as a module-level name because it is
#: what `inspect`'s own annotation declares and what a caller importing the
#: model expects.
InspectTarget = build_inspect_target(frozenset(INSPECT_TARGET_KINDS))

PREDICATE_OPS: tuple[str, ...] = (
    "and", "or", "not", "eq", "in", "exists", "contains", "suffix",
    "prefix", "lt", "gt", "between", "matches", "like", "like_exact",
)


class InspectSchemaUnreachable(SubstrateError):
    """The registered `inspect` tool's published schema could not be reached."""

    code = "inspect_schema_unreachable"


def publish_narrowed_inspect_target(mcp: Any, kinds: frozenset[str]) -> None:
    """Publish `inspect`'s target union narrowed to `kinds`, without narrowing
    the VALIDATOR.

    The two must be separated, and an earlier attempt to do it with one model
    is why. Putting the narrowed model on the tool signature made pydantic
    reject an unserved kind before the body ran, so the caller got
    `invalid_operator_arguments` with `union_tag_invalid` — a schema-shaped
    message that cannot tell a kind that does not exist from one this launch
    withholds. The named refusal (`agent_surface_inspect_kind_unserved`) was
    unreachable, and the router guard that raises it was dead code.

    So the signature keeps the FULL union, every kind reaches the body, and
    the guard answers by name. This rewrites only what `tools/list` shows,
    which is what the byte baselines measure.

    That leaves the published schema stricter than the validator, which is
    normally the dangerous direction: a schema that hides a kind the
    validator accepts would silently serve it. It does not here, because the
    guard refuses exactly the kinds this call removes, from the same set.
    The guard is what makes the narrowing safe, so neither may be changed
    without the other — `test_the_published_schema_and_the_guard_agree` pins
    that.
    """
    manager = getattr(mcp, "_tool_manager", None)
    tools = getattr(manager, "_tools", None) if manager is not None else None
    tool = tools.get("inspect") if tools else None
    schema = getattr(tool, "parameters", None) if tool is not None else None
    target = (
        schema.get("properties", {}).get("target")
        if isinstance(schema, dict) else None
    )
    if not isinstance(target, dict):
        # RAISE rather than return. Every step above reaches into SDK
        # internals (`_tool_manager`, `_tools`, `Tool.parameters`), and a
        # rename in a dependency upgrade would otherwise make this a silent
        # no-op: the server would publish every withheld kind while the
        # guard refused them, and the byte baselines would move with no
        # explanation. Same discipline as `session_tool_surface`'s own
        # internals guard.
        raise InspectSchemaUnreachable(
            "could not reach the registered `inspect` tool's published input "
            "schema through the MCP SDK internals "
            "(_tool_manager._tools['inspect'].parameters.properties.target). "
            "Refusing rather than publishing the unnarrowed kind list in "
            "silence: the surface would advertise kinds the router refuses.",
            data={
                "has_tool_manager": manager is not None,
                "has_tools": tools is not None,
                "has_inspect": tool is not None,
                "has_parameters": isinstance(schema, dict),
            },
            fix_hint=(
                "an MCP SDK upgrade likely renamed these internals; update "
                "publish_narrowed_inspect_target to match"
            ),
        )

    if kinds >= frozenset(INSPECT_TARGET_KINDS):
        # Every kind is served, so the union pydantic already rendered IS the
        # narrowed one. Returning early keeps the published bytes identical
        # to what this tool served before the split existed, which is what
        # makes the write/admin byte baseline a usable control: it moves only
        # if the narrowing leaked into a surface that serves everything.
        return

    narrowed = build_inspect_target(kinds).model_json_schema()
    member_names = {model.__name__ for model, _ in INSPECT_TARGET_MEMBERS}
    pool = {
        name: body for name, body in (schema.get("$defs") or {}).items()
        if name not in member_names
    }
    pool.update(narrowed.pop("$defs", {}))
    schema["properties"]["target"] = {
        key: value for key, value in narrowed.items()
        if key not in {"$defs", "title"}
    }

    # Keep only what the narrowed schema can still reach. The original root
    # model's own `$defs` entry carries the FULL union and becomes
    # unreferenced the moment `target` is replaced; leaving it behind
    # published every withheld kind anyway and measured +291 B, which is the
    # whole point of this call going the wrong way.
    reachable: set[str] = set()

    def _walk(node: Any) -> None:
        if isinstance(node, dict):
            ref = node.get("$ref")
            if isinstance(ref, str) and ref.startswith("#/$defs/"):
                name = ref.removeprefix("#/$defs/")
                if name not in reachable:
                    reachable.add(name)
                    _walk(pool.get(name))
            for key, value in node.items():
                if key != "$ref":
                    _walk(value)
        elif isinstance(node, list):
            for item in node:
                _walk(item)

    _walk({k: v for k, v in schema.items() if k != "$defs"})
    kept = {name: body for name, body in pool.items() if name in reachable}
    if kept:
        schema["$defs"] = kept
    elif "$defs" in schema:
        del schema["$defs"]


def compact_tool_schemas(mcp: Any) -> int:
    """Compact the registered tools' published input schemas in place.

    The models above compact their own `$defs`, but the OUTER argument object
    is built by the SDK from each function signature, and it spends roughly
    35 bytes per parameter on a `"title"` derived from the parameter name and
    on `"anyOf":[X,{"type":"null"}],"default":null` for every optional one —
    about 2 KB across the read surface, paid by every session at startup.

    Only redundant bytes are dropped: a derived title, and the null branch of
    an optional parameter that the `required` list already accounts for. A
    `description` is NEVER dropped here — some tools carry real per-field
    guidance — and no constraint, enum, type or `required` entry is touched.
    Validation is unaffected: the SDK validates against the pydantic model,
    not against this rendering, so the published schema ends up stricter than
    the runtime rather than looser.

    Returns the number of bytes removed, so a caller can report the saving.
    """
    import json

    manager = getattr(mcp, "_tool_manager", None)
    tools = getattr(manager, "_tools", None)
    if not tools:
        return 0
    saved = 0
    for tool in tools.values():
        schema = getattr(tool, "parameters", None)
        if not isinstance(schema, dict):
            continue
        before = len(json.dumps(schema, separators=(",", ":")))
        compacted = compact_schema(schema, keep_descriptions=True)
        saved += before - len(json.dumps(compacted, separators=(",", ":")))
        tool.parameters = compacted
    return saved


# ── schemas a weaker tool-calling client can read ─────────────────────
#
# Issue #2792. A model served through Ollama (qwen3.8:27B, the cms-kb public
# chat model) converts each tool argument by the TOP-LEVEL `type` its schema
# declares. An argument spelled only as a `$ref` pointer, or as a
# `oneOf`/`anyOf` union with no `type` beside it, has no type the parser can
# see, so the model's object came back as a string holding JSON, and the
# server refused it: 16 of 16 object arguments in a three-question loop.
# Inlining the pointers and stating `"type": "object"` on every object
# argument made the same model send 8 of 8 as objects.
#
# The pydantic models above stay exactly as they are: they are what the SDK
# VALIDATES against (`Tool.fn_metadata.arg_model`, and FastMCP registers its
# call handler with `validate_input=False`, so the published `inputSchema`
# is never used to accept or refuse a call). Only `Tool.parameters`, the
# published copy, is rewritten here, after the last tool is registered.

#: How many times one `$defs` entry may be expanded inside ITSELF on one path.
#:
#: Only `Predicate` refers to itself (`clause`, `clauses`), and no `$ref` can
#: remain on the wire, so its recursion has to stop somewhere. At 1 the whole
#: grammar is shown once -- the closed fifteen-op set, `field`, `value`,
#: `values` -- and a nested `clause`/`clauses` item is published as a plain
#: object whose description says it is another predicate of the same shape.
#: That nested node still carries `"type": "object"`, which is the property
#: the Ollama parser reads. Each extra level doubles the copies (a predicate
#: has two self-references). Measured on the reader surface's seven input
#: schemas (8,780 B with pointers): +489 B at depth 1, +4,159 B at depth 2,
#: +11,499 B at depth 3 -- the deeper levels buy no information a caller
#: does not already have from the level above. The runtime still validates
#: nested predicates to any depth; this bounds only what is PUBLISHED.
INLINE_RECURSION_DEPTH = 1


class ToolSchemaUnpublishable(SubstrateError):
    """A registered tool's input schema could not be made client-readable."""

    code = "tool_schema_unpublishable"


_NAME_MAPS = frozenset({"$defs", "definitions", "properties", "patternProperties"})
#: Keywords whose value is DATA, not a schema: a default or an enum member
#: that happens to contain a `$ref` or `anyOf` key must be published as is.
_VALUE_KEYWORDS = frozenset({"default", "const", "enum", "examples"})
#: Sibling keywords of a `$ref` that only ANNOTATE. Pydantic emits
#: `{"$ref": ..., "description": ...}` for `Annotated[Model, Field(
#: description=...)]`; the field's own text overrides the definition's, as
#: it does in JSON Schema 2020-12, instead of refusing the launch.
_ANNOTATION_KEYWORDS = frozenset({"title", "description", "default", "examples"})


def inline_schema_refs(
    schema: Mapping[str, Any],
    *,
    recursion_depth: int = INLINE_RECURSION_DEPTH,
) -> dict[str, Any]:
    """`schema` with every local `$ref` replaced by the definition it names.

    A definition reached again while it is already being expanded
    `recursion_depth` times on the current path is CUT: replaced by a node of
    the definition's own `type` (when it states one) whose `description`
    names the definition, so the cut is visible on the wire rather than a
    silently looser schema.

    A `$ref` that is not local (`#/$defs/<name>`), names a missing
    definition, or carries a sibling VALIDATION keyword that conflicts with
    the definition raises `ToolSchemaUnpublishable` rather than publishing a
    schema that says something different from the model. A conflicting
    annotation (`description`, `title`, `default`, `examples`) is the
    field's own override and wins.
    """
    defs = schema.get("$defs") or {}

    def walk(node: Any, stack: tuple[str, ...], in_map: bool) -> Any:
        if isinstance(node, list):
            return [walk(item, stack, False) for item in node]
        if not isinstance(node, dict):
            return node
        if in_map:
            return {name: walk(body, stack, False) for name, body in node.items()}
        ref = node.get("$ref")
        if ref is None:
            return {
                key: (
                    value if key in _VALUE_KEYWORDS
                    else walk(value, stack, key in _NAME_MAPS)
                )
                for key, value in node.items()
                if key != "$defs"
            }
        if not (isinstance(ref, str) and ref.startswith("#/$defs/")):
            raise ToolSchemaUnpublishable(
                f"cannot inline the non-local schema pointer {ref!r}",
                data={"ref": str(ref)[:200]},
            )
        name = ref.removeprefix("#/$defs/")
        body = defs.get(name)
        if not isinstance(body, dict):
            raise ToolSchemaUnpublishable(
                f"schema pointer {ref!r} names no definition",
                data={"ref": ref, "defined": sorted(defs)[:32]},
            )
        if stack.count(name) >= recursion_depth:
            resolved: dict[str, Any] = {
                "description": f"{name}: same shape as the enclosing one",
            }
            kind = body.get("type")
            if kind is None:
                # A recursive union (a JSON-value alias, say) states no type
                # of its own; publish its branches' union when they all
                # state one, and otherwise leave the cut open, as the
                # recursion itself was.
                kind = type_schema_unions(body).get("type")
            if kind is not None:
                resolved["type"] = kind
        else:
            resolved = walk(body, stack + (name,), False)
        for key, value in node.items():
            if key == "$ref":
                continue
            if key not in _VALUE_KEYWORDS:
                value = walk(value, stack, key in _NAME_MAPS)
            if key in _ANNOTATION_KEYWORDS:
                resolved[key] = value
                continue
            if key in resolved and resolved[key] != value:
                raise ToolSchemaUnpublishable(
                    f"inlining {ref!r} would overwrite its {key!r}",
                    data={"ref": ref, "keyword": key},
                )
            resolved[key] = value
        return resolved

    return walk(dict(schema), (), False)


def _branch_types(branch: Any) -> Optional[list[str]]:
    if not isinstance(branch, dict):
        return None
    kind = branch.get("type")
    if isinstance(kind, str):
        return [kind]
    if isinstance(kind, list) and all(isinstance(k, str) for k in kind):
        return list(kind)
    return None


def type_schema_unions(schema: Any, *, in_map: bool = False) -> Any:
    """Give every `anyOf`/`oneOf` without a `type` the union of its branches'.

    `{"anyOf": [{"type": "string"}, {"type": "object", ...}]}` gains
    `"type": ["string", "object"]`; a `oneOf` of objects gains
    `"type": "object"`. This restates what the branches already say, so it
    neither admits nor refuses anything a JSON Schema validator would not
    already. A union with a branch that declares no type is left alone:
    stating a type there would publish a narrower schema than the model's,
    and `test_mcp_schema_client_readable.py` then names the argument.
    """
    if isinstance(schema, list):
        return [type_schema_unions(item) for item in schema]
    if not isinstance(schema, dict):
        return schema
    if in_map:
        return {name: type_schema_unions(body) for name, body in schema.items()}
    out = {
        key: (
            value if key in _VALUE_KEYWORDS
            else type_schema_unions(value, in_map=key in _NAME_MAPS)
        )
        for key, value in schema.items()
    }
    if "type" in out:
        return out
    for keyword in ("oneOf", "anyOf"):
        branches = out.get(keyword)
        if not isinstance(branches, list) or not branches:
            continue
        types: list[str] = []
        for branch in branches:
            found = _branch_types(branch)
            if found is None:
                return out
            types.extend(k for k in found if k not in types)
        out["type"] = types[0] if len(types) == 1 else types
        return out
    return out


def _describe_next_alternative(name: str, schema: dict[str, Any]) -> None:
    """Say in the property descriptions that `next` replaces the main argument.

    Not a top-level `anyOf` of the two entry forms: Claude Code 2.1.292
    drops a tool whose input schema has one (PACT
    mcp-schema-no-top-level-anyof). The body still refuses a call with
    neither, as `invalid_operator_arguments`.
    """
    argument = {"inspect": "target", "expand": "input"}.get(name)
    properties = schema.get("properties", {})
    if argument is None or "next" not in properties or argument not in properties:
        return
    properties[argument]["description"] = "Required unless `next` is given." + (
        # #3647 (`nestedargs A`): agents put select and max_bytes in target.
        " What to read; select and max_bytes go beside it, not in it."
        if name == "inspect" else ""
    )
    properties["next"]["description"] = f"Use instead of `{argument}` to continue a page."


def _unpublish(schema: dict[str, Any], names: tuple[str, ...]) -> None:
    """Drop accepted-but-unpublished arguments from a published schema.

    Only the published copy: the SDK validates calls against the tool's
    argument model, which keeps every one of them.
    """
    properties = schema.get("properties")
    if isinstance(properties, dict):
        for name in names:
            properties.pop(name, None)
    required = schema.get("required")
    if isinstance(required, list):
        schema["required"] = [name for name in required if name not in names]


def _drop_published_op(node: Any, op: str) -> None:
    """Remove `op` from every published `op` enum under `node` (published
    copy only; the argument model still validates the call)."""
    if isinstance(node, dict):
        properties = node.get("properties")
        if isinstance(properties, dict):
            enum = properties.get("op", {}).get("enum") if isinstance(
                properties.get("op"), dict) else None
            if isinstance(enum, list) and op in enum:
                properties["op"]["enum"] = [value for value in enum if value != op]
        for value in node.values():
            _drop_published_op(value, op)
    elif isinstance(node, list):
        for value in node:
            _drop_published_op(value, op)


def publish_client_readable_schemas(mcp: Any) -> int:
    """Rewrite every registered tool's PUBLISHED input schema for weak parsers.

    Four changes, all to `Tool.parameters` only:

    * every `$ref` is inlined (`inline_schema_refs`) and `$defs` removed;
    * every union gets a top-level `type` (`type_schema_unions`);
    * a read operator's `max_bytes` publishes `minimum`
      `MIN_RESPONSE_BYTES`, the floor the byte-bound wrapper raises a
      smaller value to and discloses (`max_bytes_raised`, #3153), so a
      validating client learns the floor before it calls. No maximum: the ceiling is per deployment, and #2141 decision
      3A declined publishing the substrate maximum for that reason;
    * `inspect.target`, `expand.input` and their `next` say in their
      descriptions that `next` replaces the main argument
      (`_describe_next_alternative`), never as a top-level `anyOf`.

    Returns the byte change across all tools (positive: grew), so the caller
    can see what inlining cost. Raises `ToolSchemaUnpublishable` if the SDK
    internals moved: a silent no-op would republish the schemas #2792 is
    about, and nothing downstream would notice.
    """
    import json

    from okg.substrate.mcp.operators import OPERATOR_TOOLS
    from okg.substrate.mcp.response_bounds import MIN_RESPONSE_BYTES

    manager = getattr(mcp, "_tool_manager", None)
    tools = getattr(manager, "_tools", None)
    if not isinstance(tools, dict):
        raise ToolSchemaUnpublishable(
            "could not reach the registered tools through the MCP SDK "
            "internals (_tool_manager._tools); refusing to publish schemas "
            "a weaker tool-calling client cannot read",
            data={"has_tool_manager": manager is not None},
            fix_hint=(
                "an MCP SDK upgrade likely renamed these internals; update "
                "publish_client_readable_schemas to match"
            ),
        )
    grown = 0
    for name, tool in tools.items():
        schema = getattr(tool, "parameters", None)
        if not isinstance(schema, dict):
            continue
        before = len(json.dumps(schema, separators=(",", ":")))
        published = type_schema_unions(inline_schema_refs(schema))
        if name == "search":
            _unpublish(published, UNPUBLISHED_SEARCH_ARGUMENTS)
            # #3666: search's `where` refuses a text condition (the set
            # tools take it), so its published operator list leaves it out.
            _drop_published_op(published, "matches")
            # A substring test is filter's (loose-match-3666): search's
            # `where` refuses like and like_exact, naming the filter call.
            _drop_published_op(published, "like")
            _drop_published_op(published, "like_exact")
        budget = published.get("properties", {}).get("max_bytes")
        if name in OPERATOR_TOOLS and isinstance(budget, dict):
            budget["minimum"] = MIN_RESPONSE_BYTES
        _describe_next_alternative(name, published)
        grown += len(json.dumps(published, separators=(",", ":"))) - before
        tool.parameters = published
    return grown


def publish_clean_descriptions(mcp: Any) -> int:
    """Publish every registered tool's description without its source indent.

    The SDK publishes a tool function's raw docstring, so each line after
    the first carried the source file's indentation: 740 B, or 96 cl100k
    tokens, of the read surface (Jason, via orchestrator chat 2026-10-07,
    `readtrim A`; folded into PACT surface-token-budget, which gates that
    surface in tokens). `inspect.cleandoc` removes
    the common indent and the trailing blank lines and keeps relative
    indentation; no word changes.

    Returns the bytes removed across all tools. Raises
    `ToolSchemaUnpublishable` if the SDK internals moved, as
    `publish_client_readable_schemas` does: a silent no-op would republish
    the indented text and nothing downstream would notice.
    """
    import inspect

    manager = getattr(mcp, "_tool_manager", None)
    tools = getattr(manager, "_tools", None)
    if not isinstance(tools, dict):
        raise ToolSchemaUnpublishable(
            "could not reach the registered tools through the MCP SDK "
            "internals (_tool_manager._tools); refusing to publish tool "
            "descriptions with their source indentation",
            data={"has_tool_manager": manager is not None},
            fix_hint=(
                "an MCP SDK upgrade likely renamed these internals; update "
                "publish_clean_descriptions to match"
            ),
        )
    removed = 0
    for tool in tools.values():
        description = getattr(tool, "description", None)
        if not isinstance(description, str):
            continue
        cleaned = inspect.cleandoc(description)
        removed += len(description.encode("utf-8")) - len(cleaned.encode("utf-8"))
        tool.description = cleaned
    return removed


# ── the operator result envelope ──────────────────────────────────────


class _OutputModel(BaseModel):
    """Base for the declared result shape.

    Two deliberate differences from `_Model`:

    * `extra="allow"`, because this envelope describes the fields EVERY
      operator shares, not the whole payload. `inspect` alone returns
      seventeen different bodies, and a model that forbade their keys would
      refuse almost every real response. The schema says what a client can
      rely on; it does not pretend to enumerate what it cannot.
    * `keep_nulls=True` in the rendering. See `compact_schema`: an output
      schema that forbids null would be violated by a payload that really
      does send `logical_session_id: null`.
    """

    model_config = ConfigDict(extra="allow")

    @classmethod
    def __get_pydantic_json_schema__(cls, core_schema, handler):  # type: ignore[no-untyped-def]
        return compact_schema(handler(core_schema), keep_nulls=True)

    #: On the SDK success path, a declared scalar is COERCED to its declared
    #: type — `3.0` becomes `3`, `1` becomes `True`, `"7"` becomes `7` — and
    #: a value that cannot be coerced (`3.5` for an integer id) is refused.
    #: A returned refusal takes a distinct path: it is validated against this
    #: envelope but its raw dictionary is served unchanged. Undeclared keys
    #: pass through untouched on both paths because `extra="allow"` has no
    #: declared type to coerce them to.

    def model_dump(self, **kwargs: Any) -> dict[str, Any]:
        """Dump only the keys the operator actually produced.

        `exclude_unset` defaults to True here, and that default is the whole
        point. The SDK converts a structured result with
        `validated.model_dump(mode="json", by_alias=True)` and no exclusions,
        which would add every one of this envelope's thirty-odd declared
        fields to EVERY response as null — several hundred bytes of nothing,
        on a surface that exists to keep responses small, and a visible
        change to payloads that callers already read.

        With `extra="allow"`, a key the operator sent is "set" whether it is
        declared here or not, so excluding the unset ones reproduces the
        operator's own dictionary exactly. A caller that genuinely wants the
        null-padded form can still pass `exclude_unset=False`.
        """
        kwargs.setdefault("exclude_unset", True)
        return super().model_dump(**kwargs)


class OperatorError(_OutputModel):
    """A refusal, with its producer metadata bounded.

    `code`, `message` and `fix_hint` are the part a caller acts on and are
    never trimmed. `data` is capped at `ERROR_DATA_CAP_BYTES` (1 KiB
    serialized) where the structured envelope is built; over the cap it is
    REPLACED by `{"truncated": true, "data_bytes": <n>}` plus a `trace_id`
    when a trace is active, rather than by a shortened version that would
    read as the whole record. `data` stays an open object because producers
    put their own keys in it; the marker is what tells a reader which of the
    two it is holding.
    """

    code: Optional[str] = None
    message: Optional[str] = None
    fix_hint: Optional[str] = None
    data: Optional[dict[str, Any]] = None
    max_bytes: Optional[int] = None
    remedy: Optional[str] = None


class OperatorResult(_OutputModel):
    """What every read operator returns, declared once.

    The field names are the ones the operators ALREADY emit; nothing was
    renamed to fit a tidier model, because renaming would break every caller
    to make a schema look neater. The grouping below is the contract a client
    can read pagination and provenance from without parsing prose.

    What is NOT declared is as deliberate as what is. `extra="allow"` means
    every operator-specific key still passes, and a handful of envelope
    fields are left undeclared because they are echoes or advice rather than
    contract — `operator_name` repeats the tool you called, `returned_count`
    is `len(items)`, `response_byte_budget` repeats `max_bytes`,
    `pagination_hint` is prose, and `warning` is advisory. Each one declared
    costs about 45 bytes on all seven tools at every connect, which is the
    budget this surface is measured against.
    """

    # rows — the row-bearing key depends on the operator's shape
    items: Optional[list[Any]] = None
    results: Optional[list[Any]] = None
    neighbors: Optional[list[Any]] = None
    rows: Optional[list[Any]] = None
    #: Column names, or `{name, type}` objects from `inspect` kind `view`
    #: (issue #2793: the string-only type refused every view describe).
    columns: Optional[list[Union[str, dict[str, str]]]] = None

    # cursor
    limit: Optional[int] = None
    offset: Optional[int] = None
    next_offset: Optional[int] = None
    total_count: Optional[int] = None
    continuation_token: Optional[str] = None

    # truncation
    truncated: Optional[bool] = None
    truncation_reason: Optional[str] = None

    # generation and session pin
    generation_id: Optional[Union[int, str]] = None
    pinned_generation_id: Optional[Union[int, str]] = None
    pin_scope: Optional[str] = None
    logical_session_id: Optional[str] = None

    # freshness — how old the sources behind this answer are
    source_freshness: Optional[dict[str, Any]] = None
    publish_freshness: Optional[dict[str, Any]] = None

    # refusal
    status: Optional[str] = None
    #: A typed refusal carries the structured shape; several older paths
    #: (`expand`'s "start_node is not visible at the pinned generation",
    #: queued-work rows, a stringified exception) put a bare message here
    #: instead. Both are real and the schema says so, because a model that
    #: admitted only the object turned those perfectly good answers into a
    #: generic execution failure — which is exactly how this was found.
    error: Optional[Union[OperatorError, str]] = None

    #: `inspect(target={"kind":"node","hydrate":...})` only: state fetched
    #: from the backing system NOW, labeled outside the generation pin.
    external_live: Optional[dict[str, Any]] = None

    #: `inspect(target={"kind":"subtype"|"node"})` (issue #2013): the live
    #: relations this subtype declares, each with the argument schema the
    #: dispatcher will enforce. Declared here so an agent never has to guess
    #: an argument name; absent, never `{}`, when the subtype declares none.
    live_relations: Optional[dict[str, Any]] = None


class MinimalResult(_OutputModel):
    """The envelope the six paging operators publish.

    `inspect` publishes `OperatorResult`, the full envelope: it returns the
    widest range of bodies, so the permissive superset is the schema that
    actually fits it, and a client that wants the complete field list —
    freshness, the session pin contract, the typed error shape — reads
    `inspect`'s `outputSchema` once.

    The other six publish this instead, and the reason is measured rather
    than aesthetic. The full envelope costs about 1.2 KB, and seven copies of
    it added 8.7 KB to what every session downloads before asking a single
    question — 39% of the whole tool surface, to say the same thing seven
    times. This subset keeps what a caller needs to PAGE and to know WHICH
    GENERATION answered, which is the part of the contract that changes how
    the next call is written. `additionalProperties` is left open, so every
    other field an operator sends is still valid; it is simply documented in
    one place rather than seven.
    """

    #: Row-bearing key, named per operator by the subclasses below.
    total_count: Optional[int] = None
    next_offset: Optional[int] = None
    continuation_token: Optional[str] = None
    truncated: Optional[bool] = None
    pinned_generation_id: Optional[Union[int, str]] = None
    status: Optional[str] = None
    #: Declared on every operator, not just `inspect`: a refusal is an answer
    #: a caller has to read, and req.ooc.error-bound makes its size a
    #: contract rather than whatever the producer happened to attach.
    #:
    #: Spelled as an open object here rather than as `OperatorError`, for the
    #: same reason the rest of this model is a subset: the typed six-field
    #: shape costs about 350 bytes in each of the six schemas that would
    #: repeat it, 2.1 KB at every connect, and `inspect` already publishes it
    #: once. What these six add is that a refusal HAS an `error`, and that it
    #: is an object or a string.
    error: Optional[Union[dict[str, Any], str]] = None


class PageResult(MinimalResult):
    """`filter`, `map`, `aggregate`: rows arrive as `items`."""

    items: Optional[list[Any]] = None


class SearchResult(MinimalResult):
    """`search`: ranked rows arrive as `results`."""

    results: Optional[list[Any]] = None


class ExpandResult(MinimalResult):
    """`expand`: rows arrive as `neighbors`."""

    neighbors: Optional[list[Any]] = None
    #: `expand(live=...)` (issue #2013): rows from a subtype-declared live
    #: relation, BESIDE `neighbors` and never merged into them. Declared here
    #: rather than left to `additionalProperties`, so a client reading the
    #: output schema knows the envelope exists and is separate.
    external_live: Optional[dict[str, Any]] = None


class QueryResult(MinimalResult):
    """`query`: SQL rows arrive as `rows`, with their `columns`."""

    rows: Optional[list[Any]] = None
    columns: Optional[list[str]] = None


#: Which result model each read operator publishes.
OPERATOR_RESULT_MODELS: dict[str, type[_OutputModel]] = {
    "inspect": OperatorResult,
    "search": SearchResult,
    "expand": ExpandResult,
    "filter": PageResult,
    "map": PageResult,
    "aggregate": PageResult,
    "query": QueryResult,
}
