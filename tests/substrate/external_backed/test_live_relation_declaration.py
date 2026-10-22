"""Declaration, binding and the load-time refusals (task.lre.declare).

Issue #2013, PACT `live-relations-on-expand`. Every test here is hermetic:
nothing touches a database, a policy bundle or an adapter. What is proved is
that a malformed declaration fails at LOAD, by name, with the offending
relation or key in the message -- so a deployment learns at start rather than
on an agent's first call.
"""
from __future__ import annotations

import pytest

from okg.substrate.external_backed.live_relations import (
    ADAPTER_KINDS,
    ANNOTATION_KEY,
    BoundLiveRelation,
    LiveBindingUndeclaredRelation,
    LiveRelationActionMissing,
    LiveRelationAdapterKindUnknown,
    LiveRelationArgSchemaInvalid,
    LiveRelationCallerModeUnsupported,
    LiveRelationInvalid,
    LiveRelationPrincipalModeInvalid,
    LiveRelationSamplePolicyInvalid,
    LiveRelationUnbound,
    bind_live_relations,
    compile_args_model,
    parse_live_bindings,
    parse_live_relations,
)

HITS = {
    "action": "external.opensearch.search",
    "principal_mode": "service",
    "sample_policy": "digest",
    "retention_class": "warm",
    "deadline_seconds": 10,
    "args": {
        "q": {"type": "string", "required": True},
        "size": {"type": "integer", "default": 10},
    },
}


def _ann(**relations):
    return {ANNOTATION_KEY: relations or {"hits": dict(HITS)}}


def _parse(**relations):
    return parse_live_relations(_ann(**relations), subtype="search_index")


# ── the happy path, and its non-vacuity partner ───────────────────────

def test_a_declared_relation_compiles_with_its_argument_schema() -> None:
    relations = _parse()
    assert set(relations) == {"hits"}
    hits = relations["hits"]
    assert hits.action == "external.opensearch.search"
    assert hits.subtype == "search_index"
    assert hits.principal_mode == "service"
    assert hits.sample_policy == "digest"
    assert hits.retention_class == "warm"
    assert hits.deadline_seconds == 10.0
    assert [(a.name, a.type_name, a.required) for a in hits.args] == [
        ("q", "string", True), ("size", "integer", False),
    ]


def test_a_subtype_with_no_annotation_declares_no_relations() -> None:
    """Absent is normal and is NOT an error; malformed always is.

    The pair matters: if "absent" and "malformed" both returned {}, a typo in
    the annotation key would read as a subtype that simply declares nothing.
    """
    assert parse_live_relations({}, subtype="paper") == {}
    assert parse_live_relations({"archetype": "thing"}, subtype="paper") == {}
    with pytest.raises(LiveRelationInvalid):
        parse_live_relations({ANNOTATION_KEY: "not-a-mapping"}, subtype="p")


def test_the_published_block_is_what_the_dispatcher_validates_against() -> None:
    """`inspect` publishes the COMPILED declaration, not the raw annotation.

    Rendering both from one object is what stops the advertised schema and
    the enforced schema drifting apart.
    """
    hits = _parse()["hits"]
    published = hits.published()
    assert published["args"] == {
        "q": {"type": "string", "required": True},
        "size": {"type": "integer", "default": 10},
    }
    model = compile_args_model(hits)
    assert set(model.model_fields) == set(published["args"])


# ── the load-time refusals, one test each ─────────────────────────────

def test_a_relation_without_a_declared_action_is_refused_at_load() -> None:
    body = {k: v for k, v in HITS.items() if k != "action"}
    with pytest.raises(LiveRelationActionMissing) as exc:
        _parse(hits=body)
    assert "hits" in str(exc.value)
    assert exc.value.code == "live_relation_action_missing"


def test_caller_principal_mode_is_refused_at_deployment_load() -> None:
    """SERVICE MODE ONLY this sprint -- refused, never downgraded.

    Downgrading to `service` would run the call under the runtime's own
    principal while the deployment believed it ran under the caller's: a
    silent privilege change.
    """
    with pytest.raises(LiveRelationCallerModeUnsupported) as exc:
        _parse(hits={**HITS, "principal_mode": "caller"})
    message = str(exc.value)
    assert "hits" in message
    assert "caller" in message
    assert "NOT served yet" in message
    assert exc.value.code == "live_relation_caller_mode_unsupported"


def test_an_unknown_principal_mode_is_refused_by_name() -> None:
    with pytest.raises(LiveRelationPrincipalModeInvalid):
        _parse(hits={**HITS, "principal_mode": "root"})


@pytest.mark.parametrize("key,value", [
    ("sample_policy", "everything"),
    ("retention_class", "forever"),
])
def test_an_unknown_sample_policy_or_retention_class_is_refused(
    key: str, value: str,
) -> None:
    with pytest.raises(LiveRelationSamplePolicyInvalid) as exc:
        _parse(hits={**HITS, key: value})
    assert value in str(exc.value)


@pytest.mark.parametrize("action", ["search", "External.Search", "a..b", ""])
def test_a_malformed_action_is_refused_rather_than_denied_forever(
    action: str,
) -> None:
    """A typo'd action matches no bundle rule and would deny silently.

    That is the failure this refusal exists to prevent: "denied" looks like
    policy working, so nobody investigates.
    """
    with pytest.raises((LiveRelationInvalid, LiveRelationActionMissing)):
        _parse(hits={**HITS, "action": action})


@pytest.mark.parametrize("args,why", [
    ("not-a-mapping", "args must be a mapping"),
    ({"q": "not-a-mapping"}, "one argument must be a mapping"),
    ({"q": {"type": "date"}}, "unsupported type"),
    ({"q": {"type": "string", "nope": 1}}, "unknown argument key"),
    ({"q": {"type": "string", "required": True, "default": "x"}},
     "required and default together"),
    ({"q": {"type": "integer", "default": "ten"}}, "default of the wrong type"),
])
def test_a_malformed_argument_schema_is_refused_at_load(args, why: str) -> None:
    with pytest.raises(LiveRelationArgSchemaInvalid):
        _parse(hits={**HITS, "args": args})


@pytest.mark.parametrize("deadline", [0, -1, 121, "ten", True])
def test_an_absent_or_unbounded_deadline_is_refused(deadline) -> None:
    """No live call may be unbounded: it holds an agent's turn open on a
    system this graph does not control."""
    with pytest.raises(LiveRelationInvalid):
        _parse(hits={**HITS, "deadline_seconds": deadline})


def test_an_unknown_relation_key_is_refused_by_name() -> None:
    with pytest.raises(LiveRelationInvalid) as exc:
        _parse(hits={**HITS, "timeout": 5})
    assert "timeout" in str(exc.value)
    # `description` is now accepted; its neighbours are not.
    for key in ("descriptions", "doc", "help"):
        with pytest.raises(LiveRelationInvalid) as exc:
            _parse(hits={**HITS, "description": "ok", key: "x"})
        assert key in str(exc.value)


# ── description: operator text `inspect` shows (item 7) ─────

#: What a deployment would say on example_list_site_attributes.
QUOTA_TEXT = (
    "Site attributes. quota is the space the system may fill (an "
    "allocation), not the site's capacity; for capacity call "
    "example_get_site_usage and read source `static`."
)


def test_a_description_is_published_by_inspect_and_round_trips() -> None:
    import json

    from okg.substrate.external_backed.live_relations import LiveRelation

    hits = _parse(hits={**HITS, "description": f"  {QUOTA_TEXT}\n"})["hits"]
    assert hits.description == QUOTA_TEXT, "trimmed, otherwise verbatim"
    assert hits.published()["description"] == QUOTA_TEXT
    stored = json.loads(json.dumps(hits.to_metadata()))
    rebuilt = LiveRelation.from_metadata("hits", "search_index", stored)
    assert rebuilt == hits
    assert rebuilt.published()["description"] == QUOTA_TEXT


def test_no_description_publishes_no_key() -> None:
    """Absent stays absent: the published block of every existing
    declaration is unchanged."""
    published = _parse()["hits"].published()
    assert "description" not in published
    assert set(published) == {"action", "args", "sample_policy", "retention_class"}


def test_a_description_of_exactly_the_limit_is_accepted() -> None:
    from okg.substrate.external_backed.live_relations import MAX_DESCRIPTION_CHARS

    text = "x" * MAX_DESCRIPTION_CHARS
    assert _parse(hits={**HITS, "description": text})["hits"].description == text


@pytest.mark.parametrize(
    "value, why",
    [
        pytest.param("x" * 401, "401 characters", id="too-long"),
        pytest.param(["quota", "is an allocation"], "list", id="list"),
        pytest.param(42, "int", id="number"),
        pytest.param({"text": "x"}, "dict", id="mapping"),
        pytest.param("   ", "str", id="blank"),
    ],
)
def test_a_bad_description_is_refused_at_load(value, why: str) -> None:
    with pytest.raises(LiveRelationInvalid) as exc:
        _parse(hits={**HITS, "description": value})
    assert "description" in str(exc.value)
    assert why in str(exc.value)
    assert exc.value.data["relation"] == "hits"


def test_a_stored_overlong_description_is_refused_when_rebuilt() -> None:
    """A catalog composed by other code cannot publish an unbounded text."""
    from okg.substrate.external_backed.live_relations import LiveRelation

    stored = _parse()["hits"].to_metadata()
    stored["description"] = "y" * 500
    with pytest.raises(LiveRelationInvalid) as exc:
        LiveRelation.from_metadata("hits", "search_index", stored)
    assert "stored in the catalog" in str(exc.value)


# ── the argument model ────────────────────────────────────────────────

def test_an_unknown_argument_key_is_refused_by_name() -> None:
    model = compile_args_model(_parse()["hits"])
    with pytest.raises(Exception) as exc:
        model(q="x", sizee=10)
    assert "sizee" in str(exc.value)


def test_a_missing_required_argument_is_refused() -> None:
    model = compile_args_model(_parse()["hits"])
    with pytest.raises(Exception):
        model(size=10)


def test_the_validated_dump_applies_declared_defaults() -> None:
    """The cache fingerprint is taken from THIS, which is why it matters."""
    model = compile_args_model(_parse()["hits"])
    assert model(q="x").model_dump() == {"q": "x", "size": 10}


# ── bindings ──────────────────────────────────────────────────────────

def _bindings(**over):
    body = {"hits": {"kind": "http", "config": {"url": "http://localhost"}}}
    body.update(over)
    return parse_live_bindings(body)


def test_a_well_formed_binding_parses() -> None:
    bound = _bindings()["hits"]
    assert bound.kind == "http"
    assert bound.config == {"url": "http://localhost"}
    assert bound.secret_refs == ()


def test_an_unknown_adapter_kind_is_refused_naming_the_known_set() -> None:
    with pytest.raises(LiveRelationAdapterKindUnknown) as exc:
        _bindings(hits={"kind": "opensearch"})
    message = str(exc.value)
    assert "opensearch" in message
    for kind in ADAPTER_KINDS:
        assert kind in message


def test_a_binding_may_not_raise_its_own_principal_mode() -> None:
    """`principal_mode` belongs to the SUBTYPE, not to the deployment.

    A deployment that could set it would widen a relation's authority without
    touching the ontology that declared it.
    """
    with pytest.raises(LiveRelationInvalid) as exc:
        _bindings(hits={"kind": "http", "principal_mode": "caller"})
    assert "principal_mode" in str(exc.value)


def test_a_declared_relation_with_no_binding_fails_load() -> None:
    with pytest.raises(LiveRelationUnbound) as exc:
        bind_live_relations(_parse(), {})
    assert "hits" in str(exc.value)
    assert exc.value.code == "live_relation_unbound"


def test_a_binding_naming_no_declared_relation_fails_load() -> None:
    """An adapter configured to never run is the operator's typo."""
    with pytest.raises(LiveBindingUndeclaredRelation) as exc:
        bind_live_relations(_parse(), _bindings(hit={"kind": "http"}))
    assert "hit" in str(exc.value)


def test_binding_pairs_the_declaration_with_its_adapter() -> None:
    bound = bind_live_relations(_parse(), _bindings())
    assert set(bound) == {"hits"}
    assert isinstance(bound["hits"], BoundLiveRelation)
    assert bound["hits"].relation.action == "external.opensearch.search"
    assert bound["hits"].binding.kind == "http"


# ── argument bounds (security review B1/B2) ───────────────────────────

BOUNDED = {
    "action": "external.opensearch.search",
    "deadline_seconds": 5,
    "args": {
        "q": {"type": "string", "required": True, "maxLength": 200},
        "size": {"type": "integer", "default": 10, "minimum": 1,
                 "maximum": 100},
        "field": {"type": "string", "default": "@timestamp",
                  "enum": ["@timestamp", "event.created"]},
    },
}


def _bounded():
    return parse_live_relations(
        {ANNOTATION_KEY: {"hits": dict(BOUNDED)}}, subtype="search_index",
    )["hits"]


def test_an_optional_argument_must_declare_a_default() -> None:
    """No implicit null. B2.

    An argument that is neither required nor defaulted would arrive as
    `None`, and a null reaching an adapter is an UNBOUNDED call: a missing
    time window is a full scan. The declaration has to say which it means.
    """
    with pytest.raises(LiveRelationArgSchemaInvalid) as exc:
        _parse(hits={**HITS, "args": {"time_window": {"type": "string"}}})
    assert "time_window" in str(exc.value)
    assert "required" in str(exc.value)


def test_the_compiled_model_is_non_nullable_so_a_default_cannot_be_voided(
) -> None:
    """B2, the mechanism: `T` with a default, never `T | None`."""
    model = compile_args_model(_bounded())
    assert model(q="x").model_dump()["size"] == 10
    with pytest.raises(Exception):
        model(q="x", size=None)


@pytest.mark.parametrize("bad,why", [
    ({"q": "x", "size": 0}, "under minimum"),
    ({"q": "x", "size": 101}, "over maximum"),
    ({"q": "y" * 201}, "over maxLength"),
    ({"q": "x", "field": "evil.script"}, "outside enum"),
])
def test_a_value_outside_its_declared_bounds_is_refused(bad, why: str) -> None:
    """A typed slot is only as safe as its type. B1."""
    model = compile_args_model(_bounded())
    with pytest.raises(Exception):
        model(**bad)


def test_the_bounds_are_published_so_an_agent_is_told_them() -> None:
    """Refusal is not documentation: `inspect` carries the limits."""
    published = _bounded().published()["args"]
    assert published["q"]["maxLength"] == 200
    assert published["size"]["minimum"] == 1
    assert published["size"]["maximum"] == 100
    assert published["field"]["enum"] == ["@timestamp", "event.created"]


@pytest.mark.parametrize("args,why", [
    ({"n": {"type": "integer", "default": 1, "maxLength": 5}},
     "maxLength on an integer"),
    ({"s": {"type": "string", "default": "x", "minimum": 1}},
     "minimum on a string"),
])
def test_a_constraint_the_type_does_not_take_is_refused_not_ignored(
    args, why: str,
) -> None:
    """An ignored constraint reads as an enforced one."""
    with pytest.raises(LiveRelationArgSchemaInvalid) as exc:
        _parse(hits={**HITS, "args": args})
    assert "does not take" in str(exc.value)


@pytest.mark.parametrize("args,why", [
    ({"s": {"type": "string", "default": "x", "enum": ["a", "b"]}},
     "default outside its own enum"),
    ({"s": {"type": "string", "default": "x", "pattern": "([unclosed"}},
     "invalid regex"),
    ({"n": {"type": "integer", "default": 5, "minimum": 10, "maximum": 1}},
     "minimum above maximum"),
    ({"s": {"type": "string", "default": "x", "maxLength": 0}},
     "non-positive maxLength"),
    ({"s": {"type": "string", "default": "x", "enum": []}}, "empty enum"),
])
def test_an_unsatisfiable_constraint_is_refused_at_load(args, why: str) -> None:
    """A bound nothing can satisfy is a mistake, not a very strict rule."""
    with pytest.raises(LiveRelationArgSchemaInvalid):
        _parse(hits={**HITS, "args": args})


# ── bound and type hygiene (round-3 review, items 7/8 + notes) ────────

@pytest.mark.parametrize("args,why", [
    ({"n": {"type": "integer", "default": True}}, "bool default on integer"),
    ({"n": {"type": "integer", "default": 1, "enum": [True, 2]}},
     "bool inside an integer enum"),
])
def test_a_boolean_is_not_an_integer(args, why: str) -> None:
    """`isinstance(True, int)` is True in Python, and that is not what an
    operator writing `default: true` on an integer meant."""
    with pytest.raises(LiveRelationArgSchemaInvalid):
        _parse(hits={**HITS, "args": args})


@pytest.mark.parametrize("bound,value,why", [
    ("minimum", "abc", "non-numeric raised a bare ValueError"),
    ("minimum", True, "boolean silently became 1.0"),
    ("maximum", [1], "a list is not a bound"),
])
def test_a_non_numeric_bound_is_refused_by_name(bound, value, why: str) -> None:
    with pytest.raises(LiveRelationArgSchemaInvalid) as exc:
        _parse(hits={**HITS, "args": {
            "n": {"type": "integer", "default": 1, bound: value},
        }})
    assert bound in str(exc.value)
    assert "not a number" in str(exc.value)


def test_a_pattern_is_compiled_by_the_engine_that_enforces_it() -> None:
    """Python's `re` accepts constructs pydantic's engine rejects.

    A lookahead passes `re.compile` and then raises inside `create_model`,
    turning an operator's typo into a crash at server build instead of a
    named refusal at load.
    """
    with pytest.raises(LiveRelationArgSchemaInvalid) as exc:
        _parse(hits={**HITS, "args": {
            "s": {"type": "string", "default": "x",
                  "pattern": "(?=a)b", "maxLength": 10},
        }})
    assert "engine that enforces it" in str(exc.value)


def test_a_pattern_requires_a_length_bound() -> None:
    """A regex over an unbounded string is how one becomes a DoS."""
    with pytest.raises(LiveRelationArgSchemaInvalid) as exc:
        _parse(hits={**HITS, "args": {
            "s": {"type": "string", "default": "x", "pattern": "^[a-z]+$"},
        }})
    assert "maxLength" in str(exc.value)


def test_an_anchored_bounded_pattern_is_accepted_and_enforced() -> None:
    """Non-vacuity partner, and the documented shape: anchor it, bound it."""
    relation = _parse(hits={**HITS, "args": {
        "s": {"type": "string", "default": "abc",
              "pattern": "^[a-z]+$", "maxLength": 10},
    }})["hits"]
    assert relation.published()["args"]["s"]["pattern"] == "^[a-z]+$"
    model = compile_args_model(relation)
    assert model(s="abc").model_dump()["s"] == "abc"
    with pytest.raises(Exception):
        model(s="ABC")


# ── one argument order on both paths (issue #2144) ─────────────────────

#: Declared deliberately OUT of name order. `from_metadata` used to sort
#: while `parse_live_relations` kept this order, so the relation the composer
#: built and the one the dispatcher rebuilt compared unequal.
_UNSORTED_ARGS = {
    "size": {"type": "integer", "default": 10, "minimum": 1, "maximum": 50},
    "q": {"type": "string", "required": True, "maxLength": 32},
    "after": {"type": "string", "default": "0", "enum": ["0", "10"]},
}


def test_the_catalog_round_trip_is_an_identity_for_unsorted_args() -> None:
    """`from_metadata(to_metadata(r)) == r`, through JSON as jsonb stores it.

    The catalog column is jsonb, which does not keep object-key order, so
    declaration order cannot survive storage. Both paths therefore use name
    order; this pins that they agree.
    """
    import json

    from okg.substrate.external_backed.live_relations import LiveRelation

    declared = _parse(hits={**HITS, "args": dict(_UNSORTED_ARGS)})["hits"]
    stored = json.loads(json.dumps(declared.to_metadata()))
    rebuilt = LiveRelation.from_metadata("hits", "search_index", stored)

    assert rebuilt == declared
    assert [a.name for a in declared.args] == ["after", "q", "size"]
    assert list(declared.published()["args"]) == ["after", "q", "size"]


def test_argument_order_does_not_depend_on_declaration_order() -> None:
    """Two declarations that differ only in key order are one declaration."""
    forward = _parse(hits={**HITS, "args": dict(_UNSORTED_ARGS)})["hits"]
    backward = _parse(hits={**HITS, "args": dict(
        reversed(list(_UNSORTED_ARGS.items())),
    )})["hits"]
    assert forward == backward


# ── an argument pattern must match the WHOLE value (issue #2148) ───────

@pytest.mark.parametrize("pattern,why", [
    ("[A-Za-z0-9 ]{1,60}", "no anchors: accepts '!!bad!!' via a substring"),
    ("^[a-z]+", "start anchor only"),
    ("[a-z]+$", "end anchor only"),
    ("^[a-z]+\\$", "the trailing dollar is an escaped literal"),
    ("^a|b$", "top-level alternation: '^a' OR 'b$'"),
    ("(?m)^[a-z]+$", "multi-line mode makes ^ and $ line anchors"),
    ("^(?m)[a-z]+$", "multi-line mode switched on mid-pattern"),
    ("^[a-z]+$[0-9]", "the dollar is not the last token"),
])
def test_an_unanchored_pattern_is_refused_at_load(pattern, why: str) -> None:
    """pydantic applies `pattern` as a SEARCH, as JSON Schema does.

    An operator who writes `[A-Za-z0-9 ]{1,60}` believes the alphabet is
    restricted; only `maxLength` bounds anything. Refused by name, with the
    relation and the argument, so the mistake is a load error and not an
    open argument.
    """
    with pytest.raises(LiveRelationArgSchemaInvalid) as exc:
        _parse(hits={**HITS, "args": {
            "q": {"type": "string", "required": True,
                  "pattern": pattern, "maxLength": 60},
        }})
    message = str(exc.value)
    assert "'hits'" in message and "'q'" in message, message
    assert "anchor" in message
    assert exc.value.data["relation"] == "hits"
    assert exc.value.data["arg"] == "q"
    assert exc.value.data["reason"] == "pattern_unanchored"
    assert exc.value.code == "live_relation_arg_schema_invalid"


@pytest.mark.parametrize("pattern", [
    "^[a-z]+$",
    "\\A[a-z]+\\z",
    "^(?:red|green)$",
    "^[|]+$",
    "^a\\|b$",
    "^[a-z]+\\\\$",
    "(?i)^[a-z]+$",
    "(?m)\\A[a-z]+\\z",
    "^[[:alpha:]]+$",
    "^[]|]+$",
    "^[^]|]+$",
])
def test_an_anchored_pattern_is_accepted(pattern: str) -> None:
    """Non-vacuity partner: the refusal is not unconditional."""
    relation = _parse(hits={**HITS, "args": {
        "q": {"type": "string", "required": True,
              "pattern": pattern, "maxLength": 60},
    }})["hits"]
    assert relation.args[0].pattern == pattern


@pytest.mark.parametrize("pattern,why", [
    ("^?[a-z]+$", "`^?` makes the start anchor optional; '!!ab' passes"),
    ("^*[a-z]+$", "`^*` repeats the anchor zero times"),
    ("^{0}[a-z]+$", "`^{0}` matches the anchor zero times"),
    ("^+[a-z]+$", "a quantified anchor is refused whatever the quantifier"),
    ("\\A?[a-z]+\\z", "`\\A?` makes the text anchor optional"),
])
def test_a_quantified_start_anchor_is_refused(pattern, why: str) -> None:
    """Review finding 1: a start anchor followed by a quantifier is no anchor.

    pydantic accepts `!!ab` against `^?[a-z]+$`: the `?` lets the search
    skip the anchor and match `ab` inside the value.
    """
    with pytest.raises(LiveRelationArgSchemaInvalid) as exc:
        _parse(hits={**HITS, "args": {
            "q": {"type": "string", "required": True,
                  "pattern": pattern, "maxLength": 60},
        }})
    assert exc.value.data["reason"] == "pattern_unanchored"
    assert "quantifier" in str(exc.value)


@pytest.mark.parametrize("pattern,why", [
    ("(?x)^a#(\n|.*$", "a `#` comment hides the `(`: the engine sees ^a|.*$"),
    ("^(?x)a#(\n|.*$", "the same, with the flag switched on mid-pattern"),
    ("^(?x:a)$", "group-scoped verbose mode"),
    ("(?ix)^a$", "verbose among other flags"),
])
def test_a_verbose_mode_pattern_is_refused(pattern, why: str) -> None:
    """Review finding 2: in verbose mode `#` starts a comment.

    A comment can hide a `(` from a syntactic check, so `(?x)^a#(\n|.*$`
    reads as one group to the check while the engine sees a top-level `|`
    and accepts anything. Verbose mode is refused outright.
    """
    with pytest.raises(LiveRelationArgSchemaInvalid) as exc:
        _parse(hits={**HITS, "args": {
            "q": {"type": "string", "required": True,
                  "pattern": pattern, "maxLength": 60},
        }})
    assert exc.value.data["reason"] == "pattern_unanchored"
    assert "verbose" in str(exc.value)


def test_a_stored_unanchored_pattern_is_refused_when_rebuilt() -> None:
    """A catalog composed before the load refusal still carries the pattern.

    Rebuilding it without the check would dispatch an argument the operator
    believed was restricted. Same precedent as `principal_mode: caller`.
    """
    from okg.substrate.external_backed.live_relations import LiveRelation

    stored = _parse()["hits"].to_metadata()
    stored["args"]["q"] = {"type": "string", "required": True,
                           "pattern": "[a-z]+", "maxLength": 60}
    with pytest.raises(LiveRelationArgSchemaInvalid) as exc:
        LiveRelation.from_metadata("hits", "search_index", stored)
    assert exc.value.data["reason"] == "pattern_unanchored"
    assert exc.value.data["arg"] == "q"
    # Fixing the schema is not enough: the STORED catalog still carries the
    # old pattern until it is recomposed, so the hint says both.
    assert "okg catalog load" in (exc.value.fix_hint or "")


# ── the recompose hint names a real command (issue #2201) ──────────────

def test_the_recompose_hint_names_a_registered_cli_command() -> None:
    """The hint used to say `okg catalog apply`, which does not exist.

    The command inside the hint's backticks is parsed by the real `okg`
    argument parser, with each `<placeholder>` filled in, so a hint naming
    an unregistered verb or flag fails here rather than on an operator.
    """
    import re

    from okg.substrate.cli.main import _build_parser
    from okg.substrate.external_backed.live_relations import LiveRelation

    with pytest.raises(LiveRelationInvalid) as exc:
        LiveRelation.from_metadata("hits", "search_index", {"action": "a.b"})
    hint = exc.value.fix_hint or ""
    found = re.search(r"`okg ([^`]+)`", hint)
    assert found, hint
    # `<a|b>` is a choice the operator fills in: take its first option.
    # Any other `<placeholder>` is free text.
    argv = [
        (token.strip("<>").split("|")[0] if "|" in token else "placeholder")
        if token.startswith("<") else token
        for token in found.group(1).split()
    ]
    parsed = _build_parser(argv).parse_args(argv)
    assert argv[:2] == ["catalog", "load"], hint
    assert parsed.apply is True
    # The writer scope is part of the command `okg catalog ownership claim`
    # prints (`cli/catalog.py`), so the hint names it too.
    assert "--writer-scope" in hint
