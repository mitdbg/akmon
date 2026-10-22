"""`run`'s argument layer (issue #3666): steps, references, and the rewrite
of a reference into a condition the read tools take. No database."""
from __future__ import annotations

import pytest

from okg.substrate.mcp import run_steps
from okg.substrate.mcp.attr_filter_source import pushable_clauses


def _sets(*keys: str, fields: tuple[str, ...] = ()) -> run_steps.RunSets:
    sets = run_steps.RunSets()
    for key in keys:
        entry = run_steps.StepSet(raw_sql=f"SELECT 'id-{key}'")
        for field in fields:
            entry.values[field] = run_steps.ValueList(raw_sql="SELECT to_jsonb('T-1'::text)")
        sets.sets[key] = entry
    return sets


def test_steps_are_checked_before_anything_runs():
    # Eight since live steps (#3666, lane ri): a listing, a fan-out, a total.
    with pytest.raises(run_steps.RunRefusal, match="at most 8 steps"):
        run_steps.parse_steps([{"filter": {}}] * 9)
    assert len(run_steps.parse_steps([{"filter": {}}] * 8)) == 8
    with pytest.raises(run_steps.RunRefusal, match="a step is one of"):
        run_steps.parse_steps([{"query": {"sql": "select 1"}}])
    with pytest.raises(run_steps.RunRefusal, match="a step is one of"):
        run_steps.parse_steps([{"filter": {}, "map": {}}])
    with pytest.raises(run_steps.RunRefusal, match="show is true or false"):
        run_steps.parse_steps([{"filter": {}, "show": "no"}])
    with pytest.raises(run_steps.RunRefusal, match="continues a reply on its own"):
        run_steps.parse_steps([{"filter": {"next": "tok"}}])
    steps = run_steps.parse_steps([{"filter": {"where": {}}, "show": False}, {"map": {}}])
    assert [(s.index, s.operator, s.show) for s in steps] == [
        (1, "filter", False), (2, "map", True),
    ]


def test_by_default_only_the_last_step_prints_rows():
    steps = run_steps.parse_steps([{"filter": {}}, {"map": {}}, {"aggregate": {}}])
    assert [s.show for s in steps] == [False, False, True]
    steps = run_steps.parse_steps([{"filter": {}, "show": True}, {"map": {}}])
    assert [s.show for s in steps] == [True, True]


def test_a_reference_to_step_zero_a_later_step_or_past_the_last_is_refused_with_the_steps():
    """Failure cases, refused before any step runs; the refusal names the
    steps that exist."""
    cases = [
        ([{"filter": {"where": "$0"}}, {"map": {}}], r"names \$0.*steps 1 to 2"),
        ([{"filter": {}}, {"map": {"where": "$2"}}], r"step 2 names \$2.*steps 1 to 1"),
        ([{"filter": {"where": "$3"}}, {"map": {}}], r"step 1 names \$3.*no earlier step"),
        ([{"filter": {}}, {"map": {"where": "$9"}}], r"names \$9.*this call has steps 1 to 2"),
    ]
    for raw, message in cases:
        with pytest.raises(run_steps.RunRefusal, match=message):
            run_steps.check_references(run_steps.parse_steps(raw))
    named = run_steps.check_references(run_steps.parse_steps([
        {"filter": {}}, {"map": {"where": "$1"}}, {"aggregate": {"where": {
            "op": "and", "clauses": ["$1", {"attrs.k": "$2.attrs.k"}]}}},
    ]))
    assert named == {1, 2}


def test_references_are_found_where_a_condition_or_a_set_stands():
    (step,) = run_steps.parse_steps([{"filter": {"where": {"op": "and", "clauses": [
        "$1", {"attrs.key": "$2.attrs.k"}, {"op": "not", "clause": "$3"}]}}}])
    assert run_steps.step_references(step) == {1: set(), 2: {"attrs.k"}, 3: set()}
    (search,) = run_steps.parse_steps([{"search": {"query": "$1"}}])
    assert run_steps.step_references(search) == {}
    (inspect,) = run_steps.parse_steps([{"inspect": {"target": {"kind": "nodes", "set": "$4"}}}])
    assert run_steps.step_references(inspect) == {4: set()}


def test_a_reference_becomes_a_marker_leaf_the_compiler_reads():
    sets = _sets("1", "2", fields=("attrs.k",))
    (step,) = run_steps.parse_steps([{"filter": {"where": {"op": "and", "clauses": [
        "$1", {"attrs.key": "$2.attrs.k"}]}}}])
    bound = run_steps.bind_step(step, sets, inspect_ids=lambda n: [])
    first, second = bound["where"]["clauses"]
    assert first == {"op": "in", "field": "node_id", "values": [sets.set_marker("1")]}
    assert second == {"attrs.key": [sets.values_marker("2", "attrs.k")]}
    with run_steps.registry_scope(sets):
        assert run_steps.holds_marker(bound["where"])
        sql = run_steps.lower_marker(
            first, kind="nodes", columns={"node_id": "target_id"}, relation="r",
            attrs_sql="r.attrs", readable=False,
        )
        assert sql == (
            "(r.target_id IS NOT NULL AND r.target_id = ANY(ARRAY(SELECT run_set.v "
            "FROM (SELECT 'id-1') AS run_set(v) WHERE run_set.v IS NOT NULL)))"
        )
        values = run_steps.lower_marker(
            {"op": "in", "field": "attrs.key", "values": [sets.values_marker("2", "attrs.k")]},
            kind="nodes", columns={}, relation="r", attrs_sql="r.attrs", readable=False,
        )
        assert values.startswith(
            "((r.attrs -> 'key') IS NOT NULL AND (r.attrs -> 'key') IN (SELECT run_values.v"
        )
        # Under the reader's view a marker is not lowered: the compile refuses.
        assert run_steps.lower_marker(
            first, kind="nodes", columns={"node_id": "target_id"}, relation="r",
            attrs_sql="r.attrs", readable=True,
        ) is None
    # Outside its run a marker is an ordinary string.
    assert not run_steps.holds_marker(bound["where"])
    assert run_steps.lower_marker(
        first, kind="nodes", columns={}, relation="r", attrs_sql="r.attrs", readable=False,
    ) is run_steps.NOT_A_MARKER


def test_a_marker_lowers_to_an_index_probe_or_a_hashed_test_never_null():
    """#3666: ``COALESCE(x IN (<set>), FALSE)`` hid `x` from its index, so a
    statement over a set read every record. A positive test of the record
    id is now ``x = ANY(ARRAY(<set>))`` (an index probe); a test under
    ``not`` or of another field is ``x IN (<set>)`` (hashed: ``ANY`` over an
    array is not). Every form is ``x IS NOT NULL AND ..`` over a null-free
    set, so it is never null: a null `x` (an expand's far end outside the
    view, a missing attribute) is false, and true under ``not``, as before."""
    sets = _sets("1", fields=("attrs.k", "node_id"))
    columns = {"node_id": "target_id", "subtype": "subtype"}
    whole = {"op": "in", "field": "node_id", "values": [sets.set_marker("1")]}
    attr = {"op": "in", "field": "attrs.key", "values": [sets.values_marker("1", "attrs.k")]}
    ids = {"op": "in", "field": "node_id", "values": [sets.values_marker("1", "node_id")]}
    kind = {"op": "in", "field": "subtype", "values": [sets.values_marker("1", "node_id")]}

    def lower(node):
        return run_steps.lower_marker(
            node, kind="nodes", columns=columns, relation="r",
            attrs_sql="r.attrs", readable=False,
        )

    with run_steps.registry_scope(sets):
        marked = run_steps.mark_negated({"op": "and", "clauses": [
            whole, {"op": "not", "clause": {"op": "or", "clauses": [
                ids, {"op": "not", "clause": attr}]}}]})
        positive, negated = marked["clauses"][0], marked["clauses"][1]["clause"]["clauses"][0]
        double = marked["clauses"][1]["clause"]["clauses"][1]["clause"]
        assert run_steps.NEGATED_KEY not in positive and run_steps.NEGATED_KEY not in double
        assert negated[run_steps.NEGATED_KEY] is True
        # The marking copies: the bound condition is not changed.
        assert run_steps.NEGATED_KEY not in ids
        sql = {name: lower(node) for name, node in (
            ("whole", whole), ("attr", attr), ("ids", ids), ("kind", kind),
            ("not whole", {**whole, run_steps.NEGATED_KEY: True}),
            ("not ids", negated),
            ("not attr", {**attr, run_steps.NEGATED_KEY: True}))}
    for name, text in sql.items():
        assert "COALESCE" not in text, name
        # Never null: x is tested first, and the set holds no null.
        assert text.startswith("(") and " IS NOT NULL AND " in text, name
    assert sql["whole"] == (
        "(r.target_id IS NOT NULL AND r.target_id = ANY(ARRAY(SELECT run_set.v "
        "FROM (SELECT 'id-1') AS run_set(v) WHERE run_set.v IS NOT NULL)))"
    )
    assert sql["not whole"] == (
        "(r.target_id IS NOT NULL AND r.target_id IN (SELECT run_set.v "
        "FROM (SELECT 'id-1') AS run_set(v) WHERE run_set.v IS NOT NULL))"
    )
    assert sql["ids"].startswith("(r.target_id IS NOT NULL AND r.target_id = ANY(ARRAY(")
    assert sql["not ids"].startswith("(r.target_id IS NOT NULL AND r.target_id IN (SELECT ")
    target = "(r.attrs -> 'key')"
    for name in ("attr", "not attr"):
        assert sql[name].startswith(f"({target} IS NOT NULL AND {target} IN (SELECT "), name
        assert "run_values.v IS NOT NULL AND jsonb_typeof(run_values.v) <> 'null'" in sql[name]
    assert sql["kind"].startswith("(r.subtype IS NOT NULL AND r.subtype IN (SELECT ")
    assert "WHERE jsonb_typeof(run_values.v) = 'string')" in sql["kind"]
    # Failure case: a field with no column still cannot be lowered (refused).
    with run_steps.registry_scope(sets):
        assert lower({"op": "in", "field": "date.created",
                      "values": [sets.values_marker("1", "node_id")]}) is None


PLAIN = {"type": "code_reference", "attrs.candidate_count": {"op": "exists"}}


@pytest.mark.parametrize(("definition", "inlined"), [
    (("records", "nodes", PLAIN), True),
    (("records", "nodes", {"op": "and", "clauses": [
        {"op": "eq", "field": "subtype", "value": "item"},
        {"op": "in", "field": "node_id", "values": ["a", "b"]},
        {"op": "not", "clause": {"attrs.k": 1}}]}), True),
    # Not plain: a text clause, a date meaning, an operator-as-key object.
    (("records", "nodes", {"type": "doc", "text": {"op": "matches", "value": "z"}}), False),
    (("records", "nodes", {"op": "matches", "field": "attrs.title", "value": "z"}), False),
    (("records", "nodes", {"date.created": {"op": "gte", "value": "2024"}}), False),
    (("records", "nodes", {"op": "and", "clauses": [
        {"op": "gte", "field": "date.created", "value": "2024"}]}), False),
    (("records", "nodes", {"attrs.k": {"in": ["a"]}}), False),
    # Not a node set, or no condition.
    (("records", "edges", {"edge_type": "x"}), False),
    (("expand", "n:1", "x", "out", None), False),
    (("records", "nodes", None), False),
    (("records", "nodes", {}), False),
])
def test_only_a_plain_node_condition_is_inlinable(definition, inlined):
    got = run_steps.inlinable_predicate(definition)
    assert got == (definition[2] if inlined else None)


def _bind(operator: str, arguments: dict, sets: run_steps.RunSets) -> dict:
    (step,) = run_steps.parse_steps([{operator: arguments}])
    return run_steps.bind_step(step, sets, inspect_ids=lambda n: [])


def test_a_whole_set_with_a_plain_condition_is_read_as_that_condition():
    """#3666: "$1" over a plain node set is bound as the set's own
    condition, not as the marker leaf read as a membership subquery, in
    each tool that reads node records; under `not` too."""
    sets = _sets("1", "2", fields=("attrs.k",))
    sets.sets["1"].predicate = PLAIN
    leaf2 = {"op": "in", "field": "node_id", "values": [sets.set_marker("2")]}
    values = {"op": "in", "field": "attrs.k", "values": [sets.values_marker("1", "attrs.k")]}
    where = {"op": "and", "clauses": [
        "$1", {"op": "not", "clause": "$1"}, "$2", {"attrs.k": "$1.attrs.k"}]}
    expected = {"op": "and", "clauses": [
        PLAIN, {"op": "not", "clause": PLAIN}, leaf2, {"attrs.k": values["values"]}]}
    for operator in ("filter", "map", "aggregate"):
        assert _bind(operator, {"where": where}, sets)["where"] == expected
    scoped = _bind("map", {"input": {"kind": "nodes", "filter": "$1"}}, sets)
    assert scoped["input"]["filter"] == PLAIN
    # The bound condition is a copy: the set's own condition never changes.
    _bind("filter", {"where": "$1"}, sets)["where"]["attrs.candidate_count"]["op"] = "x"
    assert PLAIN["attrs.candidate_count"] == {"op": "exists"}


def test_a_set_is_a_marker_leaf_where_its_condition_could_mean_other_records():
    """Failure cases: over edges, one record or a saved result the set's
    node condition would read other records, so the marker leaf stays (and
    is refused or lowered as before); a set with no plain condition too."""
    sets = _sets("1", "2")
    sets.sets["1"].predicate = PLAIN
    leaf1 = {"op": "in", "field": "node_id", "values": [sets.set_marker("1")]}
    leaf2 = {"op": "in", "field": "node_id", "values": [sets.set_marker("2")]}
    assert _bind("filter", {"where": "$2"}, sets)["where"] == leaf2
    for scope in ({"kind": "edges"}, {"kind": "nodes", "ref": "r1"}):
        bound = _bind("filter", {"input": scope, "where": "$1"}, sets)
        assert bound["where"] == leaf1
    assert _bind("expand", {"from": "n:1", "where": "$1"}, sets)["where"] == leaf1


def test_the_row_check_reads_the_held_set_and_refuses_one_too_large():
    sets = _sets("1")
    leaf = {"op": "in", "field": "node_id", "values": [sets.set_marker("1")]}
    with run_steps.registry_scope(sets):
        sets.sets["1"].ids = frozenset({"a"})
        assert run_steps.marker_matches({"node_id": "a"}, leaf, lambda r, f: None) is True
        assert run_steps.marker_matches({"node_id": "b"}, leaf, lambda r, f: None) is False
        sets.sets["1"].ids = None
        # Read only when a row check needs it, on the call's connection.
        with pytest.raises(run_steps.RunRefusal, match="no read connection"):
            run_steps.marker_matches({"node_id": "a"}, leaf, lambda r, f: None)


class _Cursor:
    def __init__(self, rows):
        self.rows, self.sql = rows, None

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return None

    def execute(self, sql):
        self.sql = sql

    def fetchall(self):
        return [(row,) for row in self.rows]


class _Conn:
    def __init__(self, rows):
        self.cursors = []
        self.rows = rows

    def cursor(self):
        cursor = _Cursor(self.rows)
        self.cursors.append(cursor)
        return cursor


def test_the_row_check_reads_a_set_once_and_refuses_one_past_the_limit(monkeypatch):
    sets = _sets("1", fields=("attrs.k",))
    leaf = {"op": "in", "field": "node_id", "values": [sets.set_marker("1")]}
    conn = _Conn(["a", "b"])
    token = run_steps.bind_connection(conn)
    try:
        with run_steps.registry_scope(sets):
            assert run_steps.marker_matches({"node_id": "a"}, leaf, lambda r, f: None)
            assert not run_steps.marker_matches({"node_id": "c"}, leaf, lambda r, f: None)
            assert len(conn.cursors) == 1
            assert isinstance(sets.sets["1"].ids, frozenset)
            monkeypatch.setattr(run_steps, "MATERIALIZE_CAP", 1)
            sets.sets["1"].ids = None
            with pytest.raises(run_steps.RunRefusal, match="more than 1 records"):
                run_steps.marker_matches({"node_id": "a"}, leaf, lambda r, f: None)
    finally:
        run_steps.unbind_connection(token)


def test_values_compare_as_jsonb_does():
    canonical = run_steps.canonical
    assert canonical(1) == canonical(1.0)
    assert canonical(True) != canonical(1)
    assert canonical("1") != canonical(1)
    assert canonical(None) is None
    assert canonical({"a": [1, 2]}) == canonical({"a": [1, 2]})


def test_wrong_uses_of_a_reference_are_refused_with_the_form_that_works():
    sets = _sets("1")
    cases = [
        ({"filter": {"where": "$1.attrs.k"}}, "list of values"),
        ({"filter": {"where": {"attrs.k": "$1"}}}, "is a set"),
        ({"filter": {"where": {"attrs.k": ["$1.attrs.k", "x"]}}}, "stands alone"),
        ({"search": {"query": "x", "where": "$1"}}, "does not take a set"),
        ({"expand": {"from": "$1"}}, EXPAND_FROM_SET),
        ({"inspect": {"target": {"kind": "node", "set": "$1"}}}, "inspect reads a set"),
        ({"filter": {"where": {"op": "gt", "field": "attrs.k", "value": "$1.attrs.k"}}},
         "goes in `in`"),
    ]
    for raw, message in cases:
        (step,) = run_steps.parse_steps([raw])
        with pytest.raises(run_steps.RunRefusal, match=message):
            run_steps.bind_step(step, sets, inspect_ids=lambda n: [])
    with pytest.raises(run_steps.RunRefusal, match="cannot read"):
        run_steps.value_column("title")


EXPAND_FROM_SET = r"expand from a set takes \$1 itself as from, with one link type"


def test_a_whole_step_set_as_expand_start_is_the_link_join():
    """Only a whole "$n" as `from` or `input` is a join; a field's values
    or a set inside an object stay refused, with the form that works."""
    for raw, expected in (
        ({"expand": {"from": "$1", "edge_type": "x"}}, 1),
        ({"expand": {"input": "$2", "edge_type": "x"}}, 2),
        ({"expand": {"input": "$1.node_id", "edge_type": "x"}}, None),
        ({"expand": {"input": {"node_id": "$1"}, "edge_type": "x"}}, None),
        ({"filter": {"where": "$1"}}, None),
    ):
        (step,) = run_steps.parse_steps([raw])
        assert run_steps.join_source(step) == expected, raw


def test_retain_names_a_set_only_where_the_removed_set_names_answered_it():
    from okg.substrate.mcp.run_tool import retain_named_a_set

    assert retain_named_a_set("search", {"method": "attrs", "retain": True})
    assert retain_named_a_set("filter", {"input": {"kind": "nodes"}, "retain": True})
    for name in ("map", "aggregate", "expand"):
        assert retain_named_a_set(name, {"retain": True})
    # Kept on their result reference, as on dev.
    assert not retain_named_a_set("search", {"query": "x", "retain": True})
    assert not retain_named_a_set("filter", {"input": {"ref": "rr1"}, "retain": True})
    assert not retain_named_a_set("filter", {"input": {"kind": "nodes"}})


def test_a_removed_set_name_is_left_unbound_for_its_call_to_refuse():
    sets = _sets("1")
    where = {"op": "and", "clauses": ["$1", "$r4"]}
    (step,) = run_steps.parse_steps([{"filter": {"where": where}}])
    assert run_steps.references("filter", {"where": where}) == {"1": set(), "r4": set()}
    assert run_steps.step_references(step) == {1: set()}
    bound = run_steps.bind_step(step, sets, inspect_ids=lambda n: [])
    assert bound["where"]["clauses"][1] == "$r4"


def _guard(arguments, *, inside: bool, name: str = "filter", served_run: bool = True):
    import anyio

    from okg.substrate.mcp.run_tool import guard_call

    sent: list = []

    async def plain(tool, args):
        sent.append((tool, args))
        return "served"

    class _Server:
        def okg_visible_tools(self):
            return frozenset({"filter", "map", "aggregate", *({"run"} if served_run else ())})

        async def _okg_run_blocking(self, fn, *args):
            return fn(*args)

    class _Backend:
        def refused_reason(self):
            return None

    async def call():
        return await guard_call(_Server(), name, arguments, plain, _Backend())

    if inside:
        with run_steps.registry_scope(run_steps.RunSets()):
            return anyio.run(call), sent
    return anyio.run(call), sent


def test_a_removed_set_name_and_retain_are_refused_by_name_before_any_read():
    """Failure cases: `$r1`, `retain=true` on filter over a node kind, and a
    `$1` outside its run never reach the tool; each says a set lasts one
    run call."""
    sentence = "a set lasts for one run call; put both reads in one run(steps=[...])"
    for arguments, code in (
        ({"where": "$r1"}, "set_names_removed"),
        ({"where": {"op": "and", "clauses": [{"type": "t"}, "$r12"]}}, "set_names_removed"),
        ({"where": {"type": "t"}, "retain": True}, "set_names_removed"),
        ({"where": "$1"}, "invalid_operator_arguments"),
    ):
        result, sent = _guard(arguments, inside=False)
        text = result.content[0].text
        assert result.isError and code in text and sentence in text, text
        assert sent == []
    # Where run is not served the refusal does not send the caller to it.
    result, _sent = _guard({"where": "$r1"}, inside=False, served_run=False)
    assert "run(steps=" not in result.content[0].text
    # A lexical search and a filter over a saved result keep retain.
    for name, arguments in (
        ("search", {"query": "x", "retain": True}),
        ("filter", {"input": {"ref": "rr1"}, "retain": True}),
    ):
        result, sent = _guard(arguments, inside=False, name=name)
        assert result == "served" and sent == [(name, arguments)]


def test_expand_from_one_id_is_sent_as_its_input():
    (step,) = run_steps.parse_steps([{"expand": {"from": "file:a", "edge_type": "x"}}])
    assert run_steps.bind_step(step, _sets(), inspect_ids=lambda n: [])["input"] == "file:a"


def test_a_value_list_in_a_test_written_with_its_operator_as_the_key_is_bound():
    """`{"node_id": {"in": ["$1.node_id"]}}` can mean one thing: it is bound
    as the `in` test and the binding records the note the reply prints."""
    sets = _sets("1", fields=("node_id", "attrs.k"))
    for operand in (["$1.node_id"], "$1.node_id"):
        notes: list = []
        (step,) = run_steps.parse_steps([{"filter": {"where": {
            "type": "ticket", "node_id": {"in": operand}}}}])
        bound = run_steps.bind_step(step, sets, inspect_ids=lambda n: [], notes=notes)
        assert bound["where"] == {"type": "ticket", "node_id": {
            "op": "in", "values": [sets.values_marker("1", "node_id")]}}
        assert notes == [{
            "rule": "operator_as_key",
            "sent": '{"node_id": {"in": ...}}',
            "served": '{"node_id": {"op": "in", "values": ...}}',
        }]
    (step,) = run_steps.parse_steps([{"map": {"where": {"op": "and", "clauses": [
        {"attrs.key": {"eq": "$1.attrs.k"}}]}}}])
    bound = run_steps.bind_step(step, sets, inspect_ids=lambda n: [])
    assert bound["where"]["clauses"][0] == {"attrs.key": {
        "op": "in", "values": [sets.values_marker("1", "attrs.k")]}}
    # Without a reference the object is the grammar's to read, unchanged.
    (plain,) = run_steps.parse_steps([{"filter": {"where": {"attrs.k": {"in": ["a"]}}}}])
    assert run_steps.bind_step(plain, sets, inspect_ids=lambda n: [])["where"] == {
        "attrs.k": {"in": ["a"]}}


def test_a_reference_no_binding_reads_is_refused_by_name_not_sent():
    """Failure cases: a value list under another operator, a set as expand's
    start (in `input` too), and a reference left where no set is read are
    each refused by the binding, never sent on as text."""
    sets = _sets("1", fields=("attrs.k",))
    cases = [
        ({"filter": {"where": {"attrs.k": {"gt": "$1.attrs.k"}}}}, "goes in `in`"),
        ({"expand": {"input": "$1", "edge_type": "x"}}, EXPAND_FROM_SET),
        ({"expand": {"input": "$1.node_id", "edge_type": "x"}}, EXPAND_FROM_SET),
        ({"expand": {"input": {"node_id": "$1.node_id"}, "edge_type": "x"}}, EXPAND_FROM_SET),
        ({"filter": {"where": {"attrs.k": {"in": ["$1.attrs.k"], "gt": 1}}}},
         r"\$1 stands where no set is read"),
        ({"filter": {"input": {"kind": "nodes", "filter": {"attrs.k": {
            "in": ["$1.attrs.k"], "exists": True}}}}}, r"\$1 stands where no set is read"),
    ]
    for raw, message in cases:
        (step,) = run_steps.parse_steps([raw])
        with pytest.raises(run_steps.RunRefusal, match=message):
            run_steps.bind_step(step, sets, inspect_ids=lambda n: [])


def test_inside_a_run_an_unread_step_reference_is_not_told_to_use_run():
    """A step's call that still names `$1` is refused with what is true
    inside a run, never with "send the steps together in run(...)"."""
    arguments = {"where": {"attrs.k": {"in": ["$1.attrs.k"], "gt": 1}}}
    inside, sent = _guard(arguments, inside=True)
    text = inside.content[0].text
    assert inside.isError and sent == []
    assert "$1 stands where no set is read" in text
    assert "run(steps=" not in text
    outside, sent = _guard(arguments, inside=False)
    assert sent == []
    assert "run(steps=" in outside.content[0].text


def test_a_value_list_never_narrows_an_index_candidate_set():
    sets = _sets("1", fields=("attrs.k",))
    clause = {"op": "in", "field": "attrs.key", "values": [sets.values_marker("1", "attrs.k")]}
    assert pushable_clauses(clause, {"key": "text"}) == []
    plain = {"op": "in", "field": "attrs.key", "values": ["T-1"]}
    assert len(pushable_clauses(plain, {"key": "text"})) == 1


def test_nested_json_compares_as_jsonb_does():
    canonical = run_steps.canonical
    assert canonical({"a": [1, {"b": 2}]}) == canonical({"a": [1.0, {"b": 2.0}]})
    assert canonical({"a": True}) != canonical({"a": 1})
    assert canonical([1, 2]) != canonical([2, 1])
    assert canonical({"a": None}) != canonical({})
    assert canonical({"b": 1, "a": 2}) == canonical({"a": 2, "b": 1})


class _BrokenCursor(_Cursor):
    def execute(self, sql, params=None):
        raise RuntimeError("connection lost")


class _Info:
    class transaction_status:  # noqa: N801 - mimics the psycopg enum member
        name = "IDLE"


class _RestorableConn:
    def __init__(self, broken: bool):
        self.broken, self.closed, self.executed = broken, False, []
        self.info = _Info()

    def cursor(self):
        if self.broken:
            return _BrokenCursor([])
        conn = self

        class _Recording(_Cursor):
            def execute(self, sql, params=None):
                conn.executed.append((sql, params))

        return _Recording([])

    def rollback(self):
        pass

    def close(self):
        self.closed = True


def test_a_timeout_is_restored_or_the_connection_is_closed():
    good = _RestorableConn(broken=False)
    assert run_steps.restore_timeout(good, "0") is True
    assert good.executed == [("SELECT set_config('statement_timeout', %s, false)", ("0",))]
    assert not good.closed
    bad = _RestorableConn(broken=True)
    assert run_steps.restore_timeout(bad, "0") is False
    # A closed connection is discarded by the pool, never handed out again.
    assert bad.closed


class QueryCanceled(Exception):
    pass


TIMEOUT = "canceling statement due to statement timeout"
USER_CANCEL = "canceling statement due to user request"


def _spent() -> float:
    import time

    return time.monotonic() - 1


def test_only_a_statement_the_budget_cut_is_noted():
    """A cancel is the budget's when the watch fired or the deadline had
    come; the server's message text never decides it."""
    events = run_steps.BudgetEvents()
    run_steps.note_cancelled(QueryCanceled(TIMEOUT))
    assert not events.cancelled
    early = run_steps.BudgetEvents()
    with run_steps.deadline_scope(10**9, early):
        run_steps.note_cancelled(ValueError("another refusal"))
        # Failure case (review F3): a statement timeout before the deadline
        # is a tool's own lower cap, not the budget's, whatever it says.
        run_steps.note_cancelled(QueryCanceled(TIMEOUT))
        run_steps.note_cancelled(QueryCanceled(USER_CANCEL))
    assert not early.cancelled
    with run_steps.deadline_scope(_spent(), events):
        wrapped = RuntimeError("tool failed")
        wrapped.__cause__ = QueryCanceled("any wording, any language")
        run_steps.note_cancelled(wrapped)
    assert events.cancelled
    watched = run_steps.BudgetEvents()
    watched.fired = True
    with run_steps.deadline_scope(10**9, watched):
        run_steps.note_cancelled(QueryCanceled(USER_CANCEL))
    assert watched.cancelled


def test_a_cancel_the_budget_did_not_send_keeps_its_own_error(monkeypatch):
    """L3: only the budget's own cut is served as run_over_budget; it
    offers the step as its own call when the step names no earlier set."""
    from okg.substrate.mcp import run_tool

    def refusal(code, message, fix_hint="", *, tool="run", data=None):
        return {"code": code, "message": message, "fix_hint": fix_hint, "data": data or {}}

    monkeypatch.setattr(run_tool, "_refusal", refusal)
    (alone, reads_set) = run_steps.parse_steps([
        {"filter": {"where": {"type": "doc"}}},
        {"filter": {"where": "$1"}},
    ])
    early = run_steps.BudgetEvents()
    early.deadline = 10**9
    other_error = run_tool._failure("filter", QueryCanceled(TIMEOUT), early, alone, 7)
    assert other_error["code"] == "operator_execution_failed", other_error
    spent = run_steps.BudgetEvents()
    spent.deadline = _spent()
    error = run_tool._failure("filter", QueryCanceled(USER_CANCEL), spent, alone, 7)
    assert error["code"] == "run_over_budget", error
    assert error["data"]["retry_calls"] == [{
        "tool": "filter", "arguments": {"where": {"type": "doc"}, "generation_id": 7},
    }]
    assert "send the step on its own: filter(where=" in error["fix_hint"], error
    assert "generation_id=7" in error["fix_hint"], error
    later_error = run_tool._failure("filter", QueryCanceled(TIMEOUT), spent, reads_set, 7)
    assert later_error["code"] == "run_over_budget"
    assert "retry_calls" not in later_error.get("data", {})
    assert "reads an earlier step's set" in later_error["fix_hint"]


class _CancelConn:
    def __init__(self):
        self.cancels = 0

    def cancel(self):
        self.cancels += 1


def test_the_deadline_watch_sends_nothing_once_stopped():
    """M1: stop() and a cancel share one lock; after stop() no cancel is
    sent, and stop() says whether one was."""
    import time

    conn = _CancelConn()
    watch = run_steps.DeadlineWatch(conn, time.monotonic() + 30, None)
    assert watch.stop() is False
    watch._fire()  # a timer that was already due when stop() ran
    assert conn.cancels == 0 and watch.fired is False


def test_the_deadline_watch_fires_at_the_deadline_and_again_until_stopped(monkeypatch):
    """M1 and L2: from the deadline on, a cancel is sent every
    WATCH_REARM_S, so a statement started after the deadline is cut too;
    the budget's events say the watch fired."""
    import time

    monkeypatch.setattr(run_steps, "WATCH_REARM_S", 0.02)
    conn = _CancelConn()
    events = run_steps.BudgetEvents()
    watch = run_steps.DeadlineWatch(conn, time.monotonic() + 0.01, events)
    give_up = time.monotonic() + 5
    while conn.cancels < 3 and time.monotonic() < give_up:
        time.sleep(0.01)
    assert watch.stop() is True
    sent = conn.cancels
    assert sent >= 3 and events.fired
    time.sleep(0.1)
    assert conn.cancels == sent


def test_the_set_name_store_is_gone_and_the_cut_line_has_one_form():
    from okg.substrate.mcp import run_tool
    from okg.substrate.mcp.result_text import cut_line

    for gone in ("RetainedSets", "serve_with_sets", "retains_by_name"):
        assert not hasattr(run_tool, gone), gone
    assert cut_line("cut to fit 2,048 bytes", 12, 28, "run(steps=[])") == (
        "! cut to fit 2,048 bytes: 12 rows shown, 28 left; next: run(steps=[])"
    )
    assert cut_line("cut at limit 5", 5, None, "x()").endswith("5 rows shown, count unknown; next: x()")
