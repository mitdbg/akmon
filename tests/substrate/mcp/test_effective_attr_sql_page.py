"""Regression proof for the broad Python effective-attrs fallback.

The live failure was not response size: ``filter`` read a broad effective-attrs
population, decoded full document payloads, and accumulated them in Python before
applying the predicate and page. These tests pin the replacement contract at the
production tool seam: exact SQL predicate + deterministic order + LIMIT/OFFSET,
with Nomos handed only the bounded page.
"""

from __future__ import annotations

import json
import uuid

import psycopg
import pytest

from okg.substrate.attributes import (
    materialize_effective_attributes_for_generation,
    record_attribute_observation,
)
from okg.substrate.errors import SubstrateError
from okg.substrate.graph_store import ChronosGraphStore
from okg.substrate.library.sources.base import NodeFact
from okg.substrate.mcp import server as server_mod
from okg.substrate.publisher import Publisher
from tests.pytest.catalog_fixture_builders import SubtypeSpec, seed_catalog
from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer  # noqa: E402


pytestmark = pytest.mark.postgres


def _publish_fixture(dsn: str) -> str:
    suffix = uuid.uuid4().hex[:8]
    rows = [
        ("alpha.py", "alice", 1, True, "2026-08-09T00:00:00Z"),
        ("beta.py", None, 2, False, "2026-08-10T00:00:00Z"),
        ("nested/gamma.py", "carol", 3, True, "2026-08-11T00:00:00Z"),
        ("notes.txt", "dana", 4, False, "not-a-timestamp"),
        ("nested/omega.py", "erin", 5, True, "2026-08-13T00:00:00Z"),
        (None, "frank", 0, False, "2026-08-14T00:00:00Z"),
    ]
    with psycopg.connect(dsn, autocommit=True) as conn:
        seed_catalog(
            conn,
            subtypes=[
                SubtypeSpec(
                    name="doc",
                    metadata={
                        "filterable_attrs": {
                            "path": "text",
                            "rank": "num",
                            "active": "bool",
                            "seen_at": "ts",
                        }
                    },
                )
            ],
        )
        store = ChronosGraphStore.from_connection(conn)
        store.apply_facts(
            [
                NodeFact(
                    f"doc:{index}:{suffix}",
                    "doc",
                    {
                        **({"path": path} if path is not None else {}),
                        "owner": owner,
                        "rank": rank,
                        "active": active,
                        "seen_at": seen_at,
                        # Large enough that collecting a wide population would be
                        # visible in memory, but cheap for a six-row regression.
                        "body": "x" * 16_384,
                    },
                    {"id": f"doc:{index}:{suffix}"},
                    {"v": 1},
                )
                for index, (path, owner, rank, active, seen_at) in enumerate(rows)
            ],
            source="default",
            strict_endpoints=True,
        )
    published = Publisher(dsn, "default", catalog_identity=bind_test_catalog_writer(dsn, "default")).cycle()
    assert published["status"] == "published", published
    return str(published["generation_id"])


def _items(result: dict) -> list[dict]:
    return list(result.get("items") or [])


def _unfence(value: str) -> str:
    lines = value.splitlines()
    if len(lines) >= 3 and lines[0].startswith("⟦UNTRUSTED_DATA source="):
        return "\n".join(lines[1:-1])
    return value


@pytest.mark.parametrize(
    ("predicate", "expected_paths"),
    [
        (
            {"op": "suffix", "field": "attrs.path", "value": ".py"},
            ["alpha.py", "beta.py", "nested/gamma.py", "nested/omega.py"],
        ),
        (
            {"op": "exists", "field": "attrs.path"},
            ["alpha.py", "beta.py", "nested/gamma.py", "notes.txt", "nested/omega.py"],
        ),
        (
            {"op": "contains", "field": "attrs", "value": {"active": True}},
            ["alpha.py", "nested/gamma.py", "nested/omega.py"],
        ),
        (
            {
                "op": "and",
                "clauses": [
                    {"op": "gt", "field": "attrs.rank", "value": 1},
                    {
                        "op": "not",
                        "clause": {
                            "op": "eq",
                            "field": "attrs.path",
                            "value": "notes.txt",
                        },
                    },
                ],
            },
            ["beta.py", "nested/gamma.py", "nested/omega.py"],
        ),
        (
            {
                "op": "or",
                "clauses": [
                    {"op": "eq", "field": "attrs.rank", "value": 1},
                    {"op": "in", "field": "attrs.rank", "values": [4, 5]},
                ],
            },
            ["alpha.py", "notes.txt", "nested/omega.py"],
        ),
        (
            {
                "op": "eq",
                "field": "attrs.seen_at",
                "value": "2026-08-13T00:00:00+00:00",
            },
            ["nested/omega.py"],
        ),
    ],
)
def test_supported_attr_predicates_execute_with_sql_parity(
    clean_runtime: str,
    predicate: dict,
    expected_paths: list[str],
) -> None:
    generation = _publish_fixture(clean_runtime)
    mcp = server_mod.build_server(dsn=clean_runtime)
    try:
        result = mcp._tool_manager._tools["filter"].fn(
            input={"kind": "nodes", "subtype": "doc"},
            predicate=predicate,
            generation_id=generation,
            limit=50,
            select=["node_id", "attrs.path"],
        )
    finally:
        mcp._okg_pool.close()
        if mcp._okg_write_pool:
            mcp._okg_write_pool.close()
    assert [_unfence(row["attrs_path"]) for row in _items(result)] == expected_paths
    assert result["truncated"] is False


def test_filter_pages_in_sql_before_python_policy_work(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    generation = _publish_fixture(clean_runtime)
    statements: list[tuple[str, object]] = []
    policy_batch_sizes: list[int] = []
    real_execute = psycopg.Cursor.execute
    real_filter = server_mod.filter_node_rows_for_session

    def capture_execute(self, query, params=None, **kwargs):
        text = str(query)
        if "attribute_effective_view" in text and "page_limit" in text:
            statements.append((text, params))
        return real_execute(self, query, params, **kwargs)

    def capture_filter(conn, session, rows, **kwargs):
        materialized = list(rows)
        policy_batch_sizes.append(len(materialized))
        return real_filter(conn, session, materialized, **kwargs)

    monkeypatch.setattr(psycopg.Cursor, "execute", capture_execute)
    monkeypatch.setattr(server_mod, "filter_node_rows_for_session", capture_filter)
    mcp = server_mod.build_server(dsn=clean_runtime)
    try:
        result = mcp._tool_manager._tools["filter"].fn(
            input={"kind": "nodes", "subtype": "doc"},
            predicate={"op": "suffix", "field": "attrs.path", "value": ".py"},
            generation_id=generation,
            limit=2,
            offset=1,
            select=["node_id", "attrs.path"],
        )
    finally:
        mcp._okg_pool.close()
        if mcp._okg_write_pool:
            mcp._okg_write_pool.close()

    assert len(statements) == 1, statements
    sql, params = statements[0]
    assert "ORDER BY effective_page.target_id ASC" in sql
    assert "LIMIT %(page_limit)s OFFSET %(page_offset)s" in sql
    assert params["page_limit"] == 3
    assert params["page_offset"] == 1
    # The lookahead determines truncation in SQL and never reaches Nomos.
    assert policy_batch_sizes == [2]
    assert result["truncated"] is True
    assert result["next_offset"] == 3
    assert len(_items(result)) == 2


def test_filter_attribute_order_uses_unambiguous_production_sql(
    clean_runtime: str,
) -> None:
    generation = _publish_fixture(clean_runtime)
    mcp = server_mod.build_server(dsn=clean_runtime)
    try:
        result = mcp._tool_manager._tools["filter"].fn(
            input={"kind": "nodes", "subtype": "doc"},
            predicate={"op": "exists", "field": "attrs.path"},
            generation_id=generation,
            limit=3,
            offset=1,
            select=["node_id", "attrs.path"],
            order_by="attrs.path",
        )
    finally:
        mcp._okg_pool.close()
        if mcp._okg_write_pool:
            mcp._okg_write_pool.close()

    assert [_unfence(row["attrs_path"]) for row in _items(result)] == [
        "beta.py",
        "nested/gamma.py",
        "nested/omega.py",
    ]
    assert result["truncated"] is True


def test_filter_attribute_order_keeps_missing_values_last(clean_runtime: str) -> None:
    generation = _publish_fixture(clean_runtime)
    mcp = server_mod.build_server(dsn=clean_runtime)
    try:
        result = mcp._tool_manager._tools["filter"].fn(
            input={"kind": "nodes", "subtype": "doc"},
            predicate={"op": "exists", "field": "attrs.rank"},
            generation_id=generation,
            limit=10,
            select=["node_id", "attrs.path"],
            order_by="attrs.path",
        )
    finally:
        mcp._okg_pool.close()
        if mcp._okg_write_pool:
            mcp._okg_write_pool.close()

    assert [_unfence(row["attrs_path"]) for row in _items(result)[:-1]] == [
        "alpha.py",
        "beta.py",
        "nested/gamma.py",
        "nested/omega.py",
        "notes.txt",
    ]
    assert _items(result)[-1]["attrs_path"] is None


def _through_client(dsn: str, calls: list[dict], *, tool: str = "filter") -> list[dict]:
    """Each `tool` call (filter by default) through one MCP client session:
    the JSON replies a client receives, after the response bounds."""
    import json

    import anyio
    from mcp.shared.memory import create_connected_server_and_client_session

    mcp = server_mod.build_server(dsn=dsn)
    out: list[dict] = []

    async def drive() -> None:
        async with create_connected_server_and_client_session(mcp) as client:
            for arguments in calls:
                result = await client.call_tool(
                    tool, arguments, meta={"okg/result_format": "json"},
                )
                out.append(json.loads(result.content[0].text))

    try:
        anyio.run(drive)
    finally:
        mcp._okg_pool.close()
        if mcp._okg_write_pool:
            mcp._okg_write_pool.close()
    return out


def _filter_pages(dsn: str, generation: str, order_by: str, *, limit: int,
                  pages: int, select: tuple[str, ...] = ("node_id", "attrs.path")) -> list[dict]:
    """`pages` consecutive pages of the doc set ordered by `order_by`,
    through the client."""
    return _through_client(dsn, [
        {"input": {"kind": "nodes", "subtype": "doc"}, "predicate": {},
         "generation_id": generation, "limit": limit, "offset": page * limit,
         "select": list(select), "order_by": order_by}
        for page in range(pages)
    ])


def _publish_odd_values(dsn: str) -> str:
    """Six records whose undeclared `k` is a number, the same digits as a
    string, other strings, JSON null, or absent."""
    values: list[tuple[str, object]] = [
        ("a-num10", 10), ("b-str10", "10"), ("c-str9", "9"), ("d-letter", "a"),
        ("e-absent", None), ("f-null", None),
    ]
    with psycopg.connect(dsn, autocommit=True) as conn:
        seed_catalog(conn, subtypes=[SubtypeSpec(name="doc", metadata={})])
        store = ChronosGraphStore.from_connection(conn)
        store.apply_facts(
            [
                NodeFact(
                    f"doc:{name}", "doc",
                    {"label": name, **({} if name == "e-absent" else {"k": value})},
                    {"id": f"doc:{name}"}, {"v": 1},
                )
                for name, value in values
            ],
            source="default",
            strict_endpoints=True,
        )
    published = Publisher(dsn, "default", catalog_identity=bind_test_catalog_writer(dsn, "default")).cycle()
    assert published["status"] == "published", published
    return str(published["generation_id"])


def test_text_order_puts_a_number_beside_its_digits_and_absent_beside_null(
    clean_runtime: str,
) -> None:
    """#3666 (2c), through the client: the number 10 and the string "10"
    read the same text, so they tie and the id breaks it; an absent key and
    a JSON null are both no value, after every value, again by id."""
    generation = _publish_odd_values(clean_runtime)
    up, down = _through_client(clean_runtime, [
        {"input": {"kind": "nodes", "subtype": "doc"}, "predicate": {},
         "generation_id": generation, "limit": 10, "select": ["node_id"],
         "order_by": order}
        for order in ("attrs.k", "attrs.k desc")
    ])
    assert [row["node_id"] for row in _items(up)] == [
        "doc:a-num10", "doc:b-str10", "doc:c-str9", "doc:d-letter",
        "doc:e-absent", "doc:f-null",
    ]
    # Descending turns the values round; a tie and the no-value rows keep
    # the id order.
    assert [row["node_id"] for row in _items(down)] == [
        "doc:d-letter", "doc:c-str9", "doc:a-num10", "doc:b-str10",
        "doc:e-absent", "doc:f-null",
    ]
    assert up["sorted_as_text"] == "attrs.k"


def test_filter_undeclared_attribute_orders_as_text_with_a_note(
    clean_runtime: str,
) -> None:
    """Issue #3666 (2c, design 2.5): `owner` has no declared type. It sorts
    by its text, values first and a null last, and the reply says so."""
    generation = _publish_fixture(clean_runtime)
    (up,) = _filter_pages(clean_runtime, generation, "attrs.owner", limit=10, pages=1)
    (down,) = _filter_pages(clean_runtime, generation, "attrs.owner desc", limit=10, pages=1)
    paths = [_unfence(row["attrs_path"]) if row["attrs_path"] else None for row in _items(up)]
    # alice, (null owner: beta.py) last; carol, dana, erin, frank between.
    assert paths == ["alpha.py", "nested/gamma.py", "notes.txt", "nested/omega.py", None,
                     "beta.py"]
    assert up["sorted_as_text"] == "attrs.owner"
    down_paths = [_unfence(row["attrs_path"]) if row["attrs_path"] else None
                  for row in _items(down)]
    assert down_paths == [None, "nested/omega.py", "notes.txt", "nested/gamma.py",
                          "alpha.py", "beta.py"]
    # A declared key keeps its typed order and says nothing.
    (typed,) = _filter_pages(clean_runtime, generation, "attrs.rank", limit=10, pages=1)
    assert "sorted_as_text" not in typed
    # Any undeclared key, here the long `body`, takes the same text order.
    (body,) = _filter_pages(clean_runtime, generation, "attrs.body", limit=10, pages=1)
    assert body["sorted_as_text"] == "attrs.body"


def test_an_order_as_text_is_total_and_stable_across_pages(clean_runtime: str) -> None:
    """Every row's `body` is the same text: the tie is broken by the record
    id, so pages of two read one after another give the whole order once."""
    generation = _publish_fixture(clean_runtime)
    (whole,) = _filter_pages(clean_runtime, generation, "attrs.body", limit=10, pages=1)
    ids = [row["node_id"] for row in _items(whole)]
    assert ids == sorted(ids) and len(ids) == 6
    paged = _filter_pages(clean_runtime, generation, "attrs.body", limit=2, pages=3)
    assert [row["node_id"] for page in paged for row in _items(page)] == ids
    again = _filter_pages(clean_runtime, generation, "attrs.body", limit=2, pages=3)
    assert [_items(page) for page in again] == [_items(page) for page in paged]


def test_declared_timestamp_eq_keeps_unparseable_text_parity(
    clean_runtime: str,
) -> None:
    generation = _publish_fixture(clean_runtime)
    mcp = server_mod.build_server(dsn=clean_runtime)
    try:
        result = mcp._tool_manager._tools["filter"].fn(
            input={"kind": "nodes", "subtype": "doc"},
            predicate={
                "op": "eq",
                "field": "attrs.seen_at",
                "value": "not-a-timestamp",
            },
            generation_id=generation,
            limit=10,
            select=["attrs.path"],
        )
    finally:
        mcp._okg_pool.close()
        if mcp._okg_write_pool:
            mcp._okg_write_pool.close()

    assert [_unfence(row["attrs_path"]) for row in _items(result)] == ["notes.txt"]


def test_declared_attr_overlay_bypasses_a_stale_declared_index_match(
    clean_runtime: str,
) -> None:
    """A post-publish overlay must not be lost to a declared-key candidate set.

    The effective snapshot is refreshed immediately, while the interval front
    and the graph rows a declared expression index reads may still carry the
    pre-overlay value. The SQL page must admit the overlay-touched target via
    the exact effective predicate. (Before okg#3112 step 4 the stale
    candidate set was okg.attr_index.)
    """
    generation = _publish_fixture(clean_runtime)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT v.target_id
                  FROM okg.attribute_effective_view AS view
                  JOIN okg.attribute_versions AS v
                    USING (target_kind, target_id, attrs_hash)
                 WHERE view.generation_id = %s
                   AND view.target_kind = 'node'
                   AND view.subtype = 'doc'
                   AND v.attrs @> %s::jsonb
                 LIMIT 1
                """,
                (generation, '{"path":"alpha.py"}'),
            )
            target_id = cur.fetchone()[0]
        record_attribute_observation(
            conn,
            target_kind="node",
            target_id=target_id,
            generation_id=generation,
            attr_key="path",
            value="overlaid.py",
            producer="test:effective-attr-sql-page",
        )
        materialize_effective_attributes_for_generation(
            conn, generation_id=generation,
        )

    mcp = server_mod.build_server(dsn=clean_runtime)
    try:
        result = mcp._tool_manager._tools["filter"].fn(
            input={"kind": "nodes", "subtype": "doc"},
            predicate={
                "op": "eq", "field": "attrs.path", "value": "overlaid.py",
            },
            generation_id=generation,
            limit=10,
            select=["node_id", "attrs.path"],
        )
    finally:
        mcp._okg_pool.close()
        if mcp._okg_write_pool:
            mcp._okg_write_pool.close()
    assert [row["node_id"] for row in _items(result)] == [target_id]
    assert [_unfence(row["attrs_path"]) for row in _items(result)] == [
        "overlaid.py"
    ]


def test_map_pages_effective_attrs_in_sql(clean_runtime: str) -> None:
    generation = _publish_fixture(clean_runtime)
    mcp = server_mod.build_server(dsn=clean_runtime)
    try:
        result = mcp._tool_manager._tools["map"].fn(
            input={"kind": "nodes", "subtype": "doc"},
            projection=["node_id", "attrs.path"],
            generation_id=generation,
            limit=2,
            offset=2,
        )
    finally:
        mcp._okg_pool.close()
        if mcp._okg_write_pool:
            mcp._okg_write_pool.close()
    assert len(_items(result)) == 2
    assert result["offset"] == 2
    assert result["next_offset"] == 4


def test_map_attr_input_filter_uses_sql_even_for_stable_projection(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    generation = _publish_fixture(clean_runtime)
    statements: list[str] = []
    real_execute = psycopg.Cursor.execute

    def capture_execute(self, query, params=None, **kwargs):
        text = str(query)
        if "attribute_effective_view" in text and "page_limit" in text:
            statements.append(text)
        return real_execute(self, query, params, **kwargs)

    monkeypatch.setattr(psycopg.Cursor, "execute", capture_execute)
    mcp = server_mod.build_server(dsn=clean_runtime)
    try:
        result = mcp._tool_manager._tools["map"].fn(
            input={
                "kind": "nodes",
                "subtype": "doc",
                "filter": {
                    "op": "suffix",
                    "field": "attrs.path",
                    "value": ".py",
                },
            },
            projection=["node_id", "subtype"],
            generation_id=generation,
            limit=2,
        )
    finally:
        mcp._okg_pool.close()
        if mcp._okg_write_pool:
            mcp._okg_write_pool.close()

    assert len(statements) == 1
    assert len(_items(result)) == 2
    assert result["truncated"] is True


def test_attribute_aggregate_refuses_python_fallback(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    generation = _publish_fixture(clean_runtime)
    mcp = server_mod.build_server(dsn=clean_runtime, deployment_name="default")
    broad_called = False

    def broad_scan(*args, **kwargs):
        nonlocal broad_called
        broad_called = True
        raise AssertionError("broad Python attr scan was invoked")

    monkeypatch.setattr(mcp, "_okg_operator_attr_rows", broad_scan)
    try:
        # Force the honest refusal arm without changing database policy state.
        monkeypatch.setattr(
            server_mod, "deployment_requires_nomos_runtime", lambda _d: True
        )
        with pytest.raises(SubstrateError) as exc_info:
            mcp._tool_manager._tools["aggregate"].fn(
                input={"kind": "nodes", "subtype": "doc"},
                by=["attrs.active"],
                metrics=["count"],
                generation_id=generation,
            )
    finally:
        mcp._okg_pool.close()
        if mcp._okg_write_pool:
            mcp._okg_write_pool.close()
    assert exc_info.value.code == "unsupported_operator_form"
    assert broad_called is False


#: Third re-review (#3666, 2c): a range on a declared attribute whose bound
#: is not of the declared type, or a ts bound the read check cannot read as
#: an instant, is refused naming the attribute's type, never served as an
#: empty page. Each row: the condition, and the paths served (None: refused,
#: with the declared type the refusal names).
_DECLARED_CASES = [
    ({"op": "gt", "field": "attrs.rank", "value": 1},
     ["beta.py", "nested/gamma.py", "notes.txt", "nested/omega.py"]),
    ({"op": "gt", "field": "attrs.rank", "value": "1"}, "num"),
    # These two no attribute of any type can meet: the grammar refuses them
    # first, by the attribute's name.
    ({"op": "between", "field": "attrs.rank", "values": [1, "z"]}, "`attrs.rank`"),
    ({"op": "lt", "field": "attrs.rank", "value": True}, "`attrs.rank`"),
    ({"op": "gt", "field": "attrs.seen_at", "value": "2026-08-12"}, ["nested/omega.py", None]),
    ({"op": "lt", "field": "attrs.seen_at", "value": "soon"}, "ts"),
    ({"op": "lt", "field": "attrs.seen_at", "value": "01/02/2024"}, "ts"),
    ({"op": "between", "field": "attrs.seen_at", "values": ["2026-08-01", "later"]}, "ts"),
    ({"op": "gt", "field": "attrs.seen_at", "value": 5}, "ts"),
    ({"op": "gt", "field": "attrs.path", "value": 5}, "text"),
    ({"op": "and", "clauses": [{"op": "gt", "field": "attrs.rank", "value": 1},
                               {"op": "prefix", "field": "attrs.path", "value": "nested/"}]},
     ["nested/gamma.py", "nested/omega.py"]),
    ({"op": "or", "clauses": [{"op": "lt", "field": "attrs.seen_at", "value": "soon"},
                              {"op": "eq", "field": "attrs.rank", "value": 1}]}, "ts"),
    ({"op": "not", "clause": {"op": "gt", "field": "attrs.rank", "value": "1"}}, "num"),
]


def test_a_range_bound_of_the_wrong_type_for_a_declared_attribute_is_refused(
    clean_runtime: str,
) -> None:
    generation = _publish_fixture(clean_runtime)
    calls = [{"input": {"kind": "nodes", "subtype": "doc"}, "predicate": predicate,
              "generation_id": generation, "select": ["node_id", "attrs.path"], "limit": 50}
             for predicate, _ in _DECLARED_CASES]
    wrong = []
    for (predicate, expected), reply in zip(_DECLARED_CASES, _through_client(clean_runtime, calls)):
        if isinstance(expected, str):
            error = reply.get("error") or {}
            words = expected if expected.startswith("`") else f"declared {expected}"
            if (reply.get("status") != "refused" or "items" in reply
                    or words not in error.get("message", "")):
                wrong.append((predicate, reply))
            continue
        paths = sorted(
            (_unfence(row["attrs_path"]) if row.get("attrs_path") else None
             for row in reply.get("items") or []), key=lambda p: (p is None, p or ""))
        if reply.get("status") == "refused" or paths != sorted(
                expected, key=lambda p: (p is None, p or "")):
            wrong.append((predicate, reply))
    assert not wrong, wrong


def test_a_declared_type_guess_is_offered_and_an_equality_of_another_type_is_noted(
    clean_runtime: str,
) -> None:
    """Fourth review (#3666, 2c). A range bound with one reading in the
    attribute's declared type (digits on `num`, a whole number on `text`) is
    refused with that call offered as a guess, and the offered call is
    served; a bound with no such reading (a number on `ts`, a bool range)
    gets words only. `eq` or `in` with an operand of another JSON type is
    served as written (a stored value of that type is possible) with a
    `compared_as_stored` note; the same test in the declared type has none."""
    generation = _publish_fixture(clean_runtime)
    scope = {"kind": "nodes", "subtype": "doc"}

    def call(predicate: dict) -> dict:
        return {"input": scope, "predicate": predicate, "generation_id": generation,
                "select": ["node_id", "attrs.path"], "limit": 50}

    sent = [
        {"op": "gt", "field": "attrs.rank", "value": "3"},
        {"op": "gt", "field": "attrs.path", "value": 5},
        {"op": "gt", "field": "attrs.seen_at", "value": 5},
        {"op": "gt", "field": "attrs.active", "value": 1},
        {"op": "eq", "field": "attrs.rank", "value": "1"},
        {"op": "in", "field": "attrs.path", "values": [5, "alpha.py"]},
        {"op": "eq", "field": "attrs.rank", "value": 1},
        {"op": "eq", "field": "attrs.rank", "value": None},
    ]
    sent.append({"op": "contains", "field": "attrs", "value": {"rank": "2", "owner": 5}})
    calls = [call(p) for p in sent]
    calls[0]["max_bytes"] = 60_000
    replies = _through_client(clean_runtime, calls)
    (digits, path_int, ts_num, bool_range, eq_text, in_mixed, eq_num, eq_null,
     contains_text) = replies

    retry = digits["error"]["retry_call"]
    assert digits["status"] == "refused" and "declared num" in digits["error"]["message"]
    assert retry["tool"] == "filter" and retry["mode"] == "guess"
    assert retry["arguments"]["predicate"] == {"op": "gt", "field": "attrs.rank", "value": 3}
    # Every other argument as sent: the pinned generation and the byte budget.
    assert {k: v for k, v in retry["arguments"].items() if k != "predicate"} == {
        k: v for k, v in calls[0].items() if k != "predicate"}, retry
    assert "repeat" not in retry
    assert path_int["status"] == "refused" and "declared text" in path_int["error"]["message"]
    assert path_int["error"]["retry_call"]["arguments"]["predicate"]["value"] == "5"
    assert '"10" sorts before "5"' in path_int["error"]["fix_hint"], path_int["error"]
    # A call with authority: it is named in `repeat`, never echoed.
    # (No identity claim: this deployment has no identity provider.)
    authority = {"identity_claim": None, "access_context": None}
    (claimed,) = _through_client(clean_runtime, [{**calls[0], "authority": authority}])
    offered = claimed["error"]["retry_call"]
    assert "authority" not in offered["arguments"] and offered["repeat"] == ["authority"]
    assert offered["arguments"] == retry["arguments"], offered
    assert "identity_claim" not in json.dumps(offered), offered
    for reply, words in ((ts_num, "declared ts"), (bool_range, "takes no range")):
        assert reply["status"] == "refused" and words in reply["error"]["message"], reply
        assert "retry_call" not in reply["error"] and "items" not in reply, reply
    (served,) = _through_client(clean_runtime, [retry["arguments"]])
    assert served.get("status") != "refused", served
    assert sorted(_unfence(row["attrs_path"]) for row in served["items"]) == [
        "nested/omega.py", "notes.txt"]

    assert eq_text.get("status") != "refused" and eq_text["items"] == []
    tail = "matches only a stored"
    assert eq_text["compared_as_stored"] == [
        f'attrs.rank is declared num; eq with "1" {tail} text value; for the number write 1']
    assert [_unfence(row["attrs_path"]) for row in in_mixed["items"]] == ["alpha.py"]
    assert in_mixed["compared_as_stored"] == [
        f'attrs.path is declared text; in with 5 {tail} number value; for the text write "5"']
    # contains on attrs is one equality per key; `owner` is not declared.
    assert contains_text.get("status") != "refused" and contains_text["items"] == []
    assert contains_text["compared_as_stored"] == [
        f'attrs.rank is declared num; contains with "2" {tail} text value; '
        + "for the number write 2"]
    for reply in (eq_num, eq_null):
        assert reply.get("status") != "refused" and "compared_as_stored" not in reply, reply
    assert len(eq_num["items"]) == 1


def test_expand_notes_an_equality_of_another_type_on_both_row_shapes(
    clean_runtime: str,
) -> None:
    """#3666 (2c), after #3714: an expand condition `eq` with a text operand
    on a `num` attribute is served as written and says so with
    `compared_as_stored`, whether the rows are the linked records (the
    default select) or the links (a link-field select). The same test with
    a number operand has no note and matches one record."""
    from okg.substrate.library.sources.base import EdgeFact

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog(
            conn,
            subtypes=[
                SubtypeSpec(name="doc", metadata={"filterable_attrs": {"rank": "num"}}),
                "folder",
            ],
            narrowings=[("folder", "contains", "doc")],
        )
        store = ChronosGraphStore.from_connection(conn)
        docs = [
            NodeFact(f"doc:{rank}", "doc", {"rank": rank}, {"id": f"doc:{rank}"}, {"v": 1})
            for rank in (1, 2, 3)
        ]
        links = [
            EdgeFact(
                src="folder:root", dst=doc.node_id, edge_type="contains",
                source_record_id={"id": f"folder:root|contains|{doc.node_id}"},
                source_revision={"v": 1},
            )
            for doc in docs
        ]
        store.apply_facts(
            [NodeFact("folder:root", "folder", {}, {"id": "folder:root"}, {"v": 1}),
             *docs, *links],
            source="default",
            strict_endpoints=True,
        )
    published = Publisher(
        clean_runtime, "default",
        catalog_identity=bind_test_catalog_writer(clean_runtime, "default"),
    ).cycle()
    assert published["status"] == "published", published

    base = {"input": {"node_id": "folder:root"}, "edge_type": "contains",
            "direction": "out", "generation_id": str(published["generation_id"])}
    link_select = {"select": ["edge_id", "src", "dst", "edge_type"]}
    as_text, links_as_text, as_number = _through_client(clean_runtime, [
        {**base, "filter": {"attrs.rank": "1"}},
        {**base, "filter": {"attrs.rank": "1"}, **link_select},
        {**base, "filter": {"attrs.rank": 1}},
    ], tool="expand")
    note = ('attrs.rank is declared num; eq with "1" matches only a stored text '
            "value; for the number write 1")
    for reply in (as_text, links_as_text):
        assert reply.get("status") != "refused" and reply["neighbors"] == [], reply
        assert reply["compared_as_stored"] == [note], reply
    # Failure case: the operand in the declared type gets no note.
    assert as_number.get("status") != "refused", as_number
    assert "compared_as_stored" not in as_number, as_number
    assert [row["node_id"] for row in as_number["neighbors"]] == ["doc:1"]
