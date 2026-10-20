"""The corpus and the filter runner of the okg#3112 step 2, slice 2 reader
tests (PACT lane-h-3112-step2-reader). Not a test module.

It depends only on code that exists on ``dev`` before this slice, so the
same corpus can be loaded and read by dev's reader to record the answers
``data/attr_filter_dev_answers.json`` holds
(``pact/changes/lane-h-3112-step2-reader/scripts/dev_answers_test.py``).

The corpus is the one slice 1's probe test builds (design section 7, gate
2): one node per value of the E13 numbers, the text corpus (1-512
characters, longer values sharing a 600-character prefix) and the E14 ts
strings, with a boolean, written in two revisions so a pinned generation
exists. Revision 2 rewrites every third node with the value three places on,
so every corpus value is held by some node at the head. Every 50th node carries mistyped values. The E13 numbers are
written into the graph rows as exact JSON number text before publish
(Python cannot carry 1e400 through a dict).
"""

from __future__ import annotations

import math
from typing import Any, Iterable

import psycopg

from okg.substrate.graph_store import ChronosGraphStore
from okg.substrate.library.sources.base import NodeFact
from okg.substrate.mcp.response_bounds import MAX_RESPONSE_BYTES
from okg.substrate.publisher import Publisher
from tests.pytest.catalog_fixture_builders import (
    SubtypeSpec,
    bind_test_catalog_writer,
    seed_catalog,
)
from tests.substrate.db import declared_attr_index_corpora as corpora

DEPLOYMENT = "default"
SOURCE = "attr-filter-reader"
DECL = {"t_text": "text", "t_num": "num", "t_bool": "bool", "t_ts": "ts"}
NUMS, NUM_FILTERS = corpora.e13()
TEXTS = corpora.text_corpus()
TS = corpora.E14_TS
#: Stored ts values: E14 without the clock words and the unknown zone, which
#: the step-2 attr_index derivation (removed by okg#3112 step 4b) could not
#: carry through two publishes (slice 1's probe test explains why). They stay
#: bounds.
_CLOCK_WORDS = ("now", "today", "tomorrow", "yesterday", "allballs", "foo/bar")
TS_STORED = [t for t in TS if not any(w in t.lower() for w in _CLOCK_WORDS)]
N_NODES = max(len(NUMS), len(TEXTS), len(TS)) + 20
SCOPE = {"kind": "nodes"}


def nid(i: int) -> str:
    return f"rdr:thing:{i:04d}"


def node_attrs(i: int, rev: int) -> dict[str, Any]:
    shift = 0 if rev == 1 else 3
    attrs: dict[str, Any] = {
        "label": f"n{i} r{rev}",
        "t_num": 0,  # replaced in place by the exact JSON number text
        "t_text": TEXTS[(i + shift) % len(TEXTS)],
        "t_ts": TS_STORED[(i + shift) % len(TS_STORED)],
        "t_bool": bool((i + rev) % 2),
    }
    if i % 50 == 49:
        attrs.update({"t_num": "42", "t_text": 42, "t_ts": 5, "t_bool": "true"})
    return attrs


def num_text(i: int, rev: int) -> str | None:
    """The JSON number text node ``i`` holds at ``rev`` (None: mistyped)."""
    if i % 50 == 49:
        return None
    shift = 0 if rev == 1 else 3
    return NUMS[(i + shift) % len(NUMS)]


def seed(dsn: str) -> Any:
    with psycopg.connect(dsn, autocommit=True) as conn:
        seed_catalog(
            conn,
            subtypes=[SubtypeSpec(name="thing", metadata={"filterable_attrs": dict(DECL)})],
            narrowings=[("thing", "rel", "thing")],
        )
    return bind_test_catalog_writer(dsn, DEPLOYMENT)


def _physical_node_table(cur: Any) -> str:
    cur.execute(
        "SELECT physical_table FROM public._chronos_branch_tables "
        "WHERE backend = 'interval' AND table_name = 'okg.graph_nodes'"
    )
    return str(cur.fetchone()[0])


def load(dsn: str, identity: Any, rev: int, ids: Iterable[int]) -> str:
    """Write revision ``rev`` of nodes ``ids`` and publish; the generation."""
    ids = list(ids)
    with psycopg.connect(dsn, autocommit=True) as conn:
        ChronosGraphStore.from_connection(conn).apply_facts(
            [NodeFact(nid(i), "thing", node_attrs(i, rev), {"row": i}, {"rev": rev})
             for i in ids],
            source=SOURCE,
            strict_endpoints=True,
        )
        with conn.cursor() as cur:
            table = _physical_node_table(cur)
            cur.execute(f'SELECT max(live_hi) FROM public."{table}"')
            open_hi = cur.fetchone()[0]
            for i in ids:
                text = num_text(i, rev)
                if text is None:
                    continue
                cur.execute(
                    f'UPDATE public."{table}" SET attrs = jsonb_set(attrs, %s, %s::jsonb) '
                    "WHERE node_id = %s AND live_hi = %s",
                    (["t_num"], text, nid(i), open_hi),
                )
    res = Publisher(dsn, DEPLOYMENT, catalog_identity=identity).cycle()
    assert res.get("status") == "published", res
    return str(res["generation_id"])


def load_corpus(dsn: str, identity: Any) -> tuple[str, str]:
    """(pinned generation, head generation)."""
    pinned = load(dsn, identity, 1, range(N_NODES))
    head = load(dsn, identity, 2, range(0, N_NODES, 3))
    return pinned, head


# --------------------------------------------------------------------------- #
# Session time zones                                                           #
# --------------------------------------------------------------------------- #

#: Non-UTC TimeZone settings for publisher and reader: a DST zone and the
#: largest named offset (+14).
NON_UTC_ZONES = ["America/New_York", "Pacific/Kiritimati"]
#: POSIX zones at the largest offsets PostgreSQL accepts: +167 hours
#: standard, and a DST zone named without its own offset, which reads
#: January and February at +168:59 (why the removed attr_index ts probe
#: widened by 170 hours, not one day). Python cannot load
#: a timestamptz printed with an offset of a day or more, so telemetry writes
#: warn under them; the tests keep their read sets small.
EXTREME_ZONE = "<+167>-167"
EXTREME_DST_ZONE = "XXX-167:59YYY,M10.1.0,M3.5.0"
EXTREME_ZONES = [EXTREME_ZONE, EXTREME_DST_ZONE]


def set_database_timezone(dsn: str, zone: str) -> None:
    """Every new session on the test database, the publisher's and the MCP
    reader's alike, starts in ``zone``."""
    from psycopg import sql

    with psycopg.connect(dsn, autocommit=True) as conn:
        conn.execute(
            sql.SQL("ALTER DATABASE {} SET timezone = {}").format(
                sql.Identifier(conn.info.dbname), sql.Literal(zone)
            )
        )


def session_timezone(conn: Any) -> str:
    return str(conn.execute("SELECT current_setting('TimeZone')").fetchone()[0])


def assert_zone_took_effect(dsn: str, zone: str, reader: "Reader") -> None:
    """The proof the zone reached both sides: a new session (what the
    publisher opens) and the MCP read pool report it, a date-only value cast
    in a new session reads as midnight in ``zone``, and ``okg.attr_ts_v4``
    still reads it as midnight UTC (it sets its own TimeZone). Until okg#3112
    step 4b this read the value attr_index derived in the publisher's
    session; that table is gone."""
    with psycopg.connect(dsn, autocommit=True) as conn:
        assert session_timezone(conn) == zone
    with reader._mcp._okg_pool.connection() as conn:
        assert session_timezone(conn) == zone
    holders = [
        i for i in range(N_NODES)
        if node_attrs(i, 2 if i % 3 == 0 else 1)["t_ts"] == "2024-01-02"
    ]
    assert holders, "no node holds the date-only value at the head"
    with psycopg.connect(dsn, autocommit=True) as conn:
        row = conn.execute(
            "SELECT '2024-01-02'::timestamptz "
            "  = ('2024-01-02 00:00'::timestamp AT TIME ZONE %s), "
            "'2024-01-02'::timestamptz = okg.attr_ts_v4('2024-01-02')",
            (zone,),
        ).fetchone()
    assert row == (True, False), (zone, row)


# --------------------------------------------------------------------------- #
# Filters                                                                      #
# --------------------------------------------------------------------------- #


def _mcp(attr: str, op: str, value: Any) -> dict[str, Any]:
    field = f"attrs.{attr}"
    if op in ("in", "between"):
        return {"op": op, "field": field, "values": list(value)}
    return {"op": op, "field": field, "value": value}


def _key(pred: dict[str, Any]) -> str:
    """A stable text id for a predicate (floats by repr, ints exactly)."""

    def enc(v: Any) -> str:
        if isinstance(v, bool):
            return "b:" + str(v)
        if isinstance(v, float):
            return "f:" + repr(v)
        if isinstance(v, int):
            return "i:" + str(v)
        if isinstance(v, list):
            return "[" + ",".join(enc(x) for x in v) + "]"
        return "s:" + repr(v)

    value = pred.get("values", pred.get("value"))
    return f"{pred['field']}|{pred['op']}|{enc(value)}"


def num_filters() -> list[dict[str, Any]]:
    return [_mcp("t_num", p["op"], p["value"]) for p in NUM_FILTERS]


def ts_filters(accepted: Iterable[str]) -> list[dict[str, Any]]:
    """eq, lt and gt on every E14 string; between and in over pairs of the
    strings v4 accepts (``accepted``, in E14 order)."""
    out: list[dict[str, Any]] = []
    for value in TS:
        out += [_mcp("t_ts", "eq", value), _mcp("t_ts", "lt", value), _mcp("t_ts", "gt", value)]
    accepted = list(accepted)
    for i in range(0, len(accepted) - 1, 2):
        out.append(_mcp("t_ts", "between", sorted(accepted[i:i + 2])))
        out.append(_mcp("t_ts", "in", accepted[i:i + 2]))
    return out


def text_filters() -> list[dict[str, Any]]:
    long_bounds = [t for t in TEXTS if len(t) > 512][:6]
    long_bounds += [long_bounds[0][:600] + "zzzz", long_bounds[0][:600], "a" * 700,
                    ("a" * 512) + "b"]
    out = [_mcp("t_text", "eq", v) for v in TEXTS + long_bounds]
    for value in sorted(set(TEXTS[:25] + long_bounds)):
        out += [_mcp("t_text", "lt", value), _mcp("t_text", "gt", value)]
    ordered = sorted(set(TEXTS + long_bounds))
    for i in range(0, len(ordered) - 1, 3):
        out.append(_mcp("t_text", "between", [ordered[i], ordered[i + 1]]))
        out.append(_mcp("t_text", "in", [ordered[i], ordered[i + 1], "absent"]))
    return out


def bool_filters() -> list[dict[str, Any]]:
    return [_mcp("t_bool", "eq", True), _mcp("t_bool", "eq", False),
            _mcp("t_bool", "in", [True, False])]


def predicate_key(pred: dict[str, Any]) -> str:
    return _key(pred)


# --------------------------------------------------------------------------- #
# The reader                                                                   #
# --------------------------------------------------------------------------- #


class Reader:
    """The production ``filter`` tool of one built MCP server."""

    def __init__(self, dsn: str) -> None:
        from okg.substrate.mcp import server as server_mod

        self._mcp = server_mod.build_server(dsn=dsn)
        self._fn = self._mcp._tool_manager._tools["filter"].fn

    def close(self) -> None:
        self._mcp._okg_pool.close()
        if self._mcp._okg_write_pool:
            self._mcp._okg_write_pool.close()

    def __enter__(self) -> "Reader":
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()

    def filter(self, generation: str, predicate: dict[str, Any], *, limit: int = 1000,
               offset: int = 0, order_by: str | None = None) -> dict[str, Any]:
        from okg.substrate.errors import SubstrateError

        try:
            return self._fn(
                input=dict(SCOPE),
                predicate=predicate,
                generation_id=generation,
                limit=limit,
                offset=offset,
                # ids only, under the largest response budget: the default 64 KiB
                # budget would cut long text values (and a note) off the page.
                select=["node_id"],
                order_by=order_by,
                max_bytes=MAX_RESPONSE_BYTES,
            )
        except SubstrateError as exc:
            # Only the one refusal these suites now expect, as an envelope
            # `answer` compares; any other error is raised as before.
            if not unreadable_ts_bound(predicate):
                raise
            return {"error": {"code": exc.code, "message": str(exc)}}


def unreadable_ts_bound(predicate: Any) -> bool:
    """Whether a range on ``attrs.t_ts`` has a text bound the read check
    cannot read as an instant (01/02/2024, a leap second, "now").

    Issue #3666 (step 2c, orchestrator call 2026-10-13): such a read is
    refused in every mode. Before, the lowerer compiled it to FALSE and every
    mode served the same empty page marked complete; these suites pinned that
    page, a silent empty answer, not a contract.
    """
    from okg.substrate.mcp.operator_sql import as_operator_timestamp

    stack = [predicate]
    while stack:
        node = stack.pop()
        if not isinstance(node, dict):
            continue
        stack += node.get("clauses") or []
        if isinstance(node.get("clause"), dict):
            stack.append(node["clause"])
        if node.get("field") != "attrs.t_ts" or node.get("op") not in ("lt", "gt", "between"):
            continue
        bounds = node.get("values") if node["op"] == "between" else [node.get("value")]
        if any(isinstance(b, str) and as_operator_timestamp(b) is None for b in bounds or []):
            return True
    return False


def answer(result: dict[str, Any]) -> dict[str, Any]:
    """The comparable part of a filter result: ids in order, or the error."""
    if "error" in result:
        err = result["error"]
        text = str(err.get("message") if isinstance(err, dict) else err)
        return {"error": text}
    return {
        "ids": [row["node_id"] for row in result.get("items") or []],
        "truncated": bool(result.get("truncated")),
        "next_offset": result.get("next_offset"),
    }


def is_finite_float(x: Any) -> bool:
    return isinstance(x, float) and math.isfinite(x)
