"""okg#3112 step 2, slice 2 and step 4a: the attribute-filter candidate switch
through the real MCP reader (PACT lane-h-3112-step2-reader, tasks switch and
readiness; PACT lane-h-3112-step4a; design.md sections 1, 3, 7 gate 3 and 9
items 6, 7 and 9). Native PostgreSQL.

The corpus is slice 1's gate-2 corpus (``_attr_filter_corpus``), published
twice over declared expression indexes the reconciler built before the
first write, so the writes maintained them. Every read goes through the
production ``filter`` tool, i.e. ``_effective_view_sql_page``.

Since step 4a ``OKG_ATTR_FILTER_INDEX`` accepts ``expression`` (also what an
unset switch reads as) and ``off``; no read touches ``okg.attr_index``.

* Gate 3: the default and an explicit ``expression`` return the page ``off``
  returns (rows and order) for every probe shape of gate 2, at the head and
  at a pinned generation, with identical statements and notes, and the page
  statement's plan names the declared index. Failure case: the renderer
  changes one token of the key expression and the plan no longer names it.
* The switch: ``attr_index`` and ``dual`` are refused on a filtered read with
  ``removed_value`` (failure case: a build that maps them to ``expression``
  is caught); the server starts with one set and logs one ERROR; the dual
  settings log one WARNING and change nothing; an unknown value is refused.
* Readiness (item 9): each not-ready state answers with the exact scan's
  rows and ``not_covered`` with its reason, and no other candidate set
  narrows that attribute; a ready attribute beside it still narrows.
* Overlays (item 7): a target whose overlay changes a declared key is
  returned; the expression set alone misses it.
* Random conjunctive predicates (item 6): default pages equal off pages.
"""

from __future__ import annotations

import logging
import os
import random
import re
from typing import Any, Callable, Iterator

import pytest

psycopg = pytest.importorskip("psycopg")

from psycopg.rows import dict_row  # noqa: E402
from psycopg.types.json import Jsonb  # noqa: E402

from okg.substrate import declared_attr_indexes as dai  # noqa: E402
from okg.substrate.attributes import (  # noqa: E402
    materialize_effective_attributes_for_generation,
    record_attribute_observation,
)
from okg.substrate.mcp import attr_filter_source as afs  # noqa: E402
from tests.substrate.db import declared_attr_index_corpora as corpora  # noqa: E402
from tests.substrate.mcp import _attr_filter_corpus as c  # noqa: E402

pytestmark = pytest.mark.postgres

DEFAULT = "default"
#: Every mode a read can be in: the two accepted values and an unset switch.
MODES = ("off", "expression", DEFAULT)
#: The modes that narrow (an unset switch reads as ``expression``).
NARROWING = ("expression", DEFAULT)
_DUAL_SETTINGS = ("OKG_ATTR_FILTER_DUAL_SAMPLE_EVERY", "OKG_ATTR_FILTER_DUAL_TIMEOUT_MS")
_PAGE_MARK = "page_limit"
_ATTR_INDEX = re.compile(r"\bokg\.attr_index")


@pytest.fixture
def corpus(clean_runtime: str, okg_mcp_can_log_in: str) -> Iterator[dict[str, Any]]:
    dsn = clean_runtime
    identity = c.seed(dsn)
    report = dai.reconcile(dsn, deployment=c.DEPLOYMENT, apply=True, enable=True)
    assert report["status"] == "converged", report
    pinned, head = c.load_corpus(dsn, identity)
    with c.Reader(dsn) as reader:
        yield {"dsn": dsn, "pinned": pinned, "head": head, "reader": reader,
               "identity": identity}


def _comparable(params: Any) -> dict[str, Any]:
    """The statement's parameters, with JSON adapters (which compare by
    identity) replaced by the value they wrap."""
    return {k: (v.obj if isinstance(v, Jsonb) else v) for k, v in dict(params or {}).items()}


class Capture:
    """The page statements the reader runs (text and parameters), and every
    statement any cursor runs (to prove none reads ``okg.attr_index``)."""

    def __init__(self, monkeypatch: pytest.MonkeyPatch) -> None:
        self.statements: list[tuple[str, dict[str, Any]]] = []
        self.everything: list[str] = []
        real = psycopg.Cursor.execute
        capture = self

        def execute(self: Any, query: Any, params: Any = None, **kwargs: Any) -> Any:
            text = str(query)
            capture.everything.append(text)
            if _PAGE_MARK in text and "effective_page" in text:
                capture.statements.append((text, dict(params or {})))
            return real(self, query, params, **kwargs)

        monkeypatch.setattr(psycopg.Cursor, "execute", execute)

    def last(self) -> tuple[str, dict[str, Any]]:
        assert self.statements, "no page statement ran"
        return self.statements[-1]

    def last_comparable(self) -> tuple[str, dict[str, Any]]:
        text, params = self.last()
        return text, _comparable(params)

    def assert_no_attr_index(self) -> None:
        touched = [s for s in self.everything if _ATTR_INDEX.search(s)]
        assert not touched, touched[:1]


def _set_mode(monkeypatch: pytest.MonkeyPatch, mode: str) -> None:
    if mode == DEFAULT:
        monkeypatch.delenv(afs.SWITCH_ENV, raising=False)
    else:
        monkeypatch.setenv(afs.SWITCH_ENV, mode)


def _read(corpus: dict[str, Any], monkeypatch: pytest.MonkeyPatch, mode: str,
          predicate: dict[str, Any], *, generation: str | None = None,
          **kw: Any) -> dict[str, Any]:
    _set_mode(monkeypatch, mode)
    result = corpus["reader"].filter(generation or corpus["head"], predicate, **kw)
    # A range bound the read check cannot read as an instant is refused
    # (issue #3666, 2c); every other read answers.
    assert "error" not in result or c.unreadable_ts_bound(predicate), (mode, predicate, result)
    return result


def _notes(result: dict[str, Any]) -> list[dict[str, Any]]:
    return [n for n in result.get("notes") or [] if n.get("pushdown") == "not_covered"]


def _reasons(result: dict[str, Any]) -> dict[str, str]:
    return {a["attr"]: a["reason"] for n in _notes(result) for a in n["attrs"]}


def _expression_narrowed(statement: str) -> bool:
    return "AND target_id IN (" in statement and "g.live_lo <= %(dai_bp)s" in statement


def _filters(dsn: str) -> list[dict[str, Any]]:
    with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor(row_factory=dict_row) as cur:
        accepted = [b for b, ok in zip(c.TS, dai.ts_bounds_accepted(cur, c.TS)) if ok]
    assert accepted == [t for t in c.TS if t in corpora.E14_V4_ADMITS]
    return c.num_filters() + c.text_filters() + c.ts_filters(accepted) + c.bool_filters()


# --------------------------------------------------------------------------- #
# Gate 3                                                                       #
# --------------------------------------------------------------------------- #


def test_gate3_default_and_expression_return_the_off_page_for_every_probe_shape(
    corpus: dict[str, Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    """Unset and an explicit ``expression`` run the same statement and return
    the same page and notes, equal to ``off``'s page; no statement of any read
    names ``okg.attr_index``."""
    filters = _filters(corpus["dsn"])
    assert len(filters) > 450
    capture = Capture(monkeypatch)
    narrowed = matched = refused = 0
    shapes: set[tuple[str, str]] = set()
    for pred in filters:
        off = c.answer(_read(corpus, monkeypatch, "off", pred))
        if "error" in off:
            # Issue #3666 (2c): a bound the read check cannot read as an
            # instant is refused in every mode (it was an empty page).
            for mode in NARROWING:
                assert c.answer(_read(corpus, monkeypatch, mode, pred)) == off, (mode, pred)
            refused += 1
            continue
        assert not _expression_narrowed(capture.last()[0])
        seen: dict[str, tuple[Any, ...]] = {}
        for mode in NARROWING:
            result = _read(corpus, monkeypatch, mode, pred)
            assert c.answer(result) == off, (mode, pred)
            statement, params = capture.last_comparable()
            seen[mode] = (c.answer(result), result.get("notes"), statement, params)
        assert seen[DEFAULT] == seen["expression"], pred
        statement = seen[DEFAULT][2]
        if _expression_narrowed(statement):
            narrowed += 1
            shapes.add((pred["field"], pred["op"]))
            assert seen[DEFAULT][1] is None, (pred, seen[DEFAULT][1])
        else:
            # Not narrowed: a ts bound v4 refuses, said in the response.
            assert seen[DEFAULT][1] == [
                {"pushdown": "not_covered",
                 "attrs": [{"attr": "t_ts", "reason": "bound_not_strict_iso"}]}
            ], (pred, seen[DEFAULT][1])
        matched += len(off["ids"])
    capture.assert_no_attr_index()
    assert matched > 20_000
    assert refused > 0
    assert narrowed >= len(filters) - refused - 3 * (len(c.TS) - len(corpora.E14_V4_ADMITS))
    for field in ("attrs.t_num", "attrs.t_text", "attrs.t_ts"):
        for op in ("eq", "in", "lt", "gt", "between"):
            assert (field, op) in shapes, (field, op)
    assert ("attrs.t_bool", "eq") in shapes and ("attrs.t_bool", "in") in shapes


def test_gate3_pages_orders_and_the_pinned_generation(
    corpus: dict[str, Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    preds = [
        c._mcp("t_num", "gt", 0.1), c._mcp("t_num", "between", [0, 0.1]),
        c._mcp("t_text", "lt", "b"), c._mcp("t_ts", "gt", "2024-01-02T03:04:05Z"),
        c._mcp("t_bool", "eq", True),
        {"op": "and", "clauses": [c._mcp("t_bool", "eq", False), c._mcp("t_num", "lt", 1.0)]},
    ]
    capture = Capture(monkeypatch)
    for pred in preds:
        for kw in ({"limit": 7, "offset": 3}, {"limit": 11, "offset": 0, "order_by": "attrs.t_num desc"},
                   {"limit": 5, "offset": 40, "order_by": "attrs.t_text"}):
            off = c.answer(_read(corpus, monkeypatch, "off", pred, **kw))
            assert off["ids"], (pred, kw)
            for mode in NARROWING:
                assert c.answer(_read(corpus, monkeypatch, mode, pred, **kw)) == off, (mode, pred, kw)
                assert _expression_narrowed(capture.last()[0]), (mode, pred, kw)
        # Pinned (non-head): no narrowing by any set; the response says why.
        off = c.answer(_read(corpus, monkeypatch, "off", pred, generation=corpus["pinned"]))
        for mode in NARROWING:
            result = _read(corpus, monkeypatch, mode, pred, generation=corpus["pinned"])
            assert c.answer(result) == off, (mode, pred)
            assert "AND target_id IN (" not in capture.last()[0], (mode, pred)
            reasons = _reasons(result)
            assert reasons and set(reasons.values()) == {"pinned_generation"}, result
    capture.assert_no_attr_index()


def _explain(dsn: str, statement: str, params: dict[str, Any]) -> str:
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute("SET LOCAL enable_seqscan = off")
        cur.execute("EXPLAIN (COSTS OFF) " + statement, params)
        return "\n".join(str(r[0]) for r in cur.fetchall())


_EXPLAINED = [
    ("t_num", c._mcp("t_num", "between", [0, 0.1]), "num"),
    ("t_num", c._mcp("t_num", "eq", 0.30000000000000004), "num"),
    ("t_text", c._mcp("t_text", "eq", "abc"), "text"),
    ("t_text", c._mcp("t_text", "gt", "x" * 600), "text"),
    ("t_ts", c._mcp("t_ts", "lt", "2024-01-02T03:04:05.123456789Z"), "ts"),
    ("t_ts", c._mcp("t_ts", "in", ["2024-01-02", "2024-02-29"]), "ts"),
    ("t_bool", c._mcp("t_bool", "eq", True), "bool"),
]


def _key_in_index_cond(plan: str, name: str) -> bool:
    """True when the plan reads index ``name`` with its key (the attribute
    expression) in the index condition, not only its live_lo/live_hi
    columns."""
    lines = plan.splitlines()
    for at, line in enumerate(lines):
        if name not in line:
            continue
        for follow in lines[at + 1:]:
            if follow.lstrip().startswith("->"):
                break
            if "Index Cond:" in follow and "attrs" in follow:
                return True
    return False


@pytest.mark.parametrize("mode", NARROWING)
def test_gate3_explain_of_the_page_statement_names_the_declared_index(
    corpus: dict[str, Any], monkeypatch: pytest.MonkeyPatch, mode: str
) -> None:
    capture = Capture(monkeypatch)
    for attr, pred, value_type in _EXPLAINED:
        _read(corpus, monkeypatch, mode, pred)
        statement, params = capture.last()
        assert _expression_narrowed(statement), pred
        plan = _explain(corpus["dsn"], statement, params)
        assert _key_in_index_cond(plan, dai.index_name("node", attr, value_type)), (pred, plan)
        assert "attr_index " not in plan and not _ATTR_INDEX.search(statement), plan


def test_gate3_failure_case_a_one_token_renderer_change_loses_the_index_key(
    corpus: dict[str, Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    pred = c._mcp("t_text", "eq", "abc")
    off = c.answer(_read(corpus, monkeypatch, "off", pred))
    real_render, real_key = dai.render_probe, dai.key_expression

    def one_token_off(attr_id: str, value_type: str, *, alias: str = "") -> str:
        return real_key(attr_id, value_type, alias=alias).replace(
            f", {dai.TEXT_KEY_CHARS})", f", {dai.TEXT_KEY_CHARS - 1})")

    def render(*args: Any, **kwargs: Any) -> Any:
        # Only the reader's probe changes; the readiness digest is the real one.
        with monkeypatch.context() as patch:
            patch.setattr(dai, "key_expression", one_token_off)
            return real_render(*args, **kwargs)

    monkeypatch.setattr(dai, "render_probe", render)
    capture = Capture(monkeypatch)
    result = _read(corpus, monkeypatch, DEFAULT, pred)
    statement, params = capture.last()
    assert _expression_narrowed(statement) and "511)" in statement
    plan = _explain(corpus["dsn"], statement, params)
    # The planner may still walk the index for its live_lo/live_hi columns
    # (the admission predicate is unchanged), but no longer by its key.
    assert not _key_in_index_cond(plan, dai.index_name("node", "t_text", "text")), plan
    # Still a superset, so still the exact answer.
    assert c.answer(result) == off


# --------------------------------------------------------------------------- #
# The switch: removed values, unknown values, the dual settings               #
# --------------------------------------------------------------------------- #


def _refusal(corpus: dict[str, Any], pred: dict[str, Any]) -> dict[str, Any] | None:
    """The refusal of a filtered read as ``{code, message, data, fix_hint}``,
    or None when the read answered. It may surface as a raised error or as an
    error envelope."""
    try:
        result = corpus["reader"].filter(corpus["head"], pred)
    except Exception as exc:  # noqa: BLE001 -- inspected below
        return {"code": getattr(exc, "code", None), "message": str(exc),
                "data": getattr(exc, "data", None), "fix_hint": getattr(exc, "fix_hint", None)}
    if "error" not in result:
        return None
    err = result["error"]
    if isinstance(err, dict):
        return {"code": err.get("code"), "message": err.get("message"),
                "data": err.get("data"), "fix_hint": err.get("fix_hint")}
    return {"code": None, "message": str(err), "data": None, "fix_hint": None}


@pytest.mark.parametrize("value", afs.REMOVED_MODES)
def test_a_removed_switch_value_is_refused_on_a_filtered_read(
    corpus: dict[str, Any], monkeypatch: pytest.MonkeyPatch, value: str
) -> None:
    """``attr_index`` and ``dual`` are refused by name, never read as another
    mode. Failure case: a build that maps the removed value to ``expression``
    answers the read, which this test's check reports."""
    assert afs.MODES == ("expression", "off")
    pred = c._mcp("t_text", "eq", "abc")
    monkeypatch.setenv(afs.SWITCH_ENV, value)
    refused = _refusal(corpus, pred)
    assert refused is not None, f"{afs.SWITCH_ENV}={value} answered a filtered read"
    assert refused["code"] == "invalid_configuration", refused
    assert refused["data"] == {"reason": "removed_value", "value": value,
                               "accepted": ["expression", "off"]}, refused
    assert refused["fix_hint"] == "unset OKG_ATTR_FILTER_INDEX, or set it to expression or off"
    assert afs.SWITCH_ENV in refused["message"] and value in refused["message"]

    # Failure case: a build that maps the removed value to expression.
    real_mode = afs.filter_index_mode

    def mapping_build() -> str:
        if os.environ.get(afs.SWITCH_ENV, "").strip() in afs.REMOVED_MODES:
            return "expression"
        return real_mode()

    with monkeypatch.context() as patch:
        patch.setattr(afs, "filter_index_mode", mapping_build)
        assert _refusal(corpus, pred) is None, "the mapping build must answer"
    # Back on the real build, still refused; unsetting it answers again.
    assert _refusal(corpus, pred) is not None
    monkeypatch.delenv(afs.SWITCH_ENV)
    assert _refusal(corpus, pred) is None


@pytest.mark.parametrize("value", afs.REMOVED_MODES)
def test_a_removed_switch_value_leaves_a_read_without_an_attribute_predicate_alone(
    corpus: dict[str, Any], monkeypatch: pytest.MonkeyPatch, value: str
) -> None:
    """Only a read whose predicate names an attribute is refused: a ``map``
    projecting attributes with no predicate answers as it does unset.
    Failure case: the same switch value refuses the same map once it filters
    on an attribute."""
    tools = corpus["reader"]._mcp._tool_manager._tools

    def mapped(**extra: Any) -> dict[str, Any]:
        return tools["map"].fn(input={"kind": "nodes", **extra},
                               projection={"v": "attrs.t_text"},
                               generation_id=corpus["head"], limit=5)

    monkeypatch.delenv(afs.SWITCH_ENV, raising=False)
    reference = mapped()
    assert "error" not in reference and reference.get("items"), reference
    monkeypatch.setenv(afs.SWITCH_ENV, value)
    result = mapped()
    assert "error" not in result, result
    assert result["items"] == reference["items"]
    try:
        refused = mapped(filter=c._mcp("t_text", "eq", "abc"))
    except Exception as exc:  # noqa: BLE001 -- the refusal may raise
        assert getattr(exc, "code", None) == "invalid_configuration", exc
    else:
        assert "error" in refused, refused


@pytest.mark.parametrize("value", ["expresion", "", "attr-index", "DUAL"])
def test_an_unknown_switch_value_is_refused(
    corpus: dict[str, Any], monkeypatch: pytest.MonkeyPatch, value: str
) -> None:
    monkeypatch.setenv(afs.SWITCH_ENV, value)
    refused = _refusal(corpus, c._mcp("t_text", "eq", "abc"))
    assert refused is not None, value
    assert refused["code"] == "invalid_configuration", refused
    assert refused["data"] == {"value": value, "known": ["expression", "off"]}, refused
    assert afs.SWITCH_ENV in refused["message"] + str(refused["fix_hint"]), refused


def test_the_dual_settings_have_no_effect_on_a_read(
    corpus: dict[str, Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    """Set (even to values step 3 refused), the dual-read settings change no
    page, note or statement in any mode."""
    preds = [c._mcp("t_text", "eq", "abc"), c._mcp("t_num", "between", [0, 0.1]),
             c._mcp("t_ts", "gt", "20240102")]
    capture = Capture(monkeypatch)

    def reads() -> list[Any]:
        out = []
        for pred in preds:
            for mode in MODES:
                result = _read(corpus, monkeypatch, mode, pred)
                out.append((c.answer(result), result.get("notes"), capture.last_comparable()))
        return out

    for name in _DUAL_SETTINGS:
        monkeypatch.delenv(name, raising=False)
    without = reads()
    for value in ("1", "0"):
        for name in _DUAL_SETTINGS:
            monkeypatch.setenv(name, value)
        assert reads() == without, value
    capture.assert_no_attr_index()


def _start_server(dsn: str, caplog: pytest.LogCaptureFixture) -> list[logging.LogRecord]:
    """Build (and close) one MCP server; the records ``attr_filter_source``
    logged while it started."""
    from okg.substrate.mcp import server as server_mod

    caplog.clear()
    caplog.set_level(logging.DEBUG, logger=afs.__name__)
    mcp = server_mod.build_server(dsn=dsn)
    try:
        return [r for r in caplog.records if r.name == afs.__name__]
    finally:
        mcp._okg_pool.close()
        if mcp._okg_write_pool:
            mcp._okg_write_pool.close()


@pytest.mark.parametrize("value", afs.REMOVED_MODES)
def test_the_server_starts_with_a_removed_value_and_logs_one_error(
    clean_runtime: str, okg_mcp_can_log_in: str, monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture, value: str,
) -> None:
    for name in _DUAL_SETTINGS:
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv(afs.SWITCH_ENV, value)
    records = _start_server(clean_runtime, caplog)
    errors = [r for r in records if r.levelno == logging.ERROR]
    assert len(errors) == 1, [r.getMessage() for r in records]
    assert f"{afs.SWITCH_ENV}={value}" in errors[0].getMessage()
    assert [r for r in records if r.levelno >= logging.WARNING] == errors


def test_the_server_starts_with_an_unknown_value_and_logs_one_error(
    clean_runtime: str, okg_mcp_can_log_in: str, monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A typo is named at start too; filtered reads refuse it."""
    for name in _DUAL_SETTINGS:
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv(afs.SWITCH_ENV, "expresion")
    records = _start_server(clean_runtime, caplog)
    errors = [r for r in records if r.levelno == logging.ERROR]
    assert len(errors) == 1 and "expresion" in errors[0].getMessage(), [
        r.getMessage() for r in records]


@pytest.mark.parametrize("value", afs.REMOVED_MODES)
def test_a_removed_value_is_refused_on_a_reserved_key_predicate_too(
    corpus: dict[str, Any], monkeypatch: pytest.MonkeyPatch, value: str
) -> None:
    """A predicate naming only a reserved key (the lineage envelope) is still
    an attribute-filtered read: refused, as any other."""
    from okg.substrate.attributes import RESERVED_EFFECTIVE_ATTR_KEYS

    key = sorted(RESERVED_EFFECTIVE_ATTR_KEYS)[0]
    pred = {"field": f"attrs.{key}", "op": "eq", "value": "x"}
    monkeypatch.setenv(afs.SWITCH_ENV, value)
    refused = _refusal(corpus, pred)
    assert refused is not None and refused["code"] == "invalid_configuration", refused


def test_prefix_attrs_names_top_level_prefix_clauses_on_declared_keys() -> None:
    """Ruling 10-04: a prefix clause on a declared key narrows by no index
    and is reported ``prefix_not_indexed``; undeclared keys, nested clauses
    and other ops are not."""
    declared = {"t_text": "text", "t_num": "num"}
    prefix = {"field": "attrs.t_text", "op": "prefix", "value": "ab"}
    assert afs.prefix_attrs(prefix, declared) == ["t_text"]
    assert afs.prefix_attrs({"op": "and", "clauses": [
        prefix, {"field": "attrs.t_num", "op": "eq", "value": 1},
        {"field": "attrs.other", "op": "prefix", "value": "x"}]}, declared) == ["t_text"]
    assert afs.prefix_attrs({"op": "or", "clauses": [prefix]}, declared) == []
    assert afs.prefix_attrs({"op": "not", "clause": prefix}, declared) == []
    assert afs.prefix_attrs({"field": "attrs.t_text", "op": "eq", "value": "ab"},
                            declared) == []
    assert afs.prefix_attrs(prefix, {}) == []


@pytest.mark.parametrize("value", [None, "expression", "off"])
def test_the_server_logs_nothing_for_an_accepted_value(
    clean_runtime: str, okg_mcp_can_log_in: str, monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture, value: str | None,
) -> None:
    for name in _DUAL_SETTINGS:
        monkeypatch.delenv(name, raising=False)
    _set_mode(monkeypatch, DEFAULT if value is None else value)
    records = _start_server(clean_runtime, caplog)
    assert [r for r in records if r.levelno >= logging.WARNING] == [], [
        r.getMessage() for r in records]


@pytest.mark.parametrize("names", [_DUAL_SETTINGS, _DUAL_SETTINGS[:1], _DUAL_SETTINGS[1:]])
def test_the_dual_settings_log_one_warning_at_start(
    clean_runtime: str, okg_mcp_can_log_in: str, monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture, names: tuple[str, ...],
) -> None:
    monkeypatch.delenv(afs.SWITCH_ENV, raising=False)
    for name in _DUAL_SETTINGS:
        monkeypatch.delenv(name, raising=False)
    for name in names:
        monkeypatch.setenv(name, "100")
    records = _start_server(clean_runtime, caplog)
    loud = [r for r in records if r.levelno >= logging.WARNING]
    assert [r.levelno for r in loud] == [logging.WARNING], [r.getMessage() for r in loud]
    message = loud[0].getMessage()
    for name in _DUAL_SETTINGS:
        assert (name in message) is (name in names), message


# --------------------------------------------------------------------------- #
# Readiness                                                                    #
# --------------------------------------------------------------------------- #


def _sql(dsn: str, statement: str, params: tuple[Any, ...] = ()) -> None:
    with psycopg.connect(dsn, autocommit=True) as conn:
        conn.execute(statement, params)


def _check_not_covered(corpus: dict[str, Any], monkeypatch: pytest.MonkeyPatch,
                       pred: dict[str, Any], attr: str, reason: str) -> None:
    """In the default and in ``expression``: the off page, ``not_covered``
    with ``reason``, and no candidate set at all (no other set narrows the
    attribute)."""
    capture = Capture(monkeypatch)
    off = c.answer(_read(corpus, monkeypatch, "off", pred))
    for mode in NARROWING:
        result = _read(corpus, monkeypatch, mode, pred)
        assert c.answer(result) == off, mode
        assert "AND target_id IN (" not in capture.last()[0], mode
        assert _reasons(result) == {attr: reason}, (mode, result)
    capture.assert_no_attr_index()


def _check_partial(corpus: dict[str, Any], monkeypatch: pytest.MonkeyPatch,
                   attr: str, reason: str) -> None:
    """``attr`` not ready beside a ready ``t_num``: t_num still narrows, the
    response names ``attr``, and the page is off's."""
    pred = {"op": "and", "clauses": [c._mcp(attr, "lt", "b"), c._mcp("t_num", "gt", 0.1)]}
    capture = Capture(monkeypatch)
    off = c.answer(_read(corpus, monkeypatch, "off", pred))
    assert off["ids"]
    for mode in NARROWING:
        result = _read(corpus, monkeypatch, mode, pred)
        assert c.answer(result) == off, mode
        assert _expression_narrowed(capture.last()[0]), mode
        assert _reasons(result) == {attr: reason}, (mode, result)
    capture.assert_no_attr_index()


_STATE_CASES: list[tuple[str, str, Callable[[str], tuple[str, tuple[Any, ...]]]]] = [
    ("index_building", "state",
     lambda name: ("UPDATE okg.declared_attr_indexes SET state = 'building' WHERE index_name = %s",
                   (name,))),
    ("index_failed", "state",
     lambda name: ("UPDATE okg.declared_attr_indexes SET state = 'failed' WHERE index_name = %s",
                   (name,))),
    ("index_missing", "state",
     lambda name: ("UPDATE okg.declared_attr_indexes SET state = 'pending' WHERE index_name = %s",
                   (name,))),
    ("declaration_changed", "digest",
     lambda name: ("UPDATE okg.declared_attr_indexes SET definition_digest = 'sha256:other' "
                   "WHERE index_name = %s", (name,))),
    ("view_graph_drift", "drift",
     lambda name: ("UPDATE okg.declared_attr_indexes SET drift = true WHERE index_name = %s",
                   (name,))),
    ("index_building", "invalid",
     lambda name: ("UPDATE pg_index SET indisvalid = false WHERE indexrelid = %s::regclass",
                   (f'public."{name}"',))),
    ("index_missing", "manifest_row",
     lambda name: ("DELETE FROM okg.declared_attr_indexes WHERE index_name = %s", (name,))),
    ("index_missing", "dropped",
     lambda name: (f'DROP INDEX public."{name}"', ())),
]


@pytest.mark.parametrize(("reason", "how", "mutate"), _STATE_CASES,
                         ids=[f"{r}-{h}" for r, h, _ in _STATE_CASES])
def test_readiness_each_not_ready_state_answers_with_the_exact_scan(
    corpus: dict[str, Any], monkeypatch: pytest.MonkeyPatch, reason: str, how: str,
    mutate: Callable[[str], tuple[str, tuple[Any, ...]]],
) -> None:
    """Each not-ready state, alone (failure case for ``dropped``: no other
    candidate set narrows the attribute) and beside a ready attribute, which
    still narrows (partial narrowing, new in step 4a for an explicit
    ``expression``)."""
    pred = c._mcp("t_text", "lt", "b")
    capture = Capture(monkeypatch)
    _read(corpus, monkeypatch, DEFAULT, pred)
    assert _expression_narrowed(capture.last()[0])  # ready before the change
    _sql(corpus["dsn"], *mutate(dai.index_name("node", "t_text", "text")))
    _check_not_covered(corpus, monkeypatch, pred, "t_text", reason)
    _check_partial(corpus, monkeypatch, "t_text", reason)
    # Another attribute's index is untouched and still narrows.
    _read(corpus, monkeypatch, DEFAULT, c._mcp("t_num", "gt", 0.5))
    assert _expression_narrowed(capture.last()[0])


def test_readiness_text_under_a_collation_that_refuses_text(
    corpus: dict[str, Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    """The database collation turns to one that does not order by code point
    (an ICU locale, simulated in the reader's process): a text index's
    definition digest no longer matches, so ``t_text`` reads as
    ``declaration_changed`` and is answered by the exact predicate; a number
    beside it still narrows."""
    pred = c._mcp("t_text", "lt", "b")
    capture = Capture(monkeypatch)
    _read(corpus, monkeypatch, DEFAULT, pred)
    assert _expression_narrowed(capture.last()[0])
    icu = {"provider": "i", "collate": "C", "locale": "en-US"}
    assert not dai.collation_orders_by_code_point(icu)
    monkeypatch.setattr(dai, "database_collation", lambda cur: dict(icu))
    _check_not_covered(corpus, monkeypatch, pred, "t_text", "declaration_changed")
    _check_partial(corpus, monkeypatch, "t_text", "declaration_changed")


def test_readiness_a_ts_bound_v4_refuses_is_never_pushed_in_any_mode(
    corpus: dict[str, Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    assert len(corpora.PYTHON_ONLY_TS_BOUNDS) == 11
    capture = Capture(monkeypatch)
    for bound in corpora.PYTHON_ONLY_TS_BOUNDS:
        for op in ("eq", "lt", "gt"):
            pred = c._mcp("t_ts", op, bound)
            off = c.answer(_read(corpus, monkeypatch, "off", pred))
            for mode in NARROWING:
                result = _read(corpus, monkeypatch, mode, pred)
                assert c.answer(result) == off, (mode, pred)
                statement = capture.last()[0]
                assert "AND target_id IN (" not in statement, (mode, pred)
                assert _reasons(result) == {"t_ts": "bound_not_strict_iso"}, (mode, pred, result)
    capture.assert_no_attr_index()


def test_readiness_pinned_generation_is_reported(
    corpus: dict[str, Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    pred = {"op": "and", "clauses": [c._mcp("t_text", "lt", "b"), c._mcp("t_num", "gt", 0)]}
    capture = Capture(monkeypatch)
    off = c.answer(_read(corpus, monkeypatch, "off", pred, generation=corpus["pinned"]))
    for mode in NARROWING:
        result = _read(corpus, monkeypatch, mode, pred, generation=corpus["pinned"])
        assert c.answer(result) == off, mode
        assert "AND target_id IN (" not in capture.last()[0], mode
        assert _reasons(result) == {"t_text": "pinned_generation", "t_num": "pinned_generation"}
    capture.assert_no_attr_index()


# --------------------------------------------------------------------------- #
# Overlays                                                                     #
# --------------------------------------------------------------------------- #


def test_overlay_that_changes_a_declared_key_is_returned_in_every_mode(
    corpus: dict[str, Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    dsn, head = corpus["dsn"], corpus["head"]
    moved = c.nid(10)
    with psycopg.connect(dsn, autocommit=True) as conn:
        record_attribute_observation(
            conn, target_kind="node", target_id=moved, generation_id=head,
            attr_key="t_text", value="overlaid-value", producer="test:attr-filter-source",
        )
        materialize_effective_attributes_for_generation(conn, generation_id=head)
    pred = c._mcp("t_text", "eq", "overlaid-value")
    capture = Capture(monkeypatch)
    for mode in MODES:
        assert c.answer(_read(corpus, monkeypatch, mode, pred))["ids"] == [moved], mode
    assert _expression_narrowed(capture.last()[0])
    # Failure case: the expression candidate set ALONE (no overlay union)
    # does not hold the overlaid target; the union is what returns it.
    with psycopg.connect(dsn, autocommit=True) as conn:
        plan, uncovered = afs.plan_expression_candidates(
            conn, generation_id=head, predicate=pred, declared=dict(c.DECL),
            target_kind="node",
        )
        assert plan is not None and uncovered == []
        rows = conn.execute(plan.sql, plan.params).fetchall()
    assert moved not in {r[0] for r in rows}
    # Moved away: its graph value no longer matches.
    old_value = c.node_attrs(10, 1)["t_text"]
    pred = c._mcp("t_text", "eq", old_value)
    off = c.answer(_read(corpus, monkeypatch, "off", pred))
    assert moved not in off["ids"]
    assert c.answer(_read(corpus, monkeypatch, DEFAULT, pred)) == off


# --------------------------------------------------------------------------- #
# Random conjunctive predicates                                                #
# --------------------------------------------------------------------------- #


def _random_clause(rnd: random.Random) -> dict[str, Any]:
    kind = rnd.choice(["num", "text", "ts", "bool", "residual"])
    if kind == "num":
        x = rnd.choice([0, 1, -1, 0.1, 0.5, 1e300, -1e10, 123.456, 9007199254740993, 1e-300])
        op = rnd.choice(["eq", "lt", "gt", "between", "in"])
        if op == "between":
            return c._mcp("t_num", op, sorted([x, rnd.choice([0, 1.0, 1e308, -5])]))
        if op == "in":
            return c._mcp("t_num", op, [x, rnd.choice([0, 0.3, 1])])
        return c._mcp("t_num", op, x)
    if kind == "text":
        x = rnd.choice(c.TEXTS[:30] + ["m", "b", "zz"])
        op = rnd.choice(["eq", "lt", "gt", "between", "in"])
        if op in ("between", "in"):
            return c._mcp("t_text", op, sorted([x, rnd.choice(["a", "x", "é"])]))
        return c._mcp("t_text", op, x)
    if kind == "ts":
        x = rnd.choice(list(corpora.E14_V4_ADMITS))
        op = rnd.choice(["eq", "lt", "gt", "between"])
        if op == "between":
            return c._mcp("t_ts", op, sorted([x, "2024-01-02T12:00:00Z"]))
        return c._mcp("t_ts", op, x)
    if kind == "bool":
        return c._mcp("t_bool", "eq", rnd.random() < 0.5)
    return rnd.choice([
        {"op": "exists", "field": "attrs.t_num"},
        {"op": "not", "clause": c._mcp("t_bool", "eq", True)},
        {"op": "or", "clauses": [c._mcp("t_text", "eq", "a"), c._mcp("t_num", "lt", 0)]},
        c._mcp("t_num", "eq", "42"),
    ])


def test_random_conjunctive_predicates_page_equal_with_and_without_candidates(
    corpus: dict[str, Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    rnd = random.Random(3112)
    capture = Capture(monkeypatch)
    narrowed = 0
    for _ in range(120):
        clauses = [_random_clause(rnd) for _ in range(rnd.randint(1, 3))]
        pred = clauses[0] if len(clauses) == 1 else {"op": "and", "clauses": clauses}
        kw = rnd.choice([{}, {"limit": 9, "offset": 2}, {"limit": 25, "order_by": "attrs.t_num"}])
        for generation in (corpus["head"], corpus["pinned"]):
            off = c.answer(_read(corpus, monkeypatch, "off", pred, generation=generation, **kw))
            default = c.answer(_read(corpus, monkeypatch, DEFAULT, pred,
                                     generation=generation, **kw))
            assert default == off, (pred, kw, generation)
            if generation == corpus["head"] and _expression_narrowed(capture.last()[0]):
                narrowed += 1
    assert narrowed > 60
    capture.assert_no_attr_index()


# --------------------------------------------------------------------------- #
# Non-UTC publisher and reader                                                 #
# --------------------------------------------------------------------------- #


@pytest.fixture
def zoned_corpus(
    request: pytest.FixtureRequest, clean_runtime: str, okg_mcp_can_log_in: str
) -> Iterator[dict[str, Any]]:
    """The corpus published and read with both sessions in a non-UTC zone."""
    dsn, zone = clean_runtime, request.param
    c.set_database_timezone(dsn, zone)
    identity = c.seed(dsn)
    report = dai.reconcile(dsn, deployment=c.DEPLOYMENT, apply=True, enable=True)
    assert report["status"] == "converged", report
    pinned, head = c.load_corpus(dsn, identity)
    with c.Reader(dsn) as reader:
        c.assert_zone_took_effect(dsn, zone, reader)
        yield {"dsn": dsn, "pinned": pinned, "head": head, "reader": reader,
               "identity": identity, "zone": zone}


@pytest.mark.parametrize("zoned_corpus", c.NON_UTC_ZONES, indirect=True)
def test_gate3_non_utc_publisher_and_reader_every_mode_returns_the_off_page(
    zoned_corpus: dict[str, Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    """The exact predicate reads stored values in UTC (okg.attr_ts_v4), and so
    does the ts expression index, whatever zone the publisher and reader
    were in: every mode returns the off page for every ts probe shape v4
    accepts, in pages and orders, at the head and at a pinned generation."""
    corpus = zoned_corpus
    accepted = [t for t in c.TS if t in corpora.E14_V4_ADMITS]
    preds = [p for p in c.ts_filters(accepted)
             if all(v in corpora.E14_V4_ADMITS for v in p.get("values", [p.get("value")]))]
    preds += [
        {"op": "and", "clauses": [c._mcp("t_ts", "lt", "2024-01-03"), c._mcp("t_bool", "eq", True)]},
        {"op": "and", "clauses": [c._mcp("t_ts", "gt", "2024-01-01"),
                                  c._mcp("t_ts", "lt", "2024-01-02T12:00:00Z")]},
    ]
    assert len(preds) > 80
    capture = Capture(monkeypatch)
    matched = narrowed = refused = 0
    for pred in preds:
        off = c.answer(_read(corpus, monkeypatch, "off", pred))
        for mode in NARROWING:
            result = _read(corpus, monkeypatch, mode, pred)
            assert c.answer(result) == off, (corpus["zone"], mode, pred)
            if "error" not in off:
                narrowed += _expression_narrowed(capture.last()[0])
        if "error" in off:
            # Issue #3666 (2c): a bound v4 admits but the read check cannot
            # read as an instant (a leap second) is refused in every mode.
            refused += 1
            continue
        matched += len(off["ids"])
    assert matched > 2_000
    assert narrowed == len(NARROWING) * (len(preds) - refused)
    for pred in (c._mcp("t_ts", "eq", "2024-01-02"), c._mcp("t_ts", "gt", "2024-01-02T03:04:05Z"),
                 c._mcp("t_ts", "between", ["2024-01-02", "2024-02-29"])):
        for kw in ({"limit": 7, "offset": 3}, {"limit": 11, "order_by": "attrs.t_ts desc"},
                   {"limit": 5, "offset": 4, "order_by": "attrs.t_text"}):
            off = c.answer(_read(corpus, monkeypatch, "off", pred, **kw))
            assert off["ids"], (pred, kw)
            got = c.answer(_read(corpus, monkeypatch, DEFAULT, pred, **kw))
            assert got == off, (corpus["zone"], pred, kw)
        off = c.answer(_read(corpus, monkeypatch, "off", pred, generation=corpus["pinned"]))
        got = c.answer(_read(corpus, monkeypatch, DEFAULT, pred, generation=corpus["pinned"]))
        assert got == off, (corpus["zone"], pred)
    capture.assert_no_attr_index()


@pytest.mark.parametrize("zoned_corpus", c.EXTREME_ZONES, indirect=True)
def test_gate3_publisher_at_the_largest_offsets_every_mode_returns_the_off_page(
    zoned_corpus: dict[str, Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    """Publisher zones 167 hours and (summer time) 168:59 from UTC, both
    accepted by PostgreSQL: the default returns the off page for date-only
    and instant bounds."""
    corpus = zoned_corpus
    preds = [c._mcp("t_ts", op, v) for v in ("2024-01-02", "2024-02-29", "2024-01-02T03:04:05Z")
             for op in ("eq", "lt", "gt")]
    preds += [c._mcp("t_ts", "between", ["2024-01-02", "2024-01-02T10:00Z"]),
              c._mcp("t_ts", "in", ["2024-01-02", "0001-01-01"])]
    matched = 0
    for pred in preds:
        off = c.answer(_read(corpus, monkeypatch, "off", pred))
        matched += len(off["ids"])
        for mode in NARROWING:
            assert c.answer(_read(corpus, monkeypatch, mode, pred)) == off, (mode, pred)
    assert matched > 500


# --------------------------------------------------------------------------- #
# Readiness: an unreadable manifest; ts refusals narrow only their clause      #
# --------------------------------------------------------------------------- #


def test_readiness_an_unreadable_manifest_is_index_missing(
    corpus: dict[str, Any], monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """The MCP read role cannot read okg.declared_attr_indexes (a grant the
    registry apply skipped): nothing vouches for the indexes, so the read
    answers with the exact scan, reports index_missing, and warns once."""
    caplog.set_level(logging.WARNING, logger=afs.__name__)
    pred = c._mcp("t_text", "lt", "b")
    capture = Capture(monkeypatch)
    _read(corpus, monkeypatch, DEFAULT, pred)
    assert _expression_narrowed(capture.last()[0])  # readable before the revoke
    _sql(corpus["dsn"], "REVOKE SELECT ON okg.declared_attr_indexes FROM okg_mcp")
    monkeypatch.setattr(afs, "_unreadable_warned", [])
    _check_not_covered(corpus, monkeypatch, pred, "t_text", "index_missing")
    _check_not_covered(corpus, monkeypatch, c._mcp("t_num", "gt", 0.5), "t_num", "index_missing")
    warned = [r for r in caplog.records if "manifest is unreadable" in r.getMessage()]
    assert len(warned) == 1, [r.getMessage() for r in warned]


def test_a_refused_ts_bound_leaves_the_other_clauses_narrowing(
    corpus: dict[str, Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    """A ts bound v4 refuses takes only its attribute's clauses out of the
    candidate set: the text clause beside it still narrows, the response
    names the refused attribute, and the page is off's."""
    pred = {"op": "and", "clauses": [c._mcp("t_ts", "gt", "20240102"),
                                     c._mcp("t_text", "lt", "b")]}
    capture = Capture(monkeypatch)
    off = c.answer(_read(corpus, monkeypatch, "off", pred))
    assert off["ids"]
    for mode in NARROWING:
        result = _read(corpus, monkeypatch, mode, pred)
        assert c.answer(result) == off, mode
        assert _reasons(result) == {"t_ts": "bound_not_strict_iso"}, (mode, result)
        assert _expression_narrowed(capture.last()[0]), mode
    capture.assert_no_attr_index()


def test_ts_bounds_are_checked_in_one_round_trip(
    corpus: dict[str, Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    """The switch's refusal check asks the database about every ts bound of a
    call in one statement, and asks nothing when no ts bound is well shaped
    (the probe renderer then asks again per clause it renders)."""
    checks: list[str] = []
    in_refusal_check: list[int] = []
    real = psycopg.Cursor.execute
    real_refused = afs.ts_refused_attrs

    def execute(self: Any, query: Any, params: Any = None, **kwargs: Any) -> Any:
        if "okg.attr_ts_v4(v) IS NOT NULL AS ok" in str(query):
            checks.append(str(query))
        return real(self, query, params, **kwargs)

    def refused(*args: Any, **kwargs: Any) -> Any:
        before = len(checks)
        try:
            return real_refused(*args, **kwargs)
        finally:
            in_refusal_check.append(len(checks) - before)

    monkeypatch.setattr(psycopg.Cursor, "execute", execute)
    monkeypatch.setattr(afs, "ts_refused_attrs", refused)
    three_ts = {"op": "and", "clauses": [
        c._mcp("t_ts", "gt", "2024-01-01"),
        c._mcp("t_ts", "lt", "2024-02-29T00:00:00Z"),
        c._mcp("t_ts", "between", ["2023-01-01", "2025-01-01"]),
    ]}
    off = c.answer(_read(corpus, monkeypatch, "off", three_ts))
    assert checks == [] and in_refusal_check == []
    assert c.answer(_read(corpus, monkeypatch, DEFAULT, three_ts)) == off
    assert in_refusal_check == [1], in_refusal_check
    del checks[:], in_refusal_check[:]
    _read(corpus, monkeypatch, DEFAULT, c._mcp("t_ts", "gt", "20240102"))
    _read(corpus, monkeypatch, DEFAULT, c._mcp("t_text", "lt", "b"))
    assert checks == [], checks
