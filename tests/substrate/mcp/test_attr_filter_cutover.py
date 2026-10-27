"""okg#3112 steps 3 and 4a: the default attribute-filter candidate source
(PACT lane-h-3112-step3, decisions 287a and 287d; PACT lane-h-3112-step4a;
design.md section 7 steps 3 and 4). Native PostgreSQL.

With ``OKG_ATTR_FILTER_INDEX`` unset, a filtered read narrows through the
ready declared expression indexes, exactly as an explicit ``expression``
does. Since step 4a no other candidate set exists: an attribute whose
expression index is not ready is answered by the exact predicate alone and
named in a ``not_covered`` note, while the ready attributes beside it still
narrow. No read touches ``okg.attr_index``. Every read returns the page
``off`` returns, rows and order.

The corpus is slice 1's gate-2 corpus (``_attr_filter_corpus``), published
twice over declared expression indexes the reconciler built before the
first write. Every read goes through the production ``filter`` tool.

* The switch: unset reads as ``expression``; ``attr_index`` and ``dual`` are
  refused with ``removed_value``; an empty value is refused.
* Gate 3 for the default: the ``off`` page for every probe shape of gate 2,
  narrowed by the expression index alone.
* Pinned (non-head) reads: narrowed by no set, ``pinned_generation`` named,
  rows equal ``off``.
* Each not-ready state, alone (narrowed by no set: the failure case) and next
  to a ready attribute (which still narrows), in the default and in an
  explicit ``expression``, with identical pages and notes.
"""

from __future__ import annotations

import re
from typing import Any, Callable, Iterator

import pytest

psycopg = pytest.importorskip("psycopg")

from psycopg.rows import dict_row  # noqa: E402

from okg.substrate import declared_attr_indexes as dai  # noqa: E402
from okg.substrate.errors import SubstrateError  # noqa: E402
from okg.substrate.mcp import attr_filter_source as afs  # noqa: E402
from tests.substrate.db import declared_attr_index_corpora as corpora  # noqa: E402
from tests.substrate.mcp import _attr_filter_corpus as c  # noqa: E402

pytestmark = pytest.mark.postgres

_PAGE_MARK = "page_limit"
_ATTR_INDEX = re.compile(r"\bokg\.attr_index")
DEFAULT = "default"


@pytest.fixture
def corpus(clean_runtime: str, okg_mcp_can_log_in: str) -> Iterator[dict[str, Any]]:
    dsn = clean_runtime
    identity = c.seed(dsn)
    report = dai.reconcile(dsn, deployment=c.DEPLOYMENT, apply=True, enable=True)
    assert report["status"] == "converged", report
    pinned, head = c.load_corpus(dsn, identity)
    with c.Reader(dsn) as reader:
        yield {"dsn": dsn, "pinned": pinned, "head": head, "reader": reader}


class Capture:
    """The page statements the reader runs, and every statement any cursor
    runs (to prove none reads ``okg.attr_index``)."""

    def __init__(self, monkeypatch: pytest.MonkeyPatch) -> None:
        self.statements: list[str] = []
        self.everything: list[str] = []
        real = psycopg.Cursor.execute
        capture = self

        def execute(self: Any, query: Any, params: Any = None, **kwargs: Any) -> Any:
            text = str(query)
            capture.everything.append(text)
            if _PAGE_MARK in text and "effective_page" in text:
                capture.statements.append(text)
            return real(self, query, params, **kwargs)

        monkeypatch.setattr(psycopg.Cursor, "execute", execute)

    def last(self) -> str:
        assert self.statements, "no page statement ran"
        return self.statements[-1]

    def assert_no_attr_index(self) -> None:
        touched = [s for s in self.everything if _ATTR_INDEX.search(s)]
        assert not touched, touched[:1]


def _read(corpus: dict[str, Any], monkeypatch: pytest.MonkeyPatch, mode: str,
          predicate: dict[str, Any], *, generation: str | None = None,
          **kw: Any) -> dict[str, Any]:
    if mode == DEFAULT:
        monkeypatch.delenv(afs.SWITCH_ENV, raising=False)
    else:
        monkeypatch.setenv(afs.SWITCH_ENV, mode)
    result = corpus["reader"].filter(generation or corpus["head"], predicate, **kw)
    # A range bound the read check cannot read as an instant is refused
    # (issue #3666, 2c); every other read answers.
    assert "error" not in result or c.unreadable_ts_bound(predicate), (mode, predicate, result)
    return result


@pytest.fixture(autouse=True)
def ids_first(monkeypatch: pytest.MonkeyPatch) -> None:
    """Every page here is read ids first. The graph page reads a small
    scope merged instead, with no candidate set (``graph_read.small_scope``,
    a cost choice with the same rows); these tests are about where the
    declared indexes narrow, which only the ids-first form shows."""
    from okg.substrate.mcp import graph_read

    monkeypatch.setattr(graph_read, "small_scope", lambda *_a, **_k: None)


def _page(corpus: dict[str, Any], monkeypatch: pytest.MonkeyPatch, capture: "Capture",
          mode: str, predicate: dict[str, Any], **kw: Any) -> tuple[dict[str, Any], str]:
    """The page of ``predicate`` and its page statement."""
    result = _read(corpus, monkeypatch, mode, predicate, **kw)
    return result, capture.last()


def _by_expression(statement: str) -> bool:
    """The graph-id branch is narrowed by the declared expression probe."""
    return "g.live_lo <= %(dai_bp)s" in statement


def _narrowed(statement: str) -> bool:
    """The graph-id branch is narrowed by any candidate set: the ordered
    node page joins the candidate ids (``graph_read.id_stream_branches``)."""
    return "candidate_ids(target_id)" in statement or "graph_rows.target_id IN (" in statement


def _sql(dsn: str, statement: str, params: tuple[Any, ...] = ()) -> None:
    with psycopg.connect(dsn, autocommit=True) as conn:
        conn.execute(statement, params)


def _not_covered(*entries: tuple[str, str]) -> dict[str, Any]:
    return {"pushdown": "not_covered", "attrs": [{"attr": a, "reason": r} for a, r in entries]}


def _filters(dsn: str) -> list[dict[str, Any]]:
    with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor(row_factory=dict_row) as cur:
        accepted = [b for b, ok in zip(c.TS, dai.ts_bounds_accepted(cur, c.TS)) if ok]
    assert accepted == [t for t in c.TS if t in corpora.E14_V4_ADMITS]
    return c.num_filters() + c.text_filters() + c.ts_filters(accepted) + c.bool_filters()


# --------------------------------------------------------------------------- #
# The switch                                                                   #
# --------------------------------------------------------------------------- #


def test_unset_is_expression_and_the_removed_values_are_refused(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    assert afs.MODES == ("expression", "off")
    monkeypatch.delenv(afs.SWITCH_ENV, raising=False)
    assert afs.filter_index_mode() == "expression"
    assert not hasattr(afs, "attr_index_fallback")
    for mode in afs.MODES:
        monkeypatch.setenv(afs.SWITCH_ENV, mode)
        assert afs.filter_index_mode() == mode
    # Failure case: a removed value is refused by name, never read as a mode.
    for removed in ("attr_index", "dual", " attr_index "):
        monkeypatch.setenv(afs.SWITCH_ENV, removed)
        with pytest.raises(SubstrateError) as excinfo:
            afs.filter_index_mode()
        assert excinfo.value.code == "invalid_configuration"
        assert excinfo.value.data == {"reason": "removed_value", "value": removed,
                                      "accepted": ["expression", "off"]}
        assert excinfo.value.fix_hint == (
            "unset OKG_ATTR_FILTER_INDEX, or set it to expression or off")
    # An empty value is not "unset": refused, never read as the default.
    monkeypatch.setenv(afs.SWITCH_ENV, "")
    with pytest.raises(SubstrateError) as excinfo:
        afs.filter_index_mode()
    assert excinfo.value.code == "invalid_configuration"
    assert excinfo.value.data == {"value": "", "known": ["expression", "off"]}


# --------------------------------------------------------------------------- #
# Gate 3 for the default, and pinned reads                                     #
# --------------------------------------------------------------------------- #


def test_default_returns_the_off_page_for_every_probe_shape_through_the_expression_index(
    corpus: dict[str, Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    filters = _filters(corpus["dsn"])
    assert len(filters) > 450
    capture = Capture(monkeypatch)
    narrowed = refused = 0
    for pred in filters:
        off = c.answer(_read(corpus, monkeypatch, "off", pred))
        result, statement = _page(corpus, monkeypatch, capture, DEFAULT, pred)
        assert c.answer(result) == off, pred
        if "error" in off:
            # Issue #3666 (2c): a bound the read check cannot read as an
            # instant is refused in both modes (it was an empty page).
            refused += 1
            continue
        if _by_expression(statement):
            narrowed += 1
            assert "notes" not in result, (pred, result["notes"])
        else:
            # Not narrowed: a ts bound v4 refuses, said in the response.
            assert not _narrowed(statement), pred
            assert result.get("notes") == [_not_covered(("t_ts", "bound_not_strict_iso"))], (
                pred, result)
    capture.assert_no_attr_index()
    assert refused > 0
    assert narrowed >= len(filters) - refused - 3 * (len(c.TS) - len(corpora.E14_V4_ADMITS))


def test_default_pinned_reads_are_narrowed_by_no_set_and_say_so(
    corpus: dict[str, Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    """A read pinned to a non-head generation: no narrowing at all, and the
    response names ``pinned_generation`` for every attribute."""
    preds = [
        c._mcp("t_num", "gt", 0.1), c._mcp("t_text", "lt", "b"),
        c._mcp("t_ts", "gt", "2024-01-02T03:04:05Z"), c._mcp("t_bool", "eq", True),
        {"op": "and", "clauses": [c._mcp("t_bool", "eq", False), c._mcp("t_num", "lt", 1.0)]},
    ]
    capture = Capture(monkeypatch)
    for pred in preds:
        off = c.answer(_read(corpus, monkeypatch, "off", pred, generation=corpus["pinned"]))
        assert off["ids"], pred
        clauses = [pred] if "field" in pred else pred["clauses"]
        attrs = list(dict.fromkeys(cl["field"].split(".", 1)[1] for cl in clauses))
        for mode in (DEFAULT, "expression"):
            result = _read(corpus, monkeypatch, mode, pred, generation=corpus["pinned"])
            assert c.answer(result) == off, (mode, pred)
            assert not _narrowed(capture.last()), (mode, pred)
            assert result.get("notes") == [
                _not_covered(*[(a, "pinned_generation") for a in attrs])
            ], (mode, pred, result.get("notes"))
    capture.assert_no_attr_index()


# --------------------------------------------------------------------------- #
# Not-ready attributes                                                         #
# --------------------------------------------------------------------------- #


_NOT_READY: list[tuple[str, str, Callable[[str], tuple[str, tuple[Any, ...]]]]] = [
    ("index_missing", "never_built",
     lambda name: ("UPDATE okg.declared_attr_indexes SET state = 'pending' WHERE index_name = %s",
                   (name,))),
    ("index_building", "building",
     lambda name: ("UPDATE okg.declared_attr_indexes SET state = 'building' WHERE index_name = %s",
                   (name,))),
    ("index_failed", "failed",
     lambda name: ("UPDATE okg.declared_attr_indexes SET state = 'failed' WHERE index_name = %s",
                   (name,))),
    ("declaration_changed", "digest",
     lambda name: ("UPDATE okg.declared_attr_indexes SET definition_digest = 'sha256:other' "
                   "WHERE index_name = %s", (name,))),
    ("index_missing", "dropped",
     lambda name: (f'DROP INDEX public."{name}"', ())),
]


@pytest.mark.parametrize(("reason", "how", "mutate"), _NOT_READY,
                         ids=[f"{r}-{h}" for r, h, _ in _NOT_READY])
def test_a_not_ready_attribute_is_answered_by_the_exact_predicate_and_named(
    corpus: dict[str, Any], monkeypatch: pytest.MonkeyPatch, reason: str, how: str,
    mutate: Callable[[str], tuple[str, tuple[Any, ...]]],
) -> None:
    dsn = corpus["dsn"]
    alone = c._mcp("t_text", "lt", "b")
    mixed = {"op": "and", "clauses": [c._mcp("t_text", "lt", "b"), c._mcp("t_num", "gt", 0.1)]}
    capture = Capture(monkeypatch)
    _result, statement = _page(corpus, monkeypatch, capture, DEFAULT, alone)
    assert _by_expression(statement)
    _sql(dsn, *mutate(dai.index_name("node", "t_text", "text")))

    off = c.answer(_read(corpus, monkeypatch, "off", alone))
    off_mixed = c.answer(_read(corpus, monkeypatch, "off", mixed))
    assert off["ids"] and off_mixed["ids"]
    seen: dict[str, Any] = {}
    for mode in (DEFAULT, "expression"):
        # Alone: failure case, no other candidate set narrows t_text.
        result, statement = _page(corpus, monkeypatch, capture, mode, alone)
        assert c.answer(result) == off, mode
        assert not _narrowed(statement), (mode, statement)
        assert result.get("notes") == [_not_covered(("t_text", reason))], (mode, result)
        alone_seen = (result.get("notes"), statement)

        # Next to a ready attribute: t_num still narrows through its
        # expression index; t_text is named.
        result, statement = _page(corpus, monkeypatch, capture, mode, mixed)
        assert c.answer(result) == off_mixed, mode
        assert _by_expression(statement), mode
        assert result.get("notes") == [_not_covered(("t_text", reason))], (mode, result)
        seen[mode] = (alone_seen, result.get("notes"), statement)
    assert seen[DEFAULT] == seen["expression"]
    capture.assert_no_attr_index()


def test_several_not_ready_attributes_are_each_named_and_a_ready_one_narrows(
    corpus: dict[str, Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    """t_text failed and t_bool building: both answered by the exact
    predicate, each named with its reason in one note; t_num, ready, still
    narrows. No read touches okg.attr_index (dropped by migration 0043 since
    okg#3112 step 4b)."""
    dsn = corpus["dsn"]
    _sql(dsn, "UPDATE okg.declared_attr_indexes SET state = 'failed' WHERE index_name = %s",
         (dai.index_name("node", "t_text", "text"),))
    _sql(dsn, "UPDATE okg.declared_attr_indexes SET state = 'building' WHERE index_name = %s",
         (dai.index_name("node", "t_bool", "bool"),))
    capture = Capture(monkeypatch)
    pred = {"op": "and", "clauses": [
        c._mcp("t_text", "lt", "b"), c._mcp("t_bool", "eq", True), c._mcp("t_num", "gt", 0.1),
    ]}
    off = c.answer(_read(corpus, monkeypatch, "off", pred))
    assert off["ids"]
    expected = [_not_covered(("t_text", "index_failed"), ("t_bool", "index_building"))]
    before = []
    for mode in (DEFAULT, "expression"):
        result, statement = _page(corpus, monkeypatch, capture, mode, pred)
        assert c.answer(result) == off, mode
        assert _by_expression(statement), mode
        assert result.get("notes") == expected, (mode, result.get("notes"))
        before.append((c.answer(result), statement))
    capture.assert_no_attr_index()
    # The tables are gone: a read never depended on them.
    with psycopg.connect(dsn, autocommit=True) as conn:
        gone = conn.execute(
            "SELECT to_regclass('okg.attr_index'), to_regclass('okg.attr_index_coverage')"
        ).fetchone()
    assert gone == (None, None), gone
    del capture.everything[:]
    for mode in (DEFAULT, "expression"):
        result, statement = _page(corpus, monkeypatch, capture, mode, pred)
        assert result.get("notes") == expected, mode
        assert (c.answer(result), statement) in before, mode
    capture.assert_no_attr_index()
