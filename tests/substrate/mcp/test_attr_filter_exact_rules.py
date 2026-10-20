"""okg#3112 step 2, slice 2: the exact predicate's ts and number rules,
against dev's answers (PACT lane-h-3112-step2-reader, task exact-rules;
design.md sections 2.2, 2.4 and 10, questions 6 and 10). Native PostgreSQL.

``data/attr_filter_dev_answers.json`` holds what dev's reader (the commit it
names, before this slice) answered for every E13 number filter and every
E14 ts filter over the same corpus, recorded by
``pact/changes/lane-h-3112-step2-reader/scripts/dev_answers_test.py``. Here
the same filters go through the real reader in every accepted mode (``off``
and ``expression``; since okg#3112 step 4a, PACT lane-h-3112-step4a, the
``attr_index`` and ``dual`` modes are gone), and every answer that differs
from dev's must be one of the listed changes:

* question 10: dev raised 22003 (a number beyond double range in the
  attribute) and the filter now answers, with exactly the rows a double-
  precision comparison (``okg.attr_num_f8``, Python's ``float()``) and the
  Python re-check accept;
* question 6c: dev raised on a ts bound v4 refuses (attr_index cast it in
  SQL); it is no longer pushed down and answers through the exact predicate;
* question 6a: a stored ts value today's SQL and Python accept but v4
  refuses no longer matches by instant;
* question 6d: a 7-9 digit stored value or bound is rounded to microseconds
  by PostgreSQL instead of truncated by Python;
* one change outside the design's list, reported in the PACT evidence: dev
  raised DataError on an ``in`` list mixing ints and floats (attr_index
  bound the list as one array); it now answers with the exact rows.

Nothing else may differ. ``v = 1e400, gt 1.0`` returns its row.
"""

from __future__ import annotations

import json
import re
from decimal import Decimal
from pathlib import Path
from typing import Any, Iterator

import pytest

psycopg = pytest.importorskip("psycopg")

from okg.substrate import declared_attr_indexes as dai  # noqa: E402
from okg.substrate.mcp import attr_filter_source as afs  # noqa: E402
from tests.substrate.db import declared_attr_index_corpora as corpora  # noqa: E402
from tests.substrate.mcp import _attr_filter_corpus as c  # noqa: E402

pytestmark = pytest.mark.postgres

MODES = ("off", "expression")
GOLDEN = json.loads(
    (Path(__file__).parent / "data" / "attr_filter_dev_answers.json").read_text("utf-8")
)
_SUB_MICRO = re.compile(r"[.,][0-9]{7,9}(?![0-9])")


@pytest.fixture
def corpus(clean_runtime: str, okg_mcp_can_log_in: str) -> Iterator[dict[str, Any]]:
    dsn = clean_runtime
    identity = c.seed(dsn)
    report = dai.reconcile(dsn, deployment=c.DEPLOYMENT, apply=True, enable=True)
    assert report["status"] == "converged", report
    _, head = c.load_corpus(dsn, identity)
    with c.Reader(dsn) as reader:
        yield {"dsn": dsn, "head": head, "reader": reader}


def _read(corpus: dict[str, Any], monkeypatch: pytest.MonkeyPatch, mode: str,
          pred: dict[str, Any]) -> dict[str, Any]:
    monkeypatch.setenv(afs.SWITCH_ENV, mode)
    result = corpus["reader"].filter(corpus["head"], pred)
    # A range bound the read check cannot read as an instant is refused
    # (issue #3666, 2c); every other read answers.
    assert "error" not in result or c.unreadable_ts_bound(pred), (mode, pred, result)
    return result


def _ids(answer: dict[str, Any]) -> list[int]:
    assert not answer["truncated"], answer
    return [int(str(i).rsplit(":", 1)[1]) for i in answer["ids"]]


def _head_rev(i: int) -> int:
    return 2 if i % 3 == 0 else 1


# --------------------------------------------------------------------------- #
# Numbers                                                                      #
# --------------------------------------------------------------------------- #


def _decimal(x: Any) -> Decimal:
    return Decimal(repr(x)) if isinstance(x, float) else Decimal(x)


def _num_oracle(pred: dict[str, Any]) -> list[int]:
    """Rows the revised exact filter (design 2.4) and the Python re-check
    both accept: equality as exact decimals (jsonb) and as Python values; a
    float range bound in double precision, an int bound exactly; Python
    compares the decoded JSON number with the bound."""
    op = pred["op"]
    bounds = pred.get("values") if op in ("in", "between") else [pred["value"]]
    out = []
    for i in range(c.N_NODES):
        text = c.num_text(i, _head_rev(i))
        if text is None:
            continue
        # The Python re-check reads the value as PostgreSQL prints the stored
        # jsonb number (plain decimal notation: 1e300 comes back as an int).
        exact = Decimal(text)
        py = json.loads(format(exact, "f"))
        if op in ("eq", "in"):
            sql_ok = any(exact == _decimal(b) for b in bounds)
            py_ok = any(py == b for b in bounds)
        else:
            sides = ([(">=", bounds[0]), ("<=", bounds[1])] if op == "between"
                     else [("<" if op == "lt" else ">", bounds[0])])

            def cmp(left: Any, sym: str, right: Any) -> bool:
                return {"<": left < right, ">": left > right,
                        "<=": left <= right, ">=": left >= right}[sym]

            sql_ok = all(cmp(float(text), s, b) if isinstance(b, float)
                         else cmp(exact, s, Decimal(b)) for s, b in sides)
            py_ok = all(cmp(py, s, b) for s, b in sides)
        if sql_ok and py_ok:
            out.append(i)
    return out


def _mixed_in(pred: dict[str, Any]) -> bool:
    values = pred.get("values") or []
    return pred["op"] == "in" and len({type(v) for v in values}) > 1


def test_e13_numbers_every_change_against_dev_is_listed(
    corpus: dict[str, Any], monkeypatch: pytest.MonkeyPatch,
    record_property: Any,
) -> None:
    counts = {"unchanged": 0, "q10_22003_now_answers": 0, "mixed_in_list_now_answers": 0}
    filters = c.num_filters()
    assert len(filters) == 150
    for pred in filters:
        dev = GOLDEN["answers"][c.predicate_key(pred)]
        answers = {mode: c.answer(_read(corpus, monkeypatch, mode, pred)) for mode in MODES}
        assert all(a == answers["off"] for a in answers.values()), pred
        new = _ids(answers["off"])
        if "error" not in dev:
            assert new == dev["ids"], pred
            counts["unchanged"] += 1
        elif dev.get("sqlstate") == "22003":
            assert new == _num_oracle(pred), pred
            counts["q10_22003_now_answers"] += 1
        elif dev["error"] == "DataError" and _mixed_in(pred):
            assert new == _num_oracle(pred), pred
            counts["mixed_in_list_now_answers"] += 1
        else:
            pytest.fail(f"unlisted change against dev: {pred} dev={dev}")
    record_property("e13_changes", json.dumps(counts, sort_keys=True))
    assert counts["q10_22003_now_answers"] > 100, counts
    # Where dev answered, the revised filter answers the same (E13: 0 diffs).
    assert counts["unchanged"] > 10, counts


def test_v_1e400_gt_1_returns_its_row_where_dev_raised_22003(
    corpus: dict[str, Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    pred = c._mcp("t_num", "gt", 1.0)
    assert GOLDEN["answers"][c.predicate_key(pred)]["sqlstate"] == "22003"
    holders = [i for i in range(c.N_NODES) if c.num_text(i, _head_rev(i)) == "1e400"]
    assert holders
    for mode in MODES:
        ids = _ids(c.answer(_read(corpus, monkeypatch, mode, pred)))
        assert set(holders) <= set(ids), mode
        # -1e400 and 1e-400 compare as -inf and 0, as Python's float() reads them.
        assert not {i for i in range(c.N_NODES)
                    if c.num_text(i, _head_rev(i)) in ("-1e400", "1e-400")} & set(ids)


# --------------------------------------------------------------------------- #
# Timestamps                                                                   #
# --------------------------------------------------------------------------- #


def _as_timestamp(value: str) -> Any:
    """The reader's Python reading (server._as_timestamp)."""
    import datetime as dt

    text = value.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        return dt.datetime.fromisoformat(text)
    except ValueError:
        return None


def _q6a_values(dsn: str) -> set[str]:
    """Stored ts strings today's SQL check and the Python re-check accept and
    v4 refuses (design question 6a)."""
    stored = [t for t in c.TS_STORED if isinstance(t, str)]
    with psycopg.connect(dsn, autocommit=True) as conn:
        conn.execute("SET timezone = 'UTC'")
        rows = conn.execute(
            "SELECT v, pg_input_is_valid(v, 'timestamp with time zone'), "
            "okg.attr_ts_v4(v) IS NOT NULL FROM unnest(%s::text[]) AS u(v)",
            (stored,),
        ).fetchall()
    return {v for v, today_sql, v4 in rows if today_sql and not v4 and _as_timestamp(v)}


def test_e14_timestamps_every_change_against_dev_is_listed(
    corpus: dict[str, Any], monkeypatch: pytest.MonkeyPatch,
    record_property: Any,
) -> None:
    q6a = _q6a_values(corpus["dsn"])
    assert {"20240102", " 2024-01-02", "2024-01-02 "} <= q6a, q6a
    accepted = [t for t in c.TS if t in corpora.E14_V4_ADMITS]
    filters = c.ts_filters(accepted)
    counts = {"unchanged": 0, "q6c_refused_bound_now_answers": 0, "q6a_rows": 0,
              "q6d_rows": 0, "unparsable_bound_now_refused": 0}
    for pred in filters:
        dev = GOLDEN["answers"][c.predicate_key(pred)]
        answers = {mode: c.answer(_read(corpus, monkeypatch, mode, pred)) for mode in MODES}
        assert all(a == answers["off"] for a in answers.values()), pred
        if "error" in answers["off"]:
            # Issue #3666 (2c, orchestrator call 2026-10-13): a range bound
            # the read check cannot read as an instant is refused; dev
            # compiled it to FALSE, an empty page marked complete.
            assert c.unreadable_ts_bound(pred), pred
            counts["unparsable_bound_now_refused"] += 1
            continue
        new = _ids(answers["off"])
        bounds = pred.get("values") if pred["op"] in ("in", "between") else [pred["value"]]
        if "error" in dev:
            assert any(b in corpora.PYTHON_ONLY_TS_BOUNDS for b in bounds), (pred, dev)
            counts["q6c_refused_bound_now_answers"] += 1
            continue
        if new == dev["ids"]:
            counts["unchanged"] += 1
            continue
        bound_sub_micro = any(_SUB_MICRO.search(b) for b in bounds)
        for i in set(new) ^ set(dev["ids"]):
            stored = c.node_attrs(i, _head_rev(i))["t_ts"]
            # 6a: no longer matched by instant (lost); exact text equality
            # still matches (gained, where today's instant comparison missed).
            if isinstance(stored, str) and stored in q6a and (
                i in dev["ids"] or (pred["op"] in ("eq", "in") and stored in bounds)
            ):
                counts["q6a_rows"] += 1
            elif bound_sub_micro or (isinstance(stored, str) and _SUB_MICRO.search(stored)):
                counts["q6d_rows"] += 1
            else:
                pytest.fail(f"unlisted change against dev: {pred} node {i} stored={stored!r}")
        if set(new) == set(dev["ids"]):
            pytest.fail(f"order changed against dev: {pred}")
    record_property("e14_changes", json.dumps(counts, sort_keys=True))
    # Issue #3666 (2c): the ranges on a bound the read check cannot read
    # moved from `unchanged` (an empty page on dev and here) to refused.
    assert counts["unchanged"] + counts["unparsable_bound_now_refused"] > 100, counts
    assert counts["unchanged"] > 50 and counts["unparsable_bound_now_refused"] > 0, counts


@pytest.fixture(params=c.NON_UTC_ZONES)
def zoned_corpus(
    request: pytest.FixtureRequest, clean_runtime: str, okg_mcp_can_log_in: str
) -> Iterator[dict[str, Any]]:
    """The corpus published and read with both sessions in a non-UTC zone."""
    dsn, zone = clean_runtime, request.param
    c.set_database_timezone(dsn, zone)
    identity = c.seed(dsn)
    report = dai.reconcile(dsn, deployment=c.DEPLOYMENT, apply=True, enable=True)
    assert report["status"] == "converged", report
    _, head = c.load_corpus(dsn, identity)
    with c.Reader(dsn) as reader:
        c.assert_zone_took_effect(dsn, zone, reader)
        yield {"dsn": dsn, "head": head, "reader": reader, "zone": zone}


def test_e14_non_utc_publisher_and_reader_every_mode_equals_off(
    zoned_corpus: dict[str, Any], monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Every E14 ts filter, publisher and reader in a non-UTC zone: every mode
    returns off's answer (the expression index's key is read in UTC, so its
    probe must stay a superset whatever the zone), and off's answer is the
    one a UTC reader session gives (the exact predicate does not depend on
    the reader's zone)."""
    corpus = zoned_corpus
    accepted = [t for t in c.TS if t in corpora.E14_V4_ADMITS]
    filters = c.ts_filters(accepted)
    assert len(filters) > 200
    off_answers = []
    for pred in filters:
        answers = {mode: c.answer(_read(corpus, monkeypatch, mode, pred)) for mode in MODES}
        assert all(a == answers["off"] for a in answers.values()), (corpus["zone"], pred, answers)
        off_answers.append(answers["off"])
    assert sum(len(a.get("ids", [])) for a in off_answers) > 2_000
    c.set_database_timezone(corpus["dsn"], "UTC")
    with c.Reader(corpus["dsn"]) as utc_reader:
        with utc_reader._mcp._okg_pool.connection() as conn:
            assert c.session_timezone(conn) == "UTC"
        monkeypatch.setenv(afs.SWITCH_ENV, "off")
        for pred, zoned_off in zip(filters, off_answers):
            assert c.answer(utc_reader.filter(corpus["head"], pred)) == zoned_off, pred
