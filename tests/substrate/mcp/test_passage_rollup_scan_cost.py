"""Passage rollup cost: scan what the answer needs, once per call.

PACT ``passage-rollup-scan-cost`` (issue #3250). PostgreSQL tests on a
published fixture graph of 12 parents and 36 matching passages whose ranks
are fixed by construction: passage ``w`` repeats the query word ``w`` times in
a text of constant length, so it ranks ``37 - w``.

Every comparison is against ``_today``: the rollup as it was before this
change (one ``policy_scan_cap + 1`` read with full rows), kept here as the
oracle and driven with the same arguments.

* scan: Nomos off reads ``chunk_cap + 1`` narrow rows; audit and enforce
  read growing windows and refuse where the oracle refuses; the output, the
  served page and the exact-read disclosures equal the oracle's.
* read-once: a byte-budget rerun runs each rollup read once and serves the
  same page byte for byte, coverage fields included.
* narrow: the served rows equal the full read's; under enforce a redacted
  winning passage keeps its redacted attrs and is never read again.
"""
from __future__ import annotations

import contextlib
import functools
import importlib
import json
from typing import Any, Optional

import psycopg
import pytest
from psycopg.rows import dict_row

from okg.substrate.bm25_node_search import (
    branch_point,
    execute_with_custom_plan,
    resolve_node_text_bm25_index,
)
from okg.substrate.library.sources.base import EdgeFact, NodeFact
from okg.substrate.mcp import open_session, response_bounds
from okg.substrate.mcp import passage_rollup as rollup_module
from okg.substrate.mcp.passage_rollup import (
    PassageRollup,
    PassageRollupPolicyScanExhausted,
    fill_passage_rows,
    passage_rollup_candidates,
    ranked_chunk_sql,
    resolve_edge_segment,
)
from okg.substrate.mcp.readable_text import keep_readable_text
from okg.substrate.mcp.response_bounds import bound_read_tool
from okg.substrate.mcp.result_text import render
from okg.substrate.nomos import activate_policy_bundle, compile_policy_graph
from okg.substrate.nomos.read_filter import (
    filter_edge_rows_for_session,
    filter_node_rows_for_session,
)
from okg.substrate.nomos.read_masking import ReadRedactedAttrs
from okg.substrate.nomos.source_lineage import source_record_ref
from tests.substrate.mcp.erag_rollup_fixture import (
    NARROWINGS,
    ROLLUP_METADATA,
    SUBTYPES,
    TERM,
)
from tests.substrate.mcp.search_fixture import make_search_deployment, publish_nodes

pytestmark = pytest.mark.postgres

# The module, not the package's re-exported `search` function.
search_module = importlib.import_module("okg.substrate.mcp.search")

SOURCE = "system:analyst_search_fixture"
READER = "subject:scan-cost-reader"
CHUNK_CAP = 5
SCAN_CAP = 20
WEIGHTS = tuple(range(1, 37))
LENGTH = 40
#: A second word, in passages w <= 12 only: 12 matches, between
#: CHUNK_CAP + 1 and SCAN_CAP.
NARROW_WORD = "zircon"
ROLLUP = PassageRollup(
    parent_subtype="doc",
    edge_type=str(ROLLUP_METADATA["edge_type"]),
    child_subtype=str(ROLLUP_METADATA["child_subtype"]),
)


def chunk_id(weight: int) -> str:
    return f"chunk:w{weight:02d}"


def doc_id(weight: int) -> str:
    return f"doc:d{(weight - 1) // 3:02d}"


def rank_weight(rank: int) -> int:
    """The passage weight at 1-based rank `rank`."""
    return 37 - rank


def _facts(*, masked: frozenset[int]) -> list[Any]:
    facts: list[Any] = []
    for parent in sorted({doc_id(w) for w in WEIGHTS}):
        facts.append(NodeFact(
            node_id=parent, subtype="doc",
            attrs={"path": f"corpus/{parent.split(':')[1]}-report.txt"},
            source_record_id={"id": parent}, source_revision={"v": 1},
        ))
    for weight in WEIGHTS:
        words = [TERM] * weight + ([NARROW_WORD] if weight <= 12 else [])
        words += ["filler"] * (LENGTH - len(words))
        attrs: dict[str, Any] = {
            "text": " ".join(words) + f" marker{weight}",
            "classification": "masked" if weight in masked else "internal",
            "chunk_ordinal": weight,
        }
        child = chunk_id(weight)
        facts.append(NodeFact(
            node_id=child, subtype="chunk", attrs=attrs,
            source_record_id={"id": child}, source_revision={"v": 1},
        ))
        facts.append(EdgeFact(
            src=doc_id(weight), dst=child, edge_type="contains",
            source_record_id={"id": f"{doc_id(weight)}|contains|{child}"},
            source_revision={"v": 1},
        ))
        facts.append(EdgeFact(
            src=child, dst=doc_id(weight), edge_type="member_of",
            source_record_id={"id": f"{child}|member_of|{doc_id(weight)}"},
            source_revision={"v": 1},
        ))
    return facts


def _publish(
    dsn: str, tmp_path, *, lineage_less: frozenset[int] = frozenset(),
    masked: frozenset[int] = frozenset(),
) -> str:
    """Publish with autovacuum off on the node segment, so the BM25
    statistics hold still between the reads a test compares."""
    with psycopg.connect(dsn, autocommit=True) as conn:
        segment, _index = resolve_node_text_bm25_index(conn)
        conn.execute(f"ALTER TABLE {segment} SET (autovacuum_enabled = off)")
    deployment = make_search_deployment(tmp_path, dsn=dsn)
    facts = _facts(masked=masked)
    publish_nodes(
        dsn, deployment, subtypes=SUBTYPES, narrowings=NARROWINGS,
        nodes=[fact for fact in facts if isinstance(fact, NodeFact)],
        edges=[fact for fact in facts if isinstance(fact, EdgeFact)],
    )
    if lineage_less:
        # The publisher stamps every row's source lineage, so a lineage-less
        # passage is made in the stored row of this disposable database: an
        # envelope with no source ids, which the read filter drops in every
        # mode. The text, and so the BM25 rank, is unchanged.
        with psycopg.connect(dsn, autocommit=True) as conn:
            segment, _index = resolve_node_text_bm25_index(conn)
            updated = conn.execute(
                f"UPDATE {segment} SET attrs = jsonb_set(attrs, "
                "'{nomos_source_lineage}', '{\"status\": \"missing\", \"sources\": []}') "
                "WHERE node_id = ANY(%s) AND deleted = FALSE",
                ([chunk_id(w) for w in lineage_less],),
            ).rowcount
            assert updated >= len(lineage_less)
    return deployment


def _activate(dsn: str, deployment: str, *, deny: frozenset[int] = frozenset()) -> None:
    rules: list[dict[str, Any]] = [
        {
            "rule_id": f"deny_{chunk_id(w).replace(':', '_')}",
            "effect": "deny", "actions": ["graph.read.mcp"], "subjects": [READER],
            "match": {"source_record_ref": source_record_ref(SOURCE, {"id": chunk_id(w)})},
            "priority": 300,
        }
        for w in sorted(deny)
    ]
    rules += [
        {
            "rule_id": "redact_masked", "effect": "redact", "priority": 250,
            "subjects": [READER], "actions": ["graph.read.mcp"],
            "match": {"classification": "masked"},
            "obligations": [{"type": "redaction"}, {"type": "audit"}],
        },
        {
            "rule_id": "allow_fixture_source", "effect": "allow",
            "actions": ["graph.read.mcp"], "subjects": [READER],
            "match": {"source_id": SOURCE}, "priority": 100,
        },
    ]
    bundle = compile_policy_graph({
        "policy_bundle_id": "nomos:test:passage-rollup-scan-cost",
        "policy_version": "v1", "default_effect": "deny", "rules": rules,
    })
    with psycopg.connect(dsn) as conn:
        activate_policy_bundle(
            conn, deployment_name=deployment, bundle=bundle,
            scope=f"deployment:{deployment}", activated_by="test:operator",
        )
        conn.commit()


def _today(
    conn: psycopg.Connection, *, session: Any, rollup: PassageRollup,
    node_segment: str, edge_segment: str, order_sql: str, params: dict[str, Any],
    aggregation: str = "max", chunk_cap: int = rollup_module.PASSAGE_ROLLUP_CHUNK_CAP,
    policy_scan_cap: int = rollup_module.PASSAGE_ROLLUP_POLICY_SCAN_CAP,
    relation_cap: int = rollup_module.PASSAGE_ROLLUP_RELATION_CAP,
    extra_child_where: tuple[str, ...] = (), complete_read: Any = None,
    record_read: Any = None, runtime_required: Any = None, read_key: Any = None,
    text_redaction: bool = False,
) -> tuple[list[dict[str, Any]], int, bool]:
    """The rollup before this change: one policy_scan_cap + 1 full read.

    Takes the new keyword arguments so it can stand in for the function
    under `search`; it ignores `runtime_required` (each filter resolves the
    mode, as before) and `read_key` (no cache). Only `max` is implemented.
    Like the rollup, it keeps a chunk only when its text reads as stored
    under a redacting policy (`text_redaction`, #3377).
    """
    assert aggregation == "max"
    scored = {
        **params, "rollup_child_subtype": rollup.child_subtype,
        "rollup_edge_type": rollup.edge_type, "rollup_raw_limit": policy_scan_cap + 1,
    }
    sql = ranked_chunk_sql(
        node_segment=node_segment, order_sql=order_sql,
        extra_child_where=extra_child_where,
    )
    with conn.cursor(row_factory=dict_row) as cur:
        execute_with_custom_plan(cur, sql, scored)
        raw = cur.fetchall()
    count = None
    if complete_read is not None:
        raw, count = complete_read(
            raw, where=["live_lo <= %(bp)s", "%(bp)s < live_hi",
                        "subtype = %(rollup_child_subtype)s", *extra_child_where],
            limit=policy_scan_cap + 1, params=scored,
        )
    if record_read is not None:
        record_read(raw, limit=policy_scan_cap + 1, count=count)
    reached = len(raw) > policy_scan_cap
    readable = keep_readable_text(
        raw[:policy_scan_cap],
        filter_node_rows_for_session(conn, session, raw[:policy_scan_cap]),
        redaction=text_redaction,
    )
    if reached and len(readable) <= chunk_cap:
        raise PassageRollupPolicyScanExhausted("oracle refusal")
    capped = len(readable) > chunk_cap
    readable = readable[:chunk_cap]
    if not readable:
        return [], 0, capped
    by_id = {str(row["node_id"]): row for row in readable}
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            f"SELECT edge_id, src, dst, edge_type, attrs, source, source_record_id, "
            f"source_revision FROM {edge_segment} WHERE dst = ANY(%(ids)s) "
            f"AND edge_type = %(et)s AND deleted = FALSE "
            f"AND live_lo <= %(bp)s AND %(bp)s < live_hi ORDER BY edge_id LIMIT %(lim)s",
            {"ids": sorted(by_id), "et": rollup.edge_type, "bp": params["bp"],
             "lim": relation_cap + 1},
        )
        edges = cur.fetchall()
    assert len(edges) <= relation_cap
    scores: dict[str, list[tuple[Any, str]]] = {}
    for edge in filter_edge_rows_for_session(conn, session, edges):
        child = by_id.get(str(edge["dst"]))
        if child is not None:
            scores.setdefault(str(edge["src"]), []).append((child["score"], str(edge["dst"])))
    if not scores:
        return [], len(readable), capped
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            f"SELECT node_id, subtype, attrs, text_hash, source, source_record_id, "
            f"source_revision FROM {node_segment} WHERE node_id = ANY(%(ids)s) "
            f"AND subtype = %(st)s AND deleted = FALSE "
            f"AND live_lo <= %(bp)s AND %(bp)s < live_hi ORDER BY node_id",
            {"ids": sorted(scores), "st": rollup.parent_subtype, "bp": params["bp"]},
        )
        parents = cur.fetchall()
    rows = []
    for parent in parents:
        contributions = sorted(scores[str(parent["node_id"])])
        rows.append({
            **dict(parent), "score": contributions[0][0],
            "rollup_chunk_count": len(contributions),
            "rollup_best_chunk_id": contributions[0][1],
            "rollup_best_chunk": by_id[contributions[0][1]],
        })
    rows.sort(key=lambda row: (float(row["score"]), str(row["node_id"])))
    return rows, len(readable), capped


class _Reads:
    """Every statement the rollup module runs, by kind, with its rows."""

    def __init__(self, monkeypatch: pytest.MonkeyPatch) -> None:
        self.statements: list[tuple[str, dict[str, Any], list[dict[str, Any]]]] = []
        #: Each chunk scan's inner row count, before the match test.
        self.inner_rows: list[int] = []
        original = rollup_module._fetch

        def spy(conn: Any, sql: str, params: Any, *, custom_plan: bool = False) -> Any:
            rows = original(conn, sql, params, custom_plan=custom_plan)
            if "rollup_raw_limit" in params:
                kind = "chunks"
            elif "rollup_child_ids" in params:
                kind = "edges"
            elif "rollup_parent_ids" in params:
                kind = "parents"
            elif "rollup_chunk_ids" in params:
                kind = "chunk_rows"
            else:
                kind = "other"
            if kind == "chunks":
                self.inner_rows.append(
                    int(rows[0]["rollup_inner_rows"]) if rows else 0
                )
                # The matched rows as the rollup keeps them.
                kept = [
                    {k: v for k, v in row.items() if k != "rollup_inner_rows"}
                    for row in rows if row["node_id"] is not None
                ]
            else:
                kept = [dict(row) for row in rows]
            self.statements.append((kind, dict(params), kept))
            return rows

        monkeypatch.setattr(rollup_module, "_fetch", spy)

    def of(self, kind: str) -> list[tuple[dict[str, Any], list[dict[str, Any]]]]:
        return [(params, rows) for k, params, rows in self.statements if k == kind]

    def limits(self) -> list[int]:
        return [int(params["rollup_raw_limit"]) for params, _rows in self.of("chunks")]

    def reset(self) -> None:
        self.statements = []
        self.inner_rows = []


def _order(conn: psycopg.Connection, query: str) -> tuple[str, str]:
    segment, index_name = resolve_node_text_bm25_index(conn)
    literal = index_name.replace("'", "''")
    return segment, f"(attrs->>'text') <@> to_bm25query(%(q)s, '{literal}')"


def _rollup(
    conn: psycopg.Connection, session: Any, impl: Any, *, query: str = TERM, **kwargs: Any,
) -> tuple[list[dict[str, Any]], int, bool]:
    segment, order_sql = _order(conn, query)
    bp = branch_point(conn, session)
    rows, scanned, capped = impl(
        conn, session=session, rollup=ROLLUP, node_segment=segment,
        edge_segment=resolve_edge_segment(conn), order_sql=order_sql,
        params={"q": query, "bp": bp}, chunk_cap=CHUNK_CAP,
        policy_scan_cap=SCAN_CAP, **kwargs,
    )
    fill_passage_rows(
        conn, [row["rollup_best_chunk"] for row in rows], node_segment=segment,
        child_subtype=ROLLUP.child_subtype, branch_point_value=bp,
    )
    return rows, scanned, capped


def _plain(value: Any) -> Any:
    """A JSON-comparable form that keeps a redacted-attrs type visible."""
    if isinstance(value, ReadRedactedAttrs):
        return {"__redacted__": _plain(dict(value))}
    if isinstance(value, dict):
        return {str(k): _plain(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_plain(v) for v in value]
    return value if isinstance(value, (str, int, float, bool, type(None))) else repr(value)


def _same(got: tuple, want: tuple) -> None:
    """Field by field: parents, scores, order, counts, best chunk (whole row,
    key order included), chunks scanned and the cap flag."""
    rows, scanned, capped = got
    want_rows, want_scanned, want_capped = want
    assert (scanned, capped) == (want_scanned, want_capped)
    assert [row["node_id"] for row in rows] == [row["node_id"] for row in want_rows]
    for row, expected in zip(rows, want_rows):
        assert list(row) == list(expected)
        assert list(row["rollup_best_chunk"]) == list(expected["rollup_best_chunk"])
        assert json.dumps(_plain(row)) == json.dumps(_plain(expected))


def _session(conn: psycopg.Connection, deployment: str) -> Any:
    return open_session(conn, deployment_name=deployment, nomos_subject=READER)


# ---------- scan what is needed ----------

def test_nomos_off_reads_chunk_cap_plus_one_narrow_rows(clean_runtime, tmp_path, monkeypatch):
    deployment = _publish(clean_runtime, tmp_path)
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "off")
    reads = _Reads(monkeypatch)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        session = _session(conn, deployment)
        got = _rollup(conn, session, passage_rollup_candidates, runtime_required=False)
        statements = list(reads.statements)
        want = _rollup(conn, session, _today)

    # One chunk scan of chunk_cap + 1 rows, carrying only id and score.
    chunk_reads = [(p, r) for k, p, r in statements if k == "chunks"]
    assert [int(p["rollup_raw_limit"]) for p, _ in chunk_reads] == [CHUNK_CAP + 1]
    assert len(chunk_reads[0][1]) == CHUNK_CAP + 1
    assert {key for row in chunk_reads[0][1] for key in row} == {"node_id", "score"}
    # The winning chunks' rows are read once, afterwards, for those chunks only.
    fills = [p for k, p, _r in statements if k == "chunk_rows"]
    assert len(fills) == 1
    assert sorted(fills[0]["rollup_chunk_ids"]) == sorted(
        {row["rollup_best_chunk_id"] for row in got[0]}
    )
    assert got[1] == CHUNK_CAP and got[2] is True
    assert [row["rollup_best_chunk_id"] for row in got[0]][:2] == [
        chunk_id(rank_weight(1)), chunk_id(rank_weight(4)),
    ]
    _same(got, want)


def test_an_unstated_mode_keeps_full_rows(clean_runtime, tmp_path, monkeypatch):
    """Failure case for narrowing: a caller that does not state the Nomos
    mode never gets attr-less rows it might hand to a policy filter."""
    deployment = _publish(clean_runtime, tmp_path)
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "off")
    reads = _Reads(monkeypatch)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        session = _session(conn, deployment)
        got = _rollup(conn, session, passage_rollup_candidates)
        want = _rollup(conn, session, _today)
    (_params, rows), = reads.of("chunks")
    assert all("attrs" in row for row in rows)
    assert reads.of("chunk_rows") == []
    _same(got, want)


@pytest.mark.parametrize(
    ("deny_ranks", "limits", "refused"),
    [
        # Ranks 1-3 hidden: the first window holds 3 readable chunks, so a
        # second window (from rank 1, no OFFSET) is read and holds 9.
        ((1, 2, 3), [CHUNK_CAP + 1, 2 * (CHUNK_CAP + 1)], False),
        # 17 of the first 20 hidden: no window up to the bound holds
        # chunk_cap + 1 readable chunks, and the matches go on past it.
        (tuple(r for r in range(1, 21) if r not in (4, 9, 15)),
         [6, 12, SCAN_CAP + 1], True),
    ],
)
def test_enforce_reads_growing_windows_and_refuses_where_today_refuses(
    clean_runtime, tmp_path, monkeypatch, deny_ranks, limits, refused,
):
    deployment = _publish(clean_runtime, tmp_path)
    _activate(clean_runtime, deployment, deny=frozenset(rank_weight(r) for r in deny_ranks))
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "enforce")
    reads = _Reads(monkeypatch)
    records: list[tuple[int, int]] = []

    def record(rows: list, *, limit: int, count: Any) -> None:
        records.append((len(rows), limit))

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        session = _session(conn, deployment)
        if refused:
            with pytest.raises(PassageRollupPolicyScanExhausted):
                _rollup(conn, session, passage_rollup_candidates,
                        runtime_required=True, record_read=record)
            assert reads.limits() == limits
            with pytest.raises(PassageRollupPolicyScanExhausted):
                _rollup(conn, session, _today)
            return
        got = _rollup(conn, session, passage_rollup_candidates,
                      runtime_required=True, record_read=record)
        want = _rollup(conn, session, _today)
    assert reads.limits() == limits
    # Only the final window is recorded; the first never sets saturation.
    assert records == [(limits[-1], limits[-1])]
    assert reads.of("chunk_rows") == []
    hidden = {chunk_id(rank_weight(r)) for r in deny_ranks}
    assert not hidden & {row["rollup_best_chunk_id"] for row in got[0]}
    _same(got, want)


@pytest.mark.parametrize(
    ("lineage_less_ranks", "limits"),
    [
        # Past position chunk_cap + 1: the first window already decides.
        ((8,), [CHUNK_CAP + 1]),
        # Inside the first window: it holds only 4 readable chunks.
        ((1, 2), [CHUNK_CAP + 1, 2 * (CHUNK_CAP + 1)]),
    ],
)
def test_audit_drops_lineage_less_chunks_and_matches_today(
    clean_runtime, tmp_path, monkeypatch, lineage_less_ranks, limits,
):
    weights = frozenset(rank_weight(r) for r in lineage_less_ranks)
    deployment = _publish(clean_runtime, tmp_path, lineage_less=weights)
    _activate(clean_runtime, deployment)
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "audit")
    reads = _Reads(monkeypatch)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        session = _session(conn, deployment)
        got = _rollup(conn, session, passage_rollup_candidates, runtime_required=True)
        limits_read = reads.limits()
        want = _rollup(conn, session, _today)
        # Non-vacuous: audit really drops each lineage-less chunk.
        segment, _score_sql = _order(conn, TERM)
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                f"SELECT node_id, subtype, attrs, text_hash, source, source_record_id, "
                f"source_revision FROM {segment} WHERE node_id = ANY(%s) AND deleted = FALSE",
                ([chunk_id(w) for w in weights],),
            )
            stored = cur.fetchall()
        assert len(stored) == len(weights)
        assert filter_node_rows_for_session(conn, session, stored) == []
    assert limits_read == limits
    assert not {chunk_id(w) for w in weights} & {row["rollup_best_chunk_id"] for row in got[0]}
    _same(got, want)


# ---------- the served page, against the oracle ----------

def _page(dsn: str, deployment: str, query: str, **kwargs: Any) -> dict[str, Any]:
    with psycopg.connect(dsn, autocommit=True) as conn:
        session = _session(conn, deployment)
        return search_module.search(
            conn, session, query, method="lexical", filter={"subtype": "doc"},
            rank_by="passages", limit=20, **kwargs,
        )


def _served(payload: dict[str, Any]) -> tuple[str, str]:
    return render(payload, tool="search"), json.dumps(payload, sort_keys=True, default=str)


def _with_caps(impl: Any) -> Any:
    return functools.partial(impl, chunk_cap=CHUNK_CAP, policy_scan_cap=SCAN_CAP)


@pytest.mark.parametrize(
    ("mode", "completeness", "query"),
    [
        ("off", "bounded", TERM),
        ("audit", "bounded", TERM),
        ("enforce", "bounded", TERM),
        # Between chunk_cap + 1 and policy_scan_cap matches: the bounded
        # page, and the exact read whose disclosures must be today's.
        ("off", "bounded", NARROW_WORD),
        ("enforce", "bounded", NARROW_WORD),
        ("off", "exact", NARROW_WORD),
        ("enforce", "exact", NARROW_WORD),
    ],
)
def test_the_served_page_is_todays_page(
    clean_runtime, tmp_path, monkeypatch, mode, completeness, query,
):
    deployment = _publish(clean_runtime, tmp_path, masked=frozenset({rank_weight(1)}))
    if mode != "off":
        _activate(clean_runtime, deployment, deny=frozenset({rank_weight(2)}))
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", mode)
    monkeypatch.setattr(search_module, "passage_rollup_candidates", _with_caps(_today))
    want = _page(clean_runtime, deployment, query, completeness=completeness)
    monkeypatch.setattr(
        search_module, "passage_rollup_candidates", _with_caps(passage_rollup_candidates),
    )
    reads = _Reads(monkeypatch)
    got = _page(clean_runtime, deployment, query, completeness=completeness)

    assert got["results"], got
    if completeness == "exact" and mode == "off":
        assert got["index_scan_cap_reads"] == ["chunks"]
        assert "chunks" in got["lexical_exact_reads"]
        assert reads.limits() == [SCAN_CAP + 1]
    if mode == "off" and completeness == "bounded":
        assert reads.limits() == [CHUNK_CAP + 1]
        assert got["rollup_chunk_cap_reached"] is True
    for key in ("coverage", "total_count_basis", "rollup_chunks_scanned",
                "rollup_chunk_cap_reached", "index_scan_cap_reads", "lexical_exact_reads"):
        assert got.get(key) == want.get(key), key
    assert _served(got) == _served(want)


def test_enforce_keeps_the_redacted_winning_passage(clean_runtime, tmp_path, monkeypatch):
    """The winning chunk keeps its post-filter, redacted attrs; nothing reads
    it again after filtering."""
    top = rank_weight(1)
    deployment = _publish(clean_runtime, tmp_path, masked=frozenset({top}))
    _activate(clean_runtime, deployment)
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "enforce")
    reads = _Reads(monkeypatch)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        session = _session(conn, deployment)
        rows, _scanned, _capped = _rollup(
            conn, session, passage_rollup_candidates, runtime_required=True,
        )
    best = {row["node_id"]: row["rollup_best_chunk"] for row in rows}[doc_id(top)]
    assert best["node_id"] == chunk_id(top)
    assert isinstance(best["attrs"], ReadRedactedAttrs)
    assert reads.of("chunk_rows") == []
    # As search calls it (#3377): the masked chunk's text is not what the
    # reader reads, so it wins nothing.
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        session = _session(conn, deployment)
        rows, _scanned, _capped = _rollup(
            conn, session, passage_rollup_candidates, runtime_required=True,
            text_redaction=True,
        )
    assert rows and chunk_id(top) not in {row["rollup_best_chunk_id"] for row in rows}


# ---------- read once per call ----------

def _tool(dsn: str, deployment: str, runs: list[int], *, completeness: str):
    def search_tool(
        query: str, subtype_filter: Optional[str] = None, limit: int = 50,
        offset: int = 0, generation_id: Optional[str] = None,
        max_bytes: Optional[int] = None,
    ) -> dict[str, Any]:
        runs.append(limit)
        with psycopg.connect(dsn, autocommit=True) as conn:
            session = open_session(
                conn, deployment_name=deployment, requested_generation=generation_id,
            )
            return search_module.search(
                conn, session, query, method="lexical", limit=limit, offset=offset,
                filter={"subtype": subtype_filter}, rank_by="passages",
                completeness=completeness,
            )

    return bound_read_tool("search", search_tool)


@pytest.mark.parametrize("completeness", ["bounded", "exact"])
def test_a_rerun_call_reads_the_rollup_once_and_serves_the_same_page(
    clean_runtime, tmp_path, monkeypatch, completeness,
):
    deployment = _publish(clean_runtime, tmp_path)
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "off")
    arguments = {"query": TERM, "subtype_filter": "doc", "limit": 12, "max_bytes": 4096}
    reads = _Reads(monkeypatch)

    # Before: no call cache, so every rerun reads again.
    runs_before: list[int] = []
    with monkeypatch.context() as patch:
        patch.setattr(response_bounds, "call_read_cache", contextlib.nullcontext)
        before = _tool(clean_runtime, deployment, runs_before, completeness=completeness)(
            **arguments
        )
    chunks_before = len(reads.of("chunks"))
    reads.reset()
    runs_after: list[int] = []
    after = _tool(clean_runtime, deployment, runs_after, completeness=completeness)(
        **arguments
    )

    # read-passage-rows-3666 (D4): a search page over its budget keeps every
    # row and shortens its passages, so the wrapper no longer reruns this
    # page at a smaller limit (before: it did, the case the cache exists
    # for; filter, map and expand still rerun). One run each way ...
    assert runs_after == runs_before == [12]
    assert after.get("status") != "refused" and after["results"]
    # ... every run scanned without the cache, once with it: one chunk scan,
    # one edge read and one parent read for the one parameter set each.
    assert chunks_before == len(runs_before)
    assert len(reads.of("chunks")) == 1
    assert len(reads.of("edges")) == 1 and len(reads.of("parents")) == 1
    # A winning-chunk read per distinct page at most.
    fills = [tuple(p["rollup_chunk_ids"]) for p, _rows in reads.of("chunk_rows")]
    assert len(fills) == len(set(fills)) <= len(runs_after)
    # The rerun page keeps its coverage fields.
    assert after["coverage"] == before["coverage"] and after["coverage"]["retrieval"]
    assert after["rollup_chunks_scanned"] == before["rollup_chunks_scanned"] == 36
    if completeness == "exact":
        assert after["index_scan_cap_reads"] == ["chunks"]
    # Same page, byte for byte, in both served forms.
    assert _served(after) == _served(before)


def test_without_a_call_cache_every_rollup_reads(clean_runtime, tmp_path, monkeypatch):
    """Failure case: two direct calls (no wrapper, no cache) never share a
    read, so a stale cache cannot leak between calls."""
    deployment = _publish(clean_runtime, tmp_path)
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "off")
    reads = _Reads(monkeypatch)
    for limit in (10, 5):
        with psycopg.connect(clean_runtime, autocommit=True) as conn:
            search_module.search(
                conn, open_session(conn, deployment_name=deployment), TERM,
                method="lexical", filter={"subtype": "doc"}, rank_by="passages",
                limit=limit,
            )
    assert len(reads.of("chunks")) == 2


# ---------- a short window is not exhaustion (issue #3232) ----------

#: Only w01 scores; w30..w36 hold a query word but score 0, the #3232
#: stale-statistics case, and sort among the 0-score pads by node_id.
ZERO_SCORE_QUERY = "marker1 " + " ".join(f"marker{w}" for w in range(30, 37))
ZERO_SCORE_ORDER = "(CASE WHEN node_id = 'chunk:w01' THEN -1.0 ELSE 0.0 END)::float8"


@pytest.mark.parametrize("mode", ["off", "audit", "enforce"])
def test_zero_score_matches_past_a_padded_window_are_still_read(
    clean_runtime, tmp_path, monkeypatch, mode,
):
    """Pads fill the first windows' LIMIT slots, so their matched rows come
    back short while 0-score matches lie further on. Only an inner read
    shorter than its LIMIT ends the scan; the result is today's.

    Found by adversarial review: ending at a short matched result returned
    one chunk where today's read returns five and the cap flag."""
    deployment = _publish(clean_runtime, tmp_path)
    if mode != "off":
        _activate(clean_runtime, deployment)
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", mode)
    scan_cap = 40
    reads = _Reads(monkeypatch)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        session = _session(conn, deployment)
        segment, _score_sql = _order(conn, ZERO_SCORE_QUERY)
        bp = branch_point(conn, session)
        common = dict(
            session=session, rollup=ROLLUP, node_segment=segment,
            edge_segment=resolve_edge_segment(conn), order_sql=ZERO_SCORE_ORDER,
            params={"q": ZERO_SCORE_QUERY, "bp": bp}, chunk_cap=CHUNK_CAP,
            policy_scan_cap=scan_cap,
        )
        got = passage_rollup_candidates(conn, runtime_required=mode != "off", **common)
        limits, inner = reads.limits(), list(reads.inner_rows)
        fill_passage_rows(
            conn, [row["rollup_best_chunk"] for row in got[0]], node_segment=segment,
            child_subtype=ROLLUP.child_subtype, branch_point_value=bp,
        )
        want = _today(conn, **common)

    # The first window's inner read is full but holds pads, so the next
    # read is the 41-row bound, which reads all 36 and finds w30..w36.
    assert limits == [CHUNK_CAP + 1, scan_cap + 1]
    assert inner == [CHUNK_CAP + 1, 36]
    assert (got[1], got[2]) == (CHUNK_CAP, True)
    assert got[0][0]["rollup_best_chunk_id"] == chunk_id(1)
    assert chunk_id(30) in {row["rollup_best_chunk_id"] for row in got[0]}
    _same(got, want)


@pytest.mark.parametrize("mode", ["off", "enforce"])
def test_a_low_match_query_jumps_to_the_bound_after_one_padded_window(
    clean_runtime, tmp_path, monkeypatch, mode,
):
    """Three matches, fewer than chunk_cap: the first window holds pads, so
    the scan reads the policy bound next instead of doubling through padded
    windows (2,001 + 8,001 rows at the real caps, never more than today's
    read plus one window)."""
    deployment = _publish(clean_runtime, tmp_path)
    if mode != "off":
        _activate(clean_runtime, deployment)
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", mode)
    query = "marker1 marker2 marker3"
    reads = _Reads(monkeypatch)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        session = _session(conn, deployment)
        got = _rollup(
            conn, session, passage_rollup_candidates, query=query,
            runtime_required=mode != "off",
        )
        limits, inner = reads.limits(), list(reads.inner_rows)
        want = _rollup(conn, session, _today, query=query)
    assert limits == [CHUNK_CAP + 1, SCAN_CAP + 1]
    assert inner == [CHUNK_CAP + 1, SCAN_CAP + 1]
    assert (got[1], got[2]) == (3, False)
    _same(got, want)
