"""Issue #3560: search must not reveal withheld text when hidden matches
exceed a read limit.

A word that only rows the reader may not read hold, and a word nobody
stored, must give the reader the same page facts (results, counts, bounds,
continuation) even when those rows outnumber the read limit. Each test sets
the limit below the number of hidden matches. Synthetic data only.

Covered here: lexical search's ranked read (issue case 4) and the passage
rollup's chunk scan (case 5), under `enforce` (rows denied) and `audit`
(text masked by a redaction rule), with Nomos off unchanged. Grep's
bounds (cases 1-3), the BM25 statistics (case 6) and the node-column
semantic window (case 7) are not covered by this change.
"""
from __future__ import annotations

import functools
import importlib

import psycopg
from psycopg.types.json import Jsonb
import pytest

from okg.substrate.mcp.passage_rollup import PassageRollupPolicyScanExhausted
from tests.substrate.mcp.test_search_grep import (
    MAIN,
    MASKED,
    OPEN,
    READER,
    SECRET,
    _activate,
    _allow,
    _edge,
    _node,
    _publish,
)

pytestmark = pytest.mark.postgres

search_module = importlib.import_module("okg.substrate.mcp.search")

#: More hidden matches than any limit the tests below set.
HIDDEN_ROWS = 12
WITHHELD = "quasar7"
ABSENT = "zzqqxx"

_REDACT_MASKED = {
    "rule_id": "redact_masked", "effect": "redact", "actions": ["graph.read.mcp"],
    "subjects": [READER], "match": {"source_id": MASKED}, "priority": 100,
    "obligations": [{"type": "redaction"}, {"type": "audit"}],
}

#: How the hidden rows are hidden: `enforce` denies their source; `audit`
#: serves a denied row, so there they are hidden by a redaction rule that
#: masks their text.
MODES = {
    "enforce-denied": ("1", SECRET, [_allow(OPEN), _allow(MAIN)]),
    "audit-masked": ("audit", MASKED, [_allow(OPEN), _allow(MAIN), _REDACT_MASKED]),
}


def _mask_text(dsn: str) -> None:
    with psycopg.connect(dsn, autocommit=True) as conn:
        conn.execute(
            "UPDATE okg.subtypes SET metadata = coalesce(metadata, '{}'::jsonb)"
            " || jsonb_build_object('pii_classes', %s::jsonb)"
            " WHERE name IN ('note', 'doc', 'chunk')",
            (Jsonb({"text": "free_text"}),),
        )


def _setup(tmp_path, dsn: str, monkeypatch, mode: str, main: list, groups_for) -> str:
    enforcement, hidden_source, rules = MODES[mode]
    deployment = _publish(tmp_path, dsn, main, groups_for(hidden_source))
    _activate(dsn, deployment, rules)
    if hidden_source == MASKED:
        _mask_text(dsn)
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", enforcement)
    return deployment


def _notes(hidden_source: str) -> dict:
    return {
        OPEN: [
            _node("note:o1", "note", "sanderling flock on the beach"),
            _node("note:o2", "note", "one sanderling"),
        ],
        hidden_source: [
            _node(f"note:h{i:02d}", "note", f"{WITHHELD} {WITHHELD} sanderling hidden {i}")
            for i in range(HIDDEN_ROWS)
        ],
    }


def _lexical(dsn: str, deployment: str, query: str, **kwargs) -> dict:
    from okg.substrate.mcp import open_session, search

    with psycopg.connect(dsn, autocommit=True) as conn:
        session = open_session(conn, deployment_name=deployment, nomos_subject=READER)
        return search(conn, session, query, method="lexical", **kwargs)


_LEXICAL_FACTS = (
    "results", "total_count", "total_count_basis", "truncated", "next_offset",
    "next_offset_reason", "rank_capped", "ranked_cap", "candidate_cap",
    "count_basis_reasons", "scan_cap",
)


def _same_lexical(secret: dict, control: dict) -> None:
    for key in _LEXICAL_FACTS:
        assert secret.get(key) == control.get(key), key


# ---------- case 4: lexical search's ranked read ----------

@pytest.mark.parametrize("mode", sorted(MODES))
def test_lexical_hidden_rows_do_not_fill_the_rank_cap(
    clean_runtime, tmp_path, monkeypatch, mode,
):
    """Higher-scoring hidden rows must not push readable rows off the ranked
    read or set the rank-cap facts."""
    deployment = _setup(tmp_path, clean_runtime, monkeypatch, mode, [], _notes)
    monkeypatch.setattr(search_module, "_LEXICAL_RANK_CAP", 4)
    secret = _lexical(clean_runtime, deployment, f"sanderling {WITHHELD}")
    control = _lexical(clean_runtime, deployment, f"sanderling {ABSENT}")
    assert sorted(row["node_id"] for row in secret["results"]) == ["note:o1", "note:o2"]
    _same_lexical(secret, control)
    assert WITHHELD not in str(secret["results"])


def test_lexical_with_nomos_off_still_ranks_every_row(clean_runtime, tmp_path, monkeypatch):
    """Nomos off: no reader filter joins the ranked read; the cap is spent on
    stored rows as before."""
    deployment = _publish(tmp_path, clean_runtime, [], _notes(SECRET))
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "off")
    monkeypatch.setattr(search_module, "_LEXICAL_RANK_CAP", 4)
    page = _lexical(clean_runtime, deployment, f"sanderling {WITHHELD}")
    ids = [row["node_id"] for row in page["results"]]
    assert len(ids) == 4 and all(i.startswith("note:h") for i in ids)
    assert page["rank_capped"] is True


# ---------- case 5: the passage rollup's chunk scan ----------

def _passage_graph(hidden_source: str) -> tuple[list, dict]:
    main = [_edge("doc:o", "chunk:o:0")]
    groups = {
        OPEN: [
            _node("doc:o", "doc", "Open doc."),
            _node("chunk:o:0", "chunk", "one sanderling passage"),
        ],
        hidden_source: [
            _node(f"chunk:h:{i:02d}", "chunk", f"{WITHHELD} {WITHHELD} hidden passage {i}")
            for i in range(HIDDEN_ROWS)
        ],
    }
    return main, groups


@pytest.mark.parametrize("mode", sorted(MODES))
def test_passage_rollup_hidden_chunks_do_not_fill_the_policy_scan_cap(
    clean_runtime, tmp_path, monkeypatch, mode,
):
    """Before #3560 the hidden chunks filled the scan cap and the withheld
    word was refused (`passage_rollup_policy_scan_exhausted`) where the
    absent word was answered."""
    main = _passage_graph(OPEN)[0]
    deployment = _setup(
        tmp_path, clean_runtime, monkeypatch, mode, main,
        lambda source: _passage_graph(source)[1],
    )
    monkeypatch.setattr(
        search_module, "passage_rollup_candidates",
        functools.partial(
            search_module.passage_rollup_candidates, chunk_cap=1, policy_scan_cap=2,
        ),
    )
    kwargs = {"filter": {"subtype": "doc"}, "rank_by": "passages"}
    try:
        secret = _lexical(clean_runtime, deployment, f"sanderling {WITHHELD}", **kwargs)
    except PassageRollupPolicyScanExhausted as exc:  # the leak, reported plainly
        pytest.fail(f"withheld word refused where the absent word is answered: {exc}")
    control = _lexical(clean_runtime, deployment, f"sanderling {ABSENT}", **kwargs)
    assert [row["node_id"] for row in secret["results"]] == ["doc:o"]
    _same_lexical(secret, control)


# ---------- case 4, filtered records page: the passage read's cap ----------

#: Facts a filtered records page adds to the lexical page facts above.
_FOLD_FACTS = _LEXICAL_FACTS + (
    "ordered_exactly_through", "coverage", "listing", "where_checked_on",
)


@pytest.mark.parametrize("completeness", ["bounded", "exact"])
@pytest.mark.parametrize("mode", sorted(MODES))
def test_filtered_records_hidden_passages_do_not_fill_the_passage_cap(
    clean_runtime, tmp_path, monkeypatch, mode, completeness,
):
    """A records page filtered by subtype folds a capped read of passages.
    Hidden or masked passages of a readable record that outnumber the cap
    must not fill it, so the withheld word and the absent word give the same
    page facts."""
    # The hidden chunks hang under the readable doc, so the passage read
    # reaches them; `_passage_graph`'s hidden chunks have no parent.
    main = _passage_graph(OPEN)[0] + [
        _edge("doc:o", f"chunk:h:{i:02d}") for i in range(HIDDEN_ROWS)
    ]
    deployment = _setup(
        tmp_path, clean_runtime, monkeypatch, mode, main,
        lambda source: _passage_graph(source)[1],
    )
    monkeypatch.setattr(search_module, "_LEXICAL_RANK_CAP", 4)
    kwargs = {"filter": {"subtype": "doc"}, "completeness": completeness}
    secret = _lexical(clean_runtime, deployment, f"sanderling {WITHHELD}", **kwargs)
    control = _lexical(clean_runtime, deployment, f"sanderling {ABSENT}", **kwargs)
    assert [row["node_id"] for row in secret["results"]] == ["doc:o"]
    for key in _FOLD_FACTS:
        assert secret.get(key) == control.get(key), key
    assert WITHHELD not in str(secret["results"])


# ---------- case 4, attribute-filtered page: the effective-attribute read ----------

def _kind_notes(hidden_source: str) -> dict:
    return {
        OPEN: [
            _node("note:o1", "note", "sanderling flock on the beach", kind="shore"),
            _node("note:o2", "note", "one sanderling", kind="shore"),
        ],
        hidden_source: [
            _node(f"note:h{i:02d}", "note", f"{WITHHELD} {WITHHELD} sanderling hidden {i}",
                  kind="shore")
            for i in range(HIDDEN_ROWS)
        ],
    }


@pytest.mark.parametrize("completeness", ["bounded", "exact"])
@pytest.mark.parametrize("mode", sorted(MODES))
def test_attribute_filtered_hidden_rows_do_not_fill_the_effective_read(
    clean_runtime, tmp_path, monkeypatch, mode, completeness,
):
    """`attr_filters` adds a second read, of attribute values, with its own cap.
    Hidden rows that outnumber it must not fill it."""
    deployment = _setup(tmp_path, clean_runtime, monkeypatch, mode, [], _kind_notes)
    monkeypatch.setattr(search_module, "_LEXICAL_RANK_CAP", 4)
    kwargs = {
        "filter": {"subtype": "note", "attr_filters": {"kind": "shore"}},
        "completeness": completeness,
    }
    secret = _lexical(clean_runtime, deployment, f"sanderling {WITHHELD}", **kwargs)
    control = _lexical(clean_runtime, deployment, f"sanderling {ABSENT}", **kwargs)
    assert sorted(row["node_id"] for row in secret["results"]) == ["note:o1", "note:o2"]
    for key in _FOLD_FACTS:
        assert secret.get(key) == control.get(key), key
    assert WITHHELD not in str(secret["results"])


# ---------- the reader's class decisions are cached (PACT reader-view-decision-cache-3560) ----------

def _count_decisions(monkeypatch) -> list[str]:
    """One entry per policy evaluation of the reader's classes. The decision
    cache stands in front of it, so a cache hit adds nothing."""
    from okg.substrate.nomos import reader_view

    calls: list[str] = []
    real = reader_view.decide_classes

    def counting(conn, session, catalog, **kw):
        calls.append(kw["kind"])
        return real(conn, session, catalog, **kw)

    monkeypatch.setattr(reader_view, "decide_classes", counting)
    return calls


def _clause(dsn: str, deployment: str):
    """The SQL clause a search adds for this reader (None: no clause)."""
    from okg.substrate.mcp import open_session

    with psycopg.connect(dsn, autocommit=True) as conn:
        session = open_session(conn, deployment_name=deployment, nomos_subject=READER)
        return search_module._reader_predicate_sql(conn, session)


def _change_policy(monkeypatch, dsn: str, deployment: str, rules: list, enforcement: str) -> None:
    """Activate another policy (as the setup did: with enforcement off), then
    restore the deployment's enforcement."""
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "off")
    _activate(dsn, deployment, rules)
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", enforcement)


@pytest.mark.parametrize("mode", sorted(MODES))
def test_a_cached_decision_still_keeps_hidden_rows_out_of_the_rank_cap(
    clean_runtime, tmp_path, monkeypatch, mode,
):
    """The second search is served the first search's class decisions (no new
    policy evaluation) and still gives the withheld word and the absent word
    the same page facts, with no hidden row returned."""
    deployment = _setup(tmp_path, clean_runtime, monkeypatch, mode, [], _notes)
    monkeypatch.setattr(search_module, "_LEXICAL_RANK_CAP", 4)
    decisions = _count_decisions(monkeypatch)

    first = _lexical(clean_runtime, deployment, f"sanderling {WITHHELD}")
    decided_by_first = len(decisions)
    secret = _lexical(clean_runtime, deployment, f"sanderling {WITHHELD}")
    control = _lexical(clean_runtime, deployment, f"sanderling {ABSENT}")

    assert decided_by_first >= 1
    assert len(decisions) == decided_by_first, "a repeat search evaluated the policy again"
    assert _clause(clean_runtime, deployment) is not None  # this reader is restricted
    for page in (first, secret):
        assert sorted(row["node_id"] for row in page["results"]) == ["note:o1", "note:o2"]
        assert WITHHELD not in str(page["results"])
    _same_lexical(secret, control)
    _same_lexical(secret, first)


def test_a_policy_change_between_searches_is_not_answered_from_the_cache(
    clean_runtime, tmp_path, monkeypatch,
):
    """Policy 1 hides the secret source, policy 2 opens it, policy 3 hides it
    again. Each search must follow the policy active at that moment: a cache
    that kept policy 1's decisions would keep hiding rows the reader may now
    read, or, the other way, spend the cap on rows policy 3 hides."""
    deployment = _setup(tmp_path, clean_runtime, monkeypatch, "enforce-denied", [], _notes)
    monkeypatch.setattr(search_module, "_LEXICAL_RANK_CAP", 4)
    decisions = _count_decisions(monkeypatch)
    query = f"sanderling {WITHHELD}"

    hidden = _lexical(clean_runtime, deployment, query)
    _change_policy(
        monkeypatch, clean_runtime, deployment, [_allow(OPEN), _allow(MAIN), _allow(SECRET)], "1",
    )
    opened = _lexical(clean_runtime, deployment, query)
    _change_policy(monkeypatch, clean_runtime, deployment, [_allow(OPEN), _allow(MAIN)], "1")
    hidden_again = _lexical(clean_runtime, deployment, query)
    control = _lexical(clean_runtime, deployment, f"sanderling {ABSENT}")

    assert sorted(row["node_id"] for row in hidden["results"]) == ["note:o1", "note:o2"]
    opened_ids = [row["node_id"] for row in opened["results"]]
    assert opened_ids and all(i.startswith("note:h") for i in opened_ids), opened_ids
    assert sorted(row["node_id"] for row in hidden_again["results"]) == ["note:o1", "note:o2"]
    _same_lexical(hidden_again, control)
    assert WITHHELD not in str(hidden_again["results"])
    assert len(decisions) == 3  # one decision set per policy, none reused across them


#: A reader who may read every row: no clause, so no row can be dropped by it.
ALL_VISIBLE = {
    # Every source is allowed.
    "enforce-everything-allowed": ("1", [_allow(OPEN), _allow(MAIN), _allow(SECRET)]),
    # The secret source is denied, but `audit` serves a denied row and masks nothing.
    "audit-denial-is-served": ("audit", [_allow(OPEN), _allow(MAIN)]),
}


@pytest.mark.parametrize("name", sorted(ALL_VISIBLE))
def test_a_reader_who_may_read_everything_gets_no_clause_and_the_same_page(
    clean_runtime, tmp_path, monkeypatch, name,
):
    from okg.substrate.nomos import reader_view

    enforcement, rules = ALL_VISIBLE[name]
    deployment = _publish(tmp_path, clean_runtime, [], _notes(SECRET))
    _activate(clean_runtime, deployment, rules)
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", enforcement)
    monkeypatch.setattr(search_module, "_LEXICAL_RANK_CAP", 4)
    query = f"sanderling {WITHHELD}"

    assert _clause(clean_runtime, deployment) is None
    without_clause = _lexical(clean_runtime, deployment, query)

    # The same search with the clause forced in (what the code did before).
    monkeypatch.setattr(
        reader_view.ReaderView, "admits_every_row", property(lambda self: False), raising=False,
    )
    assert _clause(clean_runtime, deployment) is not None
    with_clause = _lexical(clean_runtime, deployment, query)

    assert [r["node_id"] for r in without_clause["results"]] == [
        r["node_id"] for r in with_clause["results"]
    ]
    assert without_clause["results"], "the all-visible reader found nothing: the check is empty"
    assert without_clause["results"] == with_clause["results"]
    _same_lexical(without_clause, with_clause)


# ---------- issue #3899: the planner's row estimate for the ranked read ----------

KEPT_ROWS = 200
HIDDEN_CHUNKS = 200


def _kept_and_hidden(hidden_source: str) -> dict:
    """Readable notes in one source, hidden chunks in another: the hidden
    class holds rows of one subtype only, as on a real deployment."""
    return {
        OPEN: [_node(f"note:k{i:03d}", "note", f"sanderling kept {i}") for i in range(KEPT_ROWS)],
        hidden_source: [
            _node(f"chunk:h{i:03d}", "chunk", f"sanderling hidden {i}")
            for i in range(HIDDEN_CHUNKS)
        ],
    }


def _plan_rows_and_ids(conn, segment: str, where: str) -> tuple[int, list[str]]:
    """The planner's row estimate and the actual node ids for one WHERE."""
    sql = f"SELECT node_id FROM {segment} n WHERE {where}"
    plan = conn.execute("EXPLAIN (FORMAT JSON) " + sql).fetchone()[0][0]["Plan"]
    ids = sorted(r[0] for r in conn.execute(sql).fetchall())
    return plan["Plan Rows"], ids


def test_the_wrapped_reader_clause_keeps_the_rows_the_bare_clause_keeps(
    clean_runtime, tmp_path, monkeypatch,
):
    """The planner-facing form must admit exactly what the clause admits,
    including a clause that is NULL (a NULL in WHERE drops the row)."""
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        outcomes = [
            conn.execute(
                f"SELECT {search_module._planned_as_keeping_all(value)}"
            ).fetchone()[0]
            for value in ("TRUE", "FALSE", "NULL::boolean")
        ]
    assert outcomes == [True, False, False]

    deployment = _setup(
        tmp_path, clean_runtime, monkeypatch, "enforce-denied", [], _kept_and_hidden,
    )
    bare = _clause(clean_runtime, deployment).replace("%%", "%")
    wrapped = search_module._planned_as_keeping_all(bare)
    from okg.substrate.bm25_node_search import resolve_node_text_bm25_index

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        segment, _ = resolve_node_text_bm25_index(conn)
        _, everything = _plan_rows_and_ids(conn, segment, "deleted = FALSE")
        _, kept_bare = _plan_rows_and_ids(conn, segment, bare)
        _, kept_wrapped = _plan_rows_and_ids(conn, segment, wrapped)
    assert kept_wrapped == kept_bare
    assert len(kept_bare) == KEPT_ROWS, "the clause must hide exactly the chunks"
    assert len(everything) == KEPT_ROWS + HIDDEN_CHUNKS


def test_the_ranked_read_is_planned_as_if_the_reader_clause_hid_no_row(
    clean_runtime, tmp_path, monkeypatch,
):
    """Issue #3899. The hidden class holds only chunks, so no note is hidden,
    but PostgreSQL applies the clause's table-wide share (about half) to the
    notes. That halved estimate priced the BM25 index out of the ranked read.
    The wrapped clause must leave the estimate where it is without the clause
    (PostgreSQL's 0.995 default for IS NOT NULL on an expression), while the
    bare clause lowers it: the property that keeps the BM25 index chosen."""
    deployment = _setup(
        tmp_path, clean_runtime, monkeypatch, "enforce-denied", [], _kept_and_hidden,
    )
    bare = _clause(clean_runtime, deployment).replace("%%", "%")
    wrapped = search_module._planned_as_keeping_all(bare)
    from okg.substrate.bm25_node_search import resolve_node_text_bm25_index

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        segment, _ = resolve_node_text_bm25_index(conn)
        conn.execute(f"ANALYZE {segment}")
        base = "deleted = FALSE AND subtype = 'note'"
        plain_rows, _ = _plan_rows_and_ids(conn, segment, base)
        bare_rows, _ = _plan_rows_and_ids(conn, segment, f"{base} AND {bare}")
        wrapped_rows, _ = _plan_rows_and_ids(conn, segment, f"{base} AND {wrapped}")
    assert plain_rows >= KEPT_ROWS * 0.9, plain_rows
    # The failure this guards against: the bare clause cuts the estimate.
    assert bare_rows <= plain_rows * 0.7, (bare_rows, plain_rows)
    assert wrapped_rows >= plain_rows * 0.95, (wrapped_rows, plain_rows)


def test_lexical_search_adds_the_reader_clause_in_its_planner_friendly_form(
    clean_runtime, tmp_path, monkeypatch,
):
    """The ranked read must use the wrapped form (not the bare clause), and
    the page is the same one the bare clause gives."""
    deployment = _setup(
        tmp_path, clean_runtime, monkeypatch, "enforce-denied", [], _kept_and_hidden,
    )
    seen: list[str] = []
    original = search_module._planned_as_keeping_all

    def spy(clause: str) -> str:
        seen.append(clause)
        return original(clause)

    monkeypatch.setattr(search_module, "_planned_as_keeping_all", spy)
    page = _lexical(clean_runtime, deployment, "sanderling")
    assert seen == [_clause(clean_runtime, deployment)]
    assert page["results"]
    assert all(row["node_id"].startswith("note:k") for row in page["results"])
