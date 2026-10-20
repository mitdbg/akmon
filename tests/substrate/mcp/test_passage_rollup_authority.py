"""Authority-boundary proof for passage-ranked parent reads.

PACT ``recover-passage-rollup-reads``, requirement
``req.recover-passage-rollup.authority``.
"""

from __future__ import annotations

import inspect
from dataclasses import replace
from types import SimpleNamespace

import psycopg
import pytest

from okg.substrate.bm25_node_search import (
    branch_point,
    resolve_node_text_bm25_index,
)
from okg.substrate.library.sources.base import EdgeFact, NodeFact
from okg.substrate.mcp.passage_rollup import (
    PASSAGE_ROLLUP_AGGREGATION,
    PassageRollup,
    PassageRollupPolicyScanExhausted,
    PassageRollupRelationBudgetExceeded,
    passage_rollup_candidates,
    resolve_edge_segment,
)
from okg.substrate.mcp.search import search
from okg.substrate.mcp.session import open_session
from okg.substrate.nomos import activate_policy_bundle, compile_policy_graph
from okg.substrate.nomos.source_lineage import source_record_ref
from tests.pytest.catalog_fixture_builders import SubtypeSpec
from tests.substrate.mcp.erag_rollup_fixture import (
    CHUNK,
    NARROWINGS,
    ROLLUP_METADATA,
    SUBTYPES,
    TERM,
    build_facts,
)
from tests.substrate.mcp.search_fixture import make_search_deployment, publish_nodes


pytestmark = pytest.mark.postgres

_SOURCE = "system:analyst_search_fixture"
_RESTRICTED = "subject:passage-reader"
_OPERATOR = "subject:passage-operator"
_DENIED_PASSAGE = "doc:sharp:chunk:0"
_DENIED_PARENT = "doc:quiet"
_ROLLUP = PassageRollup(
    parent_subtype="doc",
    edge_type=str(ROLLUP_METADATA["edge_type"]),
    child_subtype=str(ROLLUP_METADATA["child_subtype"]),
)


#: One word per chunk, none of them a query term. Several fixture chunks carry
#: the same body, and a records-ranked lexical search folds rows with identical
#: text into one (#3465); a distinct trailing word keeps every chunk its own
#: row. Every chunk gains exactly one token, so equal-weight chunks still tie.
_CHUNK_MARKERS = (
    "alpha",
    "bravo",
    "charlie",
    "delta",
    "echo",
    "foxtrot",
    "golf",
    "hotel",
    "india",
    "juliett",
    "kilo",
    "lima",
)


def _with_unique_chunk_texts(facts: list) -> list:
    chunks = [f for f in facts if getattr(f, "subtype", None) == CHUNK.name]
    assert len(chunks) <= len(_CHUNK_MARKERS), len(chunks)
    markers = dict(zip((f.node_id for f in chunks), _CHUNK_MARKERS))
    return [
        replace(f, attrs={**f.attrs, "text": f"{f.attrs['text']} {markers[f.node_id]}"})
        if getattr(f, "node_id", None) in markers
        else f
        for f in facts
    ]


def _publish(dsn: str, tmp_path) -> str:
    deployment = make_search_deployment(tmp_path, dsn=dsn)
    facts = _with_unique_chunk_texts(build_facts())
    publish_nodes(
        dsn,
        deployment,
        subtypes=SUBTYPES,
        narrowings=NARROWINGS,
        nodes=[fact for fact in facts if hasattr(fact, "node_id")],
        edges=[fact for fact in facts if not hasattr(fact, "node_id")],
    )
    return deployment


def _activate_read_policy(dsn: str, deployment: str) -> None:
    denied_passage_ref = source_record_ref(
        _SOURCE,
        {"id": _DENIED_PASSAGE},
    )
    denied_parent_ref = source_record_ref(
        _SOURCE,
        {"id": _DENIED_PARENT},
    )
    assert denied_passage_ref is not None
    assert denied_parent_ref is not None
    bundle = compile_policy_graph(
        {
            "policy_bundle_id": "nomos:test:passage-rollup-authority",
            "policy_version": "v1",
            "default_effect": "deny",
            "rules": [
                {
                    "rule_id": "deny_strongest_passage_for_reader",
                    "effect": "deny",
                    "actions": ["graph.read.mcp"],
                    "subjects": [_RESTRICTED],
                    "match": {"source_record_ref": denied_passage_ref},
                    "priority": 300,
                },
                {
                    "rule_id": "deny_one_parent_for_reader",
                    "effect": "deny",
                    "actions": ["graph.read.mcp"],
                    "subjects": [_RESTRICTED],
                    "match": {"source_record_ref": denied_parent_ref},
                    "priority": 300,
                },
                {
                    "rule_id": "allow_fixture_source",
                    "effect": "allow",
                    "actions": ["graph.read.mcp"],
                    "subjects": [_RESTRICTED, _OPERATOR],
                    "match": {"source_id": _SOURCE},
                    "priority": 100,
                },
            ],
        }
    )
    with psycopg.connect(dsn) as conn:
        activate_policy_bundle(
            conn,
            deployment_name=deployment,
            bundle=bundle,
            scope=f"deployment:{deployment}",
            activated_by="test:operator",
        )
        conn.commit()


def _raw_rollup(conn, session) -> dict[str, dict]:
    segment, index_name = resolve_node_text_bm25_index(conn)
    order_sql = (
        f"(attrs->>'text') <@> to_bm25query("
        f"%(q)s, '{index_name.replace(chr(39), chr(39) * 2)}')"
    )
    rows, _, _ = passage_rollup_candidates(
        conn,
        session=session,
        rollup=_ROLLUP,
        node_segment=segment,
        edge_segment=resolve_edge_segment(conn),
        order_sql=order_sql,
        params={"q": TERM, "bp": branch_point(conn, session)},
        aggregation=PASSAGE_ROLLUP_AGGREGATION,
    )
    return {str(row["node_id"]): row for row in rows}


def test_denied_passage_cannot_influence_visible_parent_rollup(
    clean_runtime: str,
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Child policy runs before aggregation; parent policy stays downstream."""
    deployment = _publish(clean_runtime, tmp_path)
    _activate_read_policy(clean_runtime, deployment)
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "1")

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        operator = open_session(
            conn,
            deployment_name=deployment,
            nomos_subject=_OPERATOR,
        )
        restricted = open_session(
            conn,
            deployment_name=deployment,
            nomos_subject=_RESTRICTED,
        )
        operator_rows = _raw_rollup(conn, operator)
        restricted_rows = _raw_rollup(conn, restricted)
        restricted_chunks = search(
            conn,
            restricted,
            TERM,
            method="lexical",
            filter={"subtype": CHUNK.name},
            limit=50,
            select=["node_id", "score"],
        )
        page = search(
            conn,
            restricted,
            TERM,
            method="lexical",
            filter={"subtype": "doc"},
            rank_by="passages",
            limit=50,
            select=["node_id", "score"],
        )

    readable_scores = {
        str(row["node_id"]): float(row["score"]) for row in restricted_chunks["results"]
    }
    assert _DENIED_PASSAGE not in readable_scores
    assert "doc:sharp:chunk:1" in readable_scores

    # The operator's best evidence is the strong passage. For the restricted
    # subject the same visible parent is scored solely by its readable sibling.
    assert operator_rows["doc:sharp"]["rollup_best_chunk_id"] == _DENIED_PASSAGE
    assert restricted_rows["doc:sharp"]["rollup_best_chunk_id"] == ("doc:sharp:chunk:1")
    assert float(restricted_rows["doc:sharp"]["score"]) == pytest.approx(
        readable_scores["doc:sharp:chunk:1"]
    )
    assert restricted_rows["doc:sharp"]["rollup_chunk_count"] == 1

    # The public order is exactly the order recomputed from readable passage
    # scores, and the shared parent filter removes the denied parent.
    expected = sorted(
        (
            float(row["score"]),
            node_id,
        )
        for node_id, row in restricted_rows.items()
        if node_id != _DENIED_PARENT
    )
    assert [row["node_id"] for row in page["results"]] == [
        node_id for _, node_id in expected
    ]
    assert _DENIED_PARENT not in {row["node_id"] for row in page["results"]}
    assert page["rollup_contributing_chunks"]["doc:sharp"] == 1
    assert _DENIED_PARENT not in page["rollup_contributing_chunks"]


def test_core_rollup_keeps_scope_and_session_resolution_outside_it() -> None:
    """The candidate boundary requires pre-resolved session and physical pins."""
    from okg.substrate.mcp import passage_rollup as rollup_module

    parameter_names = set(passage_rollup_candidates.__annotations__)
    assert "session" in parameter_names
    assert "scope" not in parameter_names
    assert "deployment" not in parameter_names
    assert "profile" not in parameter_names
    assert "generation" not in parameter_names
    source = inspect.getsource(rollup_module)
    assert "runtime_enforcement" not in source
    assert "deployment_requires_nomos_runtime" not in source


class _BatchCursor:
    def __init__(self, rows):
        self._rows = rows

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def execute(self, *_args, **_kwargs):
        return None

    def fetchall(self):
        return self._rows


class _BatchConnection:
    def __init__(self, batches):
        self._batches = list(batches)

    def cursor(self, **_kwargs):
        return _BatchCursor(self._batches.pop(0))


def test_bounded_policy_scan_refuses_an_inexact_readable_prefix(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Hidden rows cannot evict later readable rows from a partial page."""
    from okg.substrate.mcp import passage_rollup as rollup_module

    ranked = [
        {"node_id": "chunk:1", "score": -3.0},
        {"node_id": "chunk:2", "score": -2.0},
        {"node_id": "chunk:3", "score": -1.0},
    ]
    # The first window holds chunk_cap + 1 = 2 rows, the second (the bound,
    # policy_scan_cap + 1 = 3) all three; each re-reads from rank 1.
    # Each scan row carries the inner read's row count (before the match
    # test); here no pads, so it equals the matched rows.
    conn = _BatchConnection([
        [{**row, "rollup_inner_rows": 2} for row in ranked[:2]],
        [{**row, "rollup_inner_rows": 3} for row in ranked],
    ])
    monkeypatch.setattr(
        rollup_module,
        "filter_node_rows_for_session",
        lambda _conn, _session, rows, **_kw: [
            row for row in rows if row["node_id"] == "chunk:1"
        ],
    )

    with pytest.raises(PassageRollupPolicyScanExhausted) as exc:
        passage_rollup_candidates(
            conn,
            session=SimpleNamespace(),
            rollup=_ROLLUP,
            node_segment="nodes",
            edge_segment="edges",
            order_sql="score_expression",
            params={"q": TERM, "bp": 1},
            chunk_cap=1,
            policy_scan_cap=2,
        )
    assert exc.value.code == "passage_rollup_policy_scan_exhausted"


def test_relation_fanout_refuses_instead_of_truncating_attribution(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The independently bounded edge read never returns partial parents."""
    from okg.substrate.mcp import passage_rollup as rollup_module

    conn = _BatchConnection(
        [
            [
                {"node_id": "chunk:1", "score": -2.0, "rollup_inner_rows": 2},
                {"node_id": "chunk:2", "score": -1.0, "rollup_inner_rows": 2},
            ],
            [
                {"edge_id": "e1", "src": "doc:1", "dst": "chunk:1"},
                {"edge_id": "e2", "src": "doc:2", "dst": "chunk:2"},
            ],
        ]
    )
    monkeypatch.setattr(
        rollup_module,
        "filter_node_rows_for_session",
        lambda _conn, _session, rows, **_kw: rows,
    )

    with pytest.raises(PassageRollupRelationBudgetExceeded) as exc:
        passage_rollup_candidates(
            conn,
            session=SimpleNamespace(),
            rollup=_ROLLUP,
            node_segment="nodes",
            edge_segment="edges",
            order_sql="score_expression",
            params={"q": TERM, "bp": 1},
            chunk_cap=2,
            policy_scan_cap=2,
            relation_cap=1,
        )
    assert exc.value.code == "passage_rollup_relation_budget_exceeded"


def test_cli_authority_identity_distinguishes_passage_ranking(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Nomos authorizes and audits the actual CLI ranking behavior."""
    import okg.substrate.nomos as nomos
    from okg.substrate.cli import _read_side

    captured = {}
    conn = SimpleNamespace(commit=lambda: None)
    monkeypatch.setattr(
        nomos, "deployment_nomos_enforcement_mode", lambda _d: "enforce",
    )

    def evaluate(_conn, **kwargs):
        captured.update(kwargs)
        return SimpleNamespace(allowed=True)

    monkeypatch.setattr(nomos, "evaluate_runtime_boundary_with_active_policy", evaluate)
    monkeypatch.setattr(_read_side, "_nomos_profile_record", lambda *_a, **_k: {})

    _read_side._authorize_search_with_nomos(
        conn,
        deployment="fixture",
        session=SimpleNamespace(
            generation_id=7,
            branch_name="default",
            catalog_version_id=11,
        ),
        query=TERM,
        method="lexical",
        rank_by="passages",
        limit=10,
        offset=0,
        filter_obj={"subtype": "doc"},
        select=None,
        access_context=None,
        subject="subject:test",
    )

    assert captured["request"].resource["rank_by"] == "passages"
    assert captured["target"]["rank_by"] == "passages"


def test_rollup_reads_work_as_the_reviewed_mcp_database_role(
    clean_runtime: str,
    tmp_path,
) -> None:
    """Every new catalog and Chronos dependency is granted to ``okg_mcp``."""
    deployment = _publish(clean_runtime, tmp_path)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        session = open_session(conn, deployment_name=deployment)
        conn.execute("SET ROLE okg_mcp")
        try:
            page = search(
                conn,
                session,
                TERM,
                method="lexical",
                filter={"subtype": "doc"},
                rank_by="passages",
                limit=10,
                select=["node_id", "score"],
            )
        finally:
            conn.execute("RESET ROLE")

    assert page["results"]
    assert page["rank_by_relation"] == {
        "parent_subtype": "doc",
        "edge_type": "contains",
        "child_subtype": "chunk",
        "source": "catalog:okg_passage_rollup",
    }


def test_rollup_declaration_and_graph_rows_stay_at_the_session_pin(
    clean_runtime: str,
    tmp_path,
) -> None:
    """A later catalog and publish cannot move an already-open session."""
    deployment = _publish(clean_runtime, tmp_path)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        first = open_session(conn, deployment_name=deployment)

    later_term = "zirconium"
    later_chunk = NodeFact(
        node_id="doc:sharp:chunk:later",
        subtype=CHUNK.name,
        attrs={"text": f"The later publication discusses {later_term}."},
        source_record_id={"id": "doc:sharp:chunk:later"},
        source_revision={"v": 2},
    )
    later_edge = EdgeFact(
        src="doc:sharp",
        dst=later_chunk.node_id,
        edge_type="references",
        source_record_id={"id": "doc:sharp|references|later"},
        source_revision={"v": 2},
    )
    publish_nodes(
        clean_runtime,
        deployment,
        subtypes=[
            SubtypeSpec(
                "doc",
                metadata={
                    "searchable_text_fields": ["path"],
                    "searchable_text_path_fields": ["path"],
                    "passage_rollup": {
                        "edge_type": "references",
                        "child_subtype": CHUNK.name,
                    },
                },
            ),
            CHUNK,
        ],
        narrowings=[("doc", "references", CHUNK.name)],
        nodes=[later_chunk],
        edges=[later_edge],
    )

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        latest = open_session(conn, deployment_name=deployment)
        first_existing = search(
            conn,
            first,
            TERM,
            method="lexical", completeness="exact",
            filter={"subtype": "doc"},
            rank_by="passages",
            limit=10,
        )
        first_later = search(
            conn,
            first,
            later_term,
            method="lexical", completeness="exact",
            filter={"subtype": "doc"},
            rank_by="passages",
            limit=10,
        )
        latest_later = search(
            conn,
            latest,
            later_term,
            method="lexical", completeness="exact",
            filter={"subtype": "doc"},
            rank_by="passages",
            limit=10,
        )

    assert first.generation_id != latest.generation_id
    assert first.catalog_version_id != latest.catalog_version_id
    assert first_existing["rank_by_relation"]["edge_type"] == "contains"
    assert first_later["rank_by_relation"]["edge_type"] == "contains"
    assert first_later["total_count"] == 0
    assert latest_later["rank_by_relation"]["edge_type"] == "references"
    assert [row["node_id"] for row in latest_later["results"]] == ["doc:sharp"]
