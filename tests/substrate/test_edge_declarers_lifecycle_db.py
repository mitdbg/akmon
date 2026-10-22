"""PostgreSQL proof that ranked-edge declarations stay consistent through
merges, merge-back rollbacks, ranking changes, dangling cleanup, compensation
and source retirement (item 92a, review of 46385d977a).

The core publish proof is tests/substrate/test_edge_declarers_db.py; these
cases reuse its fixtures.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from tests.substrate.test_edge_declarers_db import (
    CALLEE,
    CALLER,
    DEPLOYMENT,
    ENRICHER,
    RANKING,
    STRUCTURE,
    _apply_catalog,
    _edge,
    _facts,
    _node,
    _publish,
    _rank,
    _reference,
    _shared_edges,
    _symbol,
)

pytestmark = pytest.mark.postgres


def _calls(store: Any, branch: str = "default") -> tuple[list, list]:
    """(stored calls rows, declarations) as (source, via) pairs."""
    rows = [
        (row["source"], dict(row["attrs"] or {}).get("via"))
        for row in store.query(
            "SELECT source, attrs FROM okg.graph_edges WHERE edge_type = 'calls'",
            branch=branch,
        )
    ]
    declared = sorted(
        (row["source"], dict(row["attrs"] or {}).get("via"))
        for row in store.query(
            "SELECT source, attrs FROM okg.graph_edge_declarers "
            "WHERE edge_type = 'calls'",
            branch=branch,
        )
    )
    return rows, declared


def _store(dsn: str):
    from okg.substrate.graph_store import ChronosGraphStore

    return ChronosGraphStore.connect(dsn)


def _seed(store: Any) -> None:
    """The enricher wins a->b over code-structure's s1."""
    store.apply_facts(
        [_node("sym:a"), _node("sym:b"), _edge("sym:a", "sym:b", "s1")],
        source=STRUCTURE, strict_endpoints=False,
    )
    store.apply_facts(
        [_edge("sym:a", "sym:b", "e1")], source=ENRICHER, strict_endpoints=False,
    )


def test_a_merge_both_sides_changed_stores_the_winner_of_the_merged_declarations(
    clean_runtime: str,
) -> None:
    """Review blocker 1. The fork withdraws the enricher (row handed to
    code-structure s1); the target rewrites code-structure's declaration to
    s2. A row-wise merge alone would keep the fork's s1 row while the only
    declaration says s2."""
    _rank(clean_runtime, RANKING)
    store = _store(clean_runtime)
    try:
        _seed(store)
        store.create_branch("staging:m1", from_branch="default")
        store.create_branch("experiment:f", from_branch="staging:m1")
        store.apply_facts(
            [_edge("sym:a", "sym:b", "e1", op="R")],
            source=ENRICHER, strict_endpoints=False, branch="experiment:f",
        )
        store.apply_facts(
            [_edge("sym:a", "sym:b", "s2")],
            source=STRUCTURE, strict_endpoints=False, branch="staging:m1",
        )
        assert _calls(store, "experiment:f") == ([(STRUCTURE, "s1")], [(STRUCTURE, "s1")])
        assert _calls(store, "staging:m1") == (
            [(ENRICHER, "e1")], [(ENRICHER, "e1"), (STRUCTURE, "s2")],
        )
        store.merge("experiment:f", "staging:m1")
        assert _calls(store, "staging:m1") == ([(STRUCTURE, "s2")], [(STRUCTURE, "s2")])
    finally:
        store.close()


def test_a_merge_back_rollback_keeps_declarations_written_after_the_generation(
    clean_runtime: str,
) -> None:
    """Review should-fix 2. A declaration-only rewrite (s1 -> s2) is in no
    checkpoint yet. Merge-back reads it before landing and, after rolling
    the target back to the pre-land generation, writes it back; restoring
    alone would return s1 although code-structure has moved past it."""
    _rank(clean_runtime, RANKING)
    store = _store(clean_runtime)
    try:
        _seed(store)
        generation = store.checkpoint_generation(1, deployment_name=DEPLOYMENT)
        store.apply_facts(
            [_edge("sym:a", "sym:b", "s2")], source=STRUCTURE, strict_endpoints=False,
        )
        pending = store.edge_declarations_since_generation(generation)
        assert [(row or {}).get("source") for _key, row in pending] == [STRUCTURE]
        # The landed merge: here the enricher's withdrawal, which the
        # rollback must undo.
        store.apply_facts(
            [_edge("sym:a", "sym:b", "e1", op="R")],
            source=ENRICHER, strict_endpoints=False,
        )
        store.restore_branch_to_generation(generation, deployment_name=DEPLOYMENT)
        assert _calls(store) == (
            [(ENRICHER, "e1")], [(ENRICHER, "e1"), (STRUCTURE, "s1")],
        )
        store.reapply_edge_declarations(pending)
        assert _calls(store) == (
            [(ENRICHER, "e1")], [(ENRICHER, "e1"), (STRUCTURE, "s2")],
        )
    finally:
        store.close()


def test_dropping_a_producer_from_the_ranking_removes_its_declarations(
    clean_runtime: str,
) -> None:
    """Review should-fix 3, probe A: a stale declaration of a producer the
    ranking dropped made every later write of its edge refuse."""
    _rank(clean_runtime, RANKING)
    store = _store(clean_runtime)
    try:
        _seed(store)
    finally:
        store.close()
    _rank(clean_runtime, {"calls": [ENRICHER, "code-lsp-resolution"]})
    store = _store(clean_runtime)
    try:
        assert _calls(store) == ([(ENRICHER, "e1")], [(ENRICHER, "e1")])
        store.apply_facts(
            [_edge("sym:a", "sym:b", "e2")], source=ENRICHER, strict_endpoints=False,
        )
        assert _calls(store) == ([(ENRICHER, "e2")], [(ENRICHER, "e2")])
    finally:
        store.close()


def test_an_unrank_retract_rerank_never_revives_a_retracted_declaration(
    clean_runtime: str,
) -> None:
    """Review should-fix 3, probe B: unranking keeps no declaration that a
    later re-ranking could bring back after its producer retracted the edge."""
    _rank(clean_runtime, RANKING)
    store = _store(clean_runtime)
    try:
        _seed(store)
    finally:
        store.close()
    _rank(clean_runtime, {"references": [ENRICHER]})
    store = _store(clean_runtime)
    try:
        assert _calls(store) == ([(ENRICHER, "e1")], [])
        # Unranked now: the enricher retracts its own row on today's path.
        store.apply_facts(
            [_edge("sym:a", "sym:b", "e1", op="R")],
            source=ENRICHER, strict_endpoints=False,
        )
    finally:
        store.close()
    _rank(clean_runtime, RANKING)
    store = _store(clean_runtime)
    try:
        store.apply_facts(
            [_edge("sym:a", "sym:b", "s3")], source=STRUCTURE, strict_endpoints=False,
        )
        assert _calls(store) == ([(STRUCTURE, "s3")], [(STRUCTURE, "s3")])
    finally:
        store.close()


def test_dangling_cleanup_removes_every_declaration_of_the_edge(
    clean_runtime: str,
) -> None:
    """Review should-fix 4: both dangling-edge paths purge declarations, so
    no later write can hand a dangling edge back to another declarer."""
    from okg.substrate.graph_store_internal.common import edge_id

    _rank(clean_runtime, RANKING)
    store = _store(clean_runtime)
    try:
        store.apply_facts([_node("sym:a")], source=STRUCTURE, strict_endpoints=False)
        # Both endpoints of these edges are missing their target node.
        store.apply_facts(
            [_edge("sym:a", "sym:gone", "s1"), _edge("sym:a", "sym:pruned", "s1")],
            source=STRUCTURE, strict_endpoints=False,
        )
        store.apply_facts(
            [
                _edge("sym:a", "sym:gone", "e1"),
                _edge("sym:a", "sym:pruned", "e1"),
            ],
            source=ENRICHER, strict_endpoints=False,
        )
        assert len(_calls(store)[1]) == 4
        assert store.retract_orphaned_derived_edges() == 2
        # The enricher's rows were the orphans; with them every declaration
        # of both edges went.
        assert _calls(store) == ([], [])
        store.apply_facts(
            [_edge("sym:a", "sym:pruned", "s2")],
            source=STRUCTURE, strict_endpoints=False,
        )
        assert store.prune_edges([edge_id("sym:a", "calls", "sym:pruned")]) == 1
        assert _calls(store) == ([], [])
    finally:
        store.close()


def test_a_call_removed_from_the_caller_retracts_the_enricher_declaration(
    clean_runtime: str,
    tmp_path: Path,
) -> None:
    """Review should-fix 4: the call site leaves the caller (its reference
    node is retracted) and the source stops writing the edge. The enricher
    must withdraw its own declaration, or its stale row would stay."""
    from tests.substrate.test_edge_declarers_db import _structure_edges

    catalog_identity = _apply_catalog(clean_runtime, tmp_path)
    _rank(clean_runtime, RANKING)
    first = _publish(
        clean_runtime, catalog_identity, _facts(edges=_structure_edges("v1")),
    )
    assert _shared_edges(clean_runtime, first["generation_id"])[
        (CALLER, "calls", CALLEE)
    ]["source"] == ENRICHER
    gone = _reference()
    gone = type(gone)(
        node_id=gone.node_id,
        subtype=gone.subtype,
        attrs={},
        source_record_id=gone.source_record_id,
        source_revision=gone.source_revision,
        op="R",
    )
    removed = _publish(
        clean_runtime,
        catalog_identity,
        [
            _symbol(CALLER, "tests/test_mw.py", "test_json_write", 88),
            _symbol(CALLEE, "tinydb/database.py", "TinyDB", 20),
            gone,
            *_structure_edges("v1", op="R"),
        ],
    )
    assert (CALLER, "calls", CALLEE) not in _shared_edges(
        clean_runtime, removed["generation_id"],
    )
    store = _store(clean_runtime)
    try:
        assert _calls(store) == ([], [])
    finally:
        store.close()


def test_the_pre_lock_plan_still_retracts_the_enricher_declaration(
    clean_runtime: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Issue 2941 fix 1 with item 92a: when the publisher uses
    code_reference's pre-lock plan, the stale-declaration step still runs,
    under the lock, from the plan's resolution."""
    from tests.substrate.test_edge_declarers_db import _structure_edges

    monkeypatch.delenv("OKG_PRELOCK_ENRICH", raising=False)
    catalog_identity = _apply_catalog(clean_runtime, tmp_path)
    _rank(clean_runtime, RANKING)
    _publish(clean_runtime, catalog_identity, _facts(edges=_structure_edges("v1")))
    gone = _reference()
    gone = type(gone)(
        node_id=gone.node_id,
        subtype=gone.subtype,
        attrs={},
        source_record_id=gone.source_record_id,
        source_revision=gone.source_revision,
        op="R",
    )
    removed = _publish(
        clean_runtime,
        catalog_identity,
        [
            _symbol(CALLER, "tests/test_mw.py", "test_json_write", 88),
            _symbol(CALLEE, "tinydb/database.py", "TinyDB", 20),
            gone,
            *_structure_edges("v1", op="R"),
        ],
    )
    report = (removed.get("timing_details") or {}).get("prelock_enrich") or {}
    entry = (report.get("enrichers") or {}).get("code_reference") or {}
    assert entry.get("planned") is True and entry.get("used") is True, report
    runs = {run["name"]: run for run in removed["enricher_runs"]}
    assert runs["code_reference"]["n_edges_emitted"] == 1, runs
    assert (CALLER, "calls", CALLEE) not in _shared_edges(
        clean_runtime, removed["generation_id"],
    )
    store = _store(clean_runtime)
    try:
        assert _calls(store) == ([], [])
    finally:
        store.close()


def test_a_ranking_added_after_preparing_is_seen_under_the_lock(
    clean_runtime: str,
) -> None:
    """Issue 2941 fix 1 with item 92a: an edge write that matched its stored
    row is dropped while preparing; a ranking added before the lock makes it
    a declaration to record, so the verified path recomputes."""
    store = _store(clean_runtime)
    try:
        store.apply_facts(
            [_node("sym:a"), _node("sym:b")],
            source=STRUCTURE, strict_endpoints=False,
        )
        store.apply_facts(
            [_edge("sym:a", "sym:b", "e1")],
            source=ENRICHER, strict_endpoints=False,
        )
        assert _calls(store) == ([(ENRICHER, "e1")], [])
        facts = [_edge("sym:a", "sym:b", "e1")]
        prepared = store.prepare_apply_facts(
            facts, branch="default", source=ENRICHER, strict_endpoints=False,
        )
        _rank(clean_runtime, RANKING)
        result = store.apply_facts(
            facts, source=ENRICHER, strict_endpoints=False, prepared=prepared,
        )
        assert result.timings_ms.get(
            "prepared.recomputed.edge_ranking_changed"
        ) == 1.0, result.timings_ms
        assert _calls(store) == ([(ENRICHER, "e1")], [(ENRICHER, "e1")])
    finally:
        store.close()


def test_compensating_a_run_without_a_run_base_withdraws_its_declarations(
    clean_runtime: str,
) -> None:
    """Review should-fix 4: the no-run-base compensation retracts the rows
    the failed run owns and withdraws its outranked declarations."""
    from okg.substrate.ingest.compensation import _retract_run_rows

    _rank(clean_runtime, RANKING)
    store = _store(clean_runtime)
    try:
        store.apply_facts(
            [_node("sym:a"), _node("sym:b"), _node("sym:c")],
            source=STRUCTURE, run_id="good-run", strict_endpoints=False,
        )
        store.apply_facts(
            [_edge("sym:a", "sym:b", "e1")],
            source=ENRICHER, run_id="enricher-run", strict_endpoints=False,
        )
        # The failed run declares a->b (outranked) and owns a->c alone.
        store.apply_facts(
            [_edge("sym:a", "sym:b", "s-bad"), _edge("sym:a", "sym:c", "s-bad")],
            source=STRUCTURE, run_id="bad-run", strict_endpoints=False,
        )
        stats = _retract_run_rows(
            store, store.checkout("default"), branch="default",
            source=STRUCTURE, run_id="bad-run", page_rows=100,
        )
        assert stats["edge_declarations_withdrawn"] == 1
        assert _calls(store) == ([(ENRICHER, "e1")], [(ENRICHER, "e1")])
    finally:
        store.close()


def test_retiring_a_source_hands_its_ranked_rows_on(clean_runtime: str) -> None:
    """Review should-fix 4: source retirement withdraws every declaration of
    the retired source before it retracts its rows (the call
    ``source_retirement._apply_plan`` makes), so a row it won passes to the
    next-ranked declarer."""
    _rank(
        clean_runtime,
        {"calls": [ENRICHER, "code-lsp-resolution", STRUCTURE]},
    )
    store = _store(clean_runtime)
    try:
        store.apply_facts(
            [_node("sym:a"), _node("sym:b"), _edge("sym:a", "sym:b", "s1")],
            source=STRUCTURE, strict_endpoints=False,
        )
        store.apply_facts(
            [_edge("sym:a", "sym:b", "l1")],
            source="code-lsp-resolution", strict_endpoints=False,
        )
        assert _calls(store)[0] == [("code-lsp-resolution", "l1")]
        counts = store.withdraw_edge_declarations(
            branch="default", sources=["code-lsp-resolution"],
        )
        assert counts["declarations_withdrawn"] == 1
        assert _calls(store) == ([(STRUCTURE, "s1")], [(STRUCTURE, "s1")])
    finally:
        store.close()


# --- round-2 review of 8f6079d71b ---------------------------------------------

FROZEN = {"kind": "okg_paired_workspace", "lifecycle_status": "retired"}


def _ranking_rows(dsn: str) -> list[tuple[str, str, int]]:
    import psycopg

    with psycopg.connect(dsn) as conn:
        return sorted(
            conn.execute(
                "SELECT edge_type, source, rank FROM okg.edge_precedence"
            ).fetchall()
        )


def _deployment(tmp_path: Path, monkeypatch, ranking: dict[str, list[str]]) -> str:
    """A minimal deployment whose manifest declares ``ranking``, resolvable
    by name the way `okg migrate` and the worker resolve it."""
    import yaml

    root = tmp_path / "deployments"
    dep = root / "rank-probe"
    (dep / "schemas").mkdir(parents=True, exist_ok=True)
    (dep / "source_registry.yaml").write_text("sources: {}\n", encoding="utf-8")
    (dep / "deployment.yaml").write_text(yaml.safe_dump({
        "name": "rank-probe",
        "schema_dir": "schemas",
        "source_registry": "source_registry.yaml",
        "edge_precedence": ranking,
    }), encoding="utf-8")
    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(root))
    return "rank-probe"


def test_a_frozen_paired_workspace_without_stale_rows_does_not_block_a_ranking_change(
    clean_runtime: str,
) -> None:
    """Review round 2, blocker 1: the purge writes only branches that hold
    stale declarations, so a frozen paired workspace without any is skipped
    instead of failing the whole ranking change."""
    _rank(clean_runtime, RANKING)
    store = _store(clean_runtime)
    try:
        store.create_branch("pair:frozen-clean", from_branch="default", metadata=FROZEN)
        _seed(store)
    finally:
        store.close()
    _rank(clean_runtime, {"calls": [ENRICHER, "code-lsp-resolution"]})
    store = _store(clean_runtime)
    try:
        assert _calls(store) == ([(ENRICHER, "e1")], [(ENRICHER, "e1")])
    finally:
        store.close()


def test_the_worker_refuses_a_ranking_change_a_frozen_branch_blocks_and_keeps_the_old_ranking(
    clean_runtime: str,
    tmp_path: Path,
    monkeypatch,
) -> None:
    """Review round 2, blocker 1, through worker start's own call site: a
    frozen paired workspace that holds stale declarations refuses the change,
    naming the branch, and the ranking stays as it was (one transaction), so
    the next start tries again. Once the ranking keeps those rows covered the
    start succeeds."""
    from okg.substrate.errors import SubstrateError
    from okg.substrate.runtime.worker import _sync_edge_precedence

    name = _deployment(tmp_path, monkeypatch, RANKING)
    _sync_edge_precedence(deployment=name, dsn=clean_runtime)
    before = _ranking_rows(clean_runtime)
    assert (("calls", STRUCTURE, 2)) in before
    store = _store(clean_runtime)
    try:
        _seed(store)
        store.create_branch("pair:frozen-stale", from_branch="default", metadata=FROZEN)
    finally:
        store.close()

    _deployment(tmp_path, monkeypatch, {"calls": [ENRICHER, "code-lsp-resolution"]})
    with pytest.raises(SubstrateError) as refused:
        _sync_edge_precedence(deployment=name, dsn=clean_runtime)
    assert refused.value.data["branch"] == "pair:frozen-stale"
    assert _ranking_rows(clean_runtime) == before
    store = _store(clean_runtime)
    try:
        # Nothing was purged on default either: same transaction.
        assert _calls(store)[1] == [(ENRICHER, "e1"), (STRUCTURE, "s1")]
    finally:
        store.close()


def test_migrate_applies_a_ranking_change_and_its_purge(
    clean_runtime: str,
    tmp_path: Path,
    monkeypatch,
) -> None:
    """Review round 2: `okg migrate --deployment` copies the ranking and
    removes the declarations it no longer covers, through migrate's own call
    site."""
    from okg.substrate.db.migrate import migrate

    name = _deployment(tmp_path, monkeypatch, RANKING)
    migrate(clean_runtime, deployment_name=name)
    store = _store(clean_runtime)
    try:
        _seed(store)
    finally:
        store.close()
    _deployment(tmp_path, monkeypatch, {"calls": [ENRICHER, "code-lsp-resolution"]})
    migrate(clean_runtime, deployment_name=name)
    assert ("calls", STRUCTURE, 2) not in _ranking_rows(clean_runtime)
    store = _store(clean_runtime)
    try:
        assert _calls(store) == ([(ENRICHER, "e1")], [(ENRICHER, "e1")])
    finally:
        store.close()


def test_a_merge_keeps_the_row_an_unattributed_write_stored(
    clean_runtime: str,
) -> None:
    """Review round 2, item 2: an unattributed write on the fork removes every
    declaration and stores u1. Replaying the writes in order keeps u1, so the
    merge must not delete it."""
    _rank(clean_runtime, RANKING)
    store = _store(clean_runtime)
    try:
        _seed(store)
        store.create_branch("staging:m1", from_branch="default")
        store.create_branch("experiment:f", from_branch="staging:m1")
        store.apply_facts(
            [_edge("sym:a", "sym:b", "u1")],
            strict_endpoints=False, branch="experiment:f",
        )
        assert _calls(store, "experiment:f") == ([(None, "u1")], [])
        store.merge("experiment:f", "staging:m1")
        assert _calls(store, "staging:m1") == ([(None, "u1")], [])
    finally:
        store.close()


def test_a_producer_dropped_as_sole_winner_leaves_an_ordinary_row(
    clean_runtime: str,
) -> None:
    """Review round 2, item 4 (probe P2): code-structure was the edge's only
    declarer and winner; after the ranking drops it, its row stays as an
    ordinary row, and the enricher's ranked write simply wins instead of
    adopting that row as an unranked declaration and refusing."""
    _rank(clean_runtime, {"calls": [STRUCTURE]})
    store = _store(clean_runtime)
    try:
        store.apply_facts(
            [_node("sym:a"), _node("sym:b"), _edge("sym:a", "sym:b", "s1")],
            source=STRUCTURE, strict_endpoints=False,
        )
    finally:
        store.close()
    _rank(clean_runtime, {"calls": [ENRICHER]})
    store = _store(clean_runtime)
    try:
        assert _calls(store) == ([(STRUCTURE, "s1")], [])
        store.apply_facts(
            [_edge("sym:a", "sym:b", "e1")], source=ENRICHER, strict_endpoints=False,
        )
        assert _calls(store) == ([(ENRICHER, "e1")], [(ENRICHER, "e1")])
    finally:
        store.close()


# --- round-3 review of f9cb4034c4 ---------------------------------------------

DROPPED = {"calls": [ENRICHER, "code-lsp-resolution"]}


def test_a_frozen_paired_workspace_skipped_by_the_purge_is_named_in_a_warning(
    clean_runtime: str,
    caplog,
) -> None:
    """Review round 3, note 4: the skipped frozen branch is reported."""
    import logging

    _rank(clean_runtime, RANKING)
    store = _store(clean_runtime)
    try:
        store.create_branch("pair:frozen-clean", from_branch="default", metadata=FROZEN)
        _seed(store)
    finally:
        store.close()
    with caplog.at_level(logging.WARNING):
        _rank(clean_runtime, DROPPED)
    assert any(
        "skipped frozen paired-workspace branches" in record.getMessage()
        and "pair:frozen-clean" in record.getMessage()
        for record in caplog.records
    )


def test_migrate_refuses_a_ranking_change_a_frozen_branch_blocks_and_changes_nothing(
    clean_runtime: str,
    tmp_path: Path,
    monkeypatch,
) -> None:
    """Review round 3, item 3, through migrate's own call site: the refusal
    fails migrate, the ranking stays as it was, nothing is purged, and the
    hint names the real recovery (restore the ranking or delete the branch)."""
    from okg.substrate.db.migrate import migrate
    from okg.substrate.errors import SubstrateError

    name = _deployment(tmp_path, monkeypatch, RANKING)
    migrate(clean_runtime, deployment_name=name)
    store = _store(clean_runtime)
    try:
        _seed(store)
        store.create_branch("pair:frozen-stale", from_branch="default", metadata=FROZEN)
    finally:
        store.close()
    before = _ranking_rows(clean_runtime)
    _deployment(tmp_path, monkeypatch, DROPPED)
    with pytest.raises(SubstrateError) as refused:
        migrate(clean_runtime, deployment_name=name)
    assert refused.value.data["branch"] == "pair:frozen-stale"
    hint = str(refused.value.fix_hint)
    assert "edge_precedence" in hint
    assert f"okg branch delete pair:frozen-stale --deployment {name} --confirm" in hint
    assert _ranking_rows(clean_runtime) == before
    store = _store(clean_runtime)
    try:
        assert _calls(store)[1] == [(ENRICHER, "e1"), (STRUCTURE, "s1")]
    finally:
        store.close()


def test_a_ranked_write_racing_a_ranking_change_leaves_no_stale_declaration(
    clean_runtime: str,
    tmp_path: Path,
    monkeypatch,
) -> None:
    """Review round 3, blocker 1 (probe P3), with two connections at worker
    start's call site: code-structure plans a write with the old ranking
    while the purge that drops it is uncommitted. The write waits on the
    ranking lock, sees the new ranking after the fence, and is refused
    (round 4: refused, not replanned) instead of storing a declaration the
    purge already removed; the enricher keeps writing its edge."""
    import threading
    import time

    import psycopg

    from okg.substrate.graph_store_internal import writes
    from okg.substrate.graph_store_internal.edge_declarers import (
        EdgeRankingChangedDuringWrite,
    )
    from okg.substrate.runtime.worker import _sync_edge_precedence

    name = _deployment(tmp_path, monkeypatch, RANKING)
    _sync_edge_precedence(deployment=name, dsn=clean_runtime)
    store = _store(clean_runtime)
    try:
        _seed(store)
    finally:
        store.close()
    _deployment(tmp_path, monkeypatch, DROPPED)

    in_purge, release = threading.Event(), threading.Event()
    original = writes.ChronosGraphWriteMixin.purge_declarations_outside_ranking

    def held_purge(self, **kwargs):
        out = original(self, **kwargs)
        in_purge.set()  # ranking lock held, transaction not committed
        assert release.wait(30)
        return out

    monkeypatch.setattr(
        writes.ChronosGraphWriteMixin, "purge_declarations_outside_ranking", held_purge,
    )
    outcomes: dict[str, Any] = {}

    def change_ranking() -> None:
        try:
            _sync_edge_precedence(deployment=name, dsn=clean_runtime)
            outcomes["sync"] = "ok"
        except Exception as exc:  # noqa: BLE001 - asserted below
            outcomes["sync"] = exc

    def write_structure() -> None:
        writer = _store(clean_runtime)
        try:
            writer.apply_facts(
                [_edge("sym:a", "sym:b", "s2")],
                source=STRUCTURE, strict_endpoints=False,
            )
            outcomes["write"] = "ok"
        except Exception as exc:  # noqa: BLE001 - asserted below
            outcomes["write"] = exc
        finally:
            writer.close()

    changer = threading.Thread(target=change_ranking)
    changer.start()
    assert in_purge.wait(60)
    writer = threading.Thread(target=write_structure)
    writer.start()
    waiting = 0
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        for _ in range(100):
            waiting = conn.execute(
                "SELECT count(*) FROM pg_stat_activity WHERE wait_event_type = 'Lock' "
                "AND datname = current_database()"
            ).fetchone()[0]
            if waiting:
                break
            time.sleep(0.1)
    release.set()
    changer.join(60)
    writer.join(60)
    monkeypatch.undo()

    assert waiting, "the racing write never waited on a lock"
    assert outcomes["sync"] == "ok"
    assert isinstance(outcomes["write"], EdgeRankingChangedDuringWrite), outcomes["write"]
    assert outcomes["write"].code == "edge_ranking_changed_during_write"
    assert ("calls", STRUCTURE, 2) not in _ranking_rows(clean_runtime)
    store = _store(clean_runtime)
    try:
        assert _calls(store) == ([(ENRICHER, "e1")], [(ENRICHER, "e1")])
        store.apply_facts(
            [_edge("sym:a", "sym:b", "e2")], source=ENRICHER, strict_endpoints=False,
        )
        assert _calls(store) == ([(ENRICHER, "e2")], [(ENRICHER, "e2")])
    finally:
        store.close()


def test_rerunning_an_unchanged_ranking_removes_a_stray_declaration(
    clean_runtime: str,
    tmp_path: Path,
    monkeypatch,
) -> None:
    """Review round 3, blocker 1: the purge runs on every apply, not only on
    a ranking change. Rolling default back to a checkpoint taken before the
    change brings back a declaration the ranking no longer covers; the next
    worker start with the same ranking removes it."""
    from okg.substrate.runtime.worker import _sync_edge_precedence

    name = _deployment(tmp_path, monkeypatch, RANKING)
    _sync_edge_precedence(deployment=name, dsn=clean_runtime)
    store = _store(clean_runtime)
    try:
        _seed(store)
        generation = store.checkpoint_generation(1, deployment_name=DEPLOYMENT)
    finally:
        store.close()
    _deployment(tmp_path, monkeypatch, DROPPED)
    _sync_edge_precedence(deployment=name, dsn=clean_runtime)
    store = _store(clean_runtime)
    try:
        assert _calls(store) == ([(ENRICHER, "e1")], [(ENRICHER, "e1")])
        store.restore_branch_to_generation(generation, deployment_name=DEPLOYMENT)
        assert _calls(store)[1] == [(ENRICHER, "e1"), (STRUCTURE, "s1")]
    finally:
        store.close()
    rows = _ranking_rows(clean_runtime)
    _sync_edge_precedence(deployment=name, dsn=clean_runtime)
    assert _ranking_rows(clean_runtime) == rows
    store = _store(clean_runtime)
    try:
        assert _calls(store) == ([(ENRICHER, "e1")], [(ENRICHER, "e1")])
        store.apply_facts(
            [_edge("sym:a", "sym:b", "e2")], source=ENRICHER, strict_endpoints=False,
        )
        assert _calls(store) == ([(ENRICHER, "e2")], [(ENRICHER, "e2")])
    finally:
        store.close()


# --- round-4 review of 0a293340a4 ---------------------------------------------


def _hold_ranking_change(monkeypatch, run):
    """Run a ranking change in a thread and hold it right after its purge:
    the ranking lock is held exclusively and nothing is committed yet.
    Returns ``(thread, release, outcome)``."""
    import threading

    from okg.substrate.graph_store_internal import writes

    in_purge, release = threading.Event(), threading.Event()
    original = writes.ChronosGraphWriteMixin.purge_declarations_outside_ranking

    def held_purge(self, **kwargs):
        out = original(self, **kwargs)
        in_purge.set()
        assert release.wait(60)
        return out

    monkeypatch.setattr(
        writes.ChronosGraphWriteMixin, "purge_declarations_outside_ranking", held_purge,
    )
    outcome: dict[str, Any] = {}

    def change() -> None:
        try:
            run()
            outcome["change"] = "ok"
        except Exception as exc:  # noqa: BLE001 - asserted by the caller
            outcome["change"] = exc

    thread = threading.Thread(target=change)
    thread.start()
    assert in_purge.wait(60)
    return thread, release, outcome


def _lock_waiters(dsn: str) -> int:
    import psycopg

    with psycopg.connect(dsn, autocommit=True) as conn:
        return conn.execute(
            "SELECT count(*) FROM pg_stat_activity WHERE wait_event_type = 'Lock' "
            "AND datname = current_database()"
        ).fetchone()[0]


def _write_in_thread(dsn: str, facts: list, source: str):
    import threading

    outcome: dict[str, Any] = {}

    def write() -> None:
        writer = _store(dsn)
        try:
            writer.apply_facts(facts, source=source, strict_endpoints=False)
            outcome["write"] = "ok"
        except Exception as exc:  # noqa: BLE001 - asserted by the caller
            outcome["write"] = exc
        finally:
            writer.close()

    thread = threading.Thread(target=write)
    thread.start()
    return thread, outcome


def test_a_ranked_write_waits_for_a_ranking_change_even_where_nothing_is_stale(
    clean_runtime: str,
    tmp_path: Path,
    monkeypatch,
) -> None:
    """Review round 4, note 4: default holds no declaration the new ranking
    drops, so the purge never takes default's branch lock. Only the shared
    ranking lock makes code-structure's write (planned with the old ranking)
    wait for the change and then be refused; without it the write commits a
    declaration the new ranking does not cover."""
    from okg.substrate.graph_store_internal.edge_declarers import (
        EdgeRankingChangedDuringWrite,
    )
    from okg.substrate.runtime.worker import _sync_edge_precedence

    name = _deployment(tmp_path, monkeypatch, RANKING)
    _sync_edge_precedence(deployment=name, dsn=clean_runtime)
    store = _store(clean_runtime)
    try:
        store.apply_facts(
            [_node("sym:a"), _node("sym:b")], source=STRUCTURE, strict_endpoints=False,
        )
        store.apply_facts(
            [_edge("sym:a", "sym:b", "e1")], source=ENRICHER, strict_endpoints=False,
        )
    finally:
        store.close()
    _deployment(tmp_path, monkeypatch, DROPPED)
    changer, release, changed = _hold_ranking_change(
        monkeypatch,
        lambda: _sync_edge_precedence(deployment=name, dsn=clean_runtime),
    )
    writer, wrote = _write_in_thread(
        clean_runtime, [_edge("sym:a", "sym:b", "s2")], STRUCTURE,
    )
    writer.join(3)
    finished_while_held = not writer.is_alive()
    release.set()
    changer.join(60)
    writer.join(60)
    monkeypatch.undo()

    assert changed["change"] == "ok"
    assert not finished_while_held, wrote.get("write")
    assert isinstance(wrote["write"], EdgeRankingChangedDuringWrite), wrote["write"]
    store = _store(clean_runtime)
    try:
        assert _calls(store) == ([(ENRICHER, "e1")], [(ENRICHER, "e1")])
    finally:
        store.close()


def test_a_ranking_change_mid_write_aborts_the_callers_transaction_loudly(
    clean_runtime: str,
    tmp_path: Path,
    monkeypatch,
) -> None:
    """Review round 4, blocker 1 (probe test_r4_replan_rolls_back_caller_transaction):
    a ranking change commits between planning a ranked edge write and its
    fence, inside a caller-owned transaction that already wrote a node and a
    marker row. The write is refused with a typed error and the caller's
    transaction is rolled back as a whole: nothing of it is kept, and the
    caller is told. Before, the write was replanned and reported success
    while the caller's earlier writes were silently gone."""
    import psycopg

    from okg.substrate.graph_store import ChronosGraphStore
    from okg.substrate.graph_store_internal import writes
    from okg.substrate.graph_store_internal.edge_declarers import (
        EdgeRankingChangedDuringWrite,
    )
    from okg.substrate.runtime.worker import _sync_edge_precedence

    name = _deployment(tmp_path, monkeypatch, RANKING)
    _sync_edge_precedence(deployment=name, dsn=clean_runtime)
    store = _store(clean_runtime)
    try:
        store.apply_facts(
            [_node("sym:a"), _node("sym:b")], source=STRUCTURE, strict_endpoints=False,
        )
    finally:
        store.close()
    # A change with nothing stale: it never needs default's branch lock,
    # which the caller's transaction holds.
    _deployment(
        tmp_path, monkeypatch,
        {"calls": [ENRICHER, "code-lsp-resolution", STRUCTURE, "extra-producer"]},
    )
    original = writes.ChronosGraphWriteMixin._edge_precedence_ranks
    fired = {"done": False}

    def ranks_then_change(self):
        out = original(self)
        if getattr(self, "_caller_under_test", False) and not fired["done"]:
            fired["done"] = True
            _sync_edge_precedence(deployment=name, dsn=clean_runtime)
        return out

    monkeypatch.setattr(
        writes.ChronosGraphWriteMixin, "_edge_precedence_ranks", ranks_then_change,
    )
    conn = psycopg.connect(clean_runtime)
    try:
        caller = ChronosGraphStore.from_connection(
            conn, ensure_schema=False, autocommit=False,
        )
        caller._caller_under_test = True
        caller.apply_facts([_node("sym:x")], source=STRUCTURE, strict_endpoints=False)
        conn.execute("CREATE TABLE public.edge_ranking_probe_marker (v int)")
        conn.execute("INSERT INTO public.edge_ranking_probe_marker VALUES (1)")
        with pytest.raises(EdgeRankingChangedDuringWrite) as refused:
            caller.apply_facts(
                [_edge("sym:a", "sym:b", "s1")], source=STRUCTURE, strict_endpoints=False,
            )
        assert fired["done"]
        assert refused.value.code == "edge_ranking_changed_during_write"
        assert "rerun" in str(refused.value.fix_hint)
        # Whatever the caller does next, nothing of its transaction is kept.
        conn.commit()
    finally:
        conn.close()
    monkeypatch.undo()
    with psycopg.connect(clean_runtime, autocommit=True) as check:
        assert check.execute(
            "SELECT to_regclass('public.edge_ranking_probe_marker')"
        ).fetchone()[0] is None
    store = _store(clean_runtime)
    try:
        assert not store.query(
            "SELECT node_id FROM okg.graph_nodes WHERE node_id = 'sym:x'"
        )
        assert _calls(store) == ([], [])
    finally:
        store.close()


@pytest.mark.parametrize("site", ["worker", "migrate"])
def test_a_ranking_change_gives_way_to_a_branch_lock_holder(
    clean_runtime: str,
    tmp_path: Path,
    monkeypatch,
    site: str,
) -> None:
    """Review round 4, item 2 (probe test_r4_deadlock_branch_then_ranking):
    a writer holds default's branch lock first (as merge-back, source
    retirement and the publisher do) and then needs the ranking lock, while
    a ranking change holds the ranking lock and needs default's branch lock.
    The ranking change gives way: it is refused with a typed, retryable
    error, nothing of it is applied, and the branch holder's work commits."""
    import threading
    import time

    import psycopg

    from okg.substrate.db.migrate import migrate
    from okg.substrate.graph_store import ChronosGraphStore
    from okg.substrate.graph_store_internal.edge_declarers import (
        EdgeRankingChangeBusy,
    )
    from okg.substrate.runtime.worker import _sync_edge_precedence

    def change() -> None:
        if site == "migrate":
            migrate(clean_runtime, deployment_name=name)
        else:
            _sync_edge_precedence(deployment=name, dsn=clean_runtime)

    name = _deployment(tmp_path, monkeypatch, RANKING)
    change()
    store = _store(clean_runtime)
    try:
        _seed(store)
        store.apply_facts(
            [_edge("sym:a", "sym:b", "l1")],
            source="code-lsp-resolution", strict_endpoints=False,
        )
    finally:
        store.close()
    before = _ranking_rows(clean_runtime)
    _deployment(tmp_path, monkeypatch, DROPPED)
    holding, go = threading.Event(), threading.Event()
    out: dict[str, Any] = {}

    def holder() -> None:
        conn = psycopg.connect(clean_runtime, autocommit=True)
        held = ChronosGraphStore.from_connection(conn, ensure_schema=False)
        try:
            held._lock_branch_write_session("default")
            holding.set()
            go.wait(60)
            try:
                out["holder"] = held.withdraw_edge_declarations(
                    branch="default", sources=["code-lsp-resolution"],
                )
            except Exception as exc:  # noqa: BLE001 - asserted below
                out["holder"] = exc
        finally:
            held._unlock_branch_write_session("default")
            conn.close()

    def changer() -> None:
        try:
            change()
            out["change"] = "ok"
        except Exception as exc:  # noqa: BLE001 - asserted below
            out["change"] = exc

    holder_thread = threading.Thread(target=holder)
    holder_thread.start()
    assert holding.wait(60)
    changer_thread = threading.Thread(target=changer)
    changer_thread.start()
    for _ in range(50):
        if _lock_waiters(clean_runtime):
            break
        time.sleep(0.02)
    go.set()
    holder_thread.join(120)
    changer_thread.join(120)

    assert isinstance(out["change"], EdgeRankingChangeBusy), out["change"]
    assert out["change"].code == "edge_ranking_change_busy"
    assert out["change"].data["branch"] == "default"
    assert "retry the ranking change" in str(out["change"].fix_hint)
    assert isinstance(out["holder"], dict), out["holder"]
    assert out["holder"]["declarations_withdrawn"] == 1
    assert _ranking_rows(clean_runtime) == before
    store = _store(clean_runtime)
    try:
        # The holder's withdrawal committed; the change purged nothing.
        assert _calls(store)[1] == [(ENRICHER, "e1"), (STRUCTURE, "s1")]
    finally:
        store.close()


def test_the_frozen_refusal_names_cascade_when_the_branch_has_children(
    clean_runtime: str,
    tmp_path: Path,
    monkeypatch,
) -> None:
    """Review round 4, note 7: `okg branch delete` refuses a branch with
    children unless --cascade is passed, so the hint says so and says plainly
    that it deletes the children too."""
    from okg.substrate.errors import SubstrateError
    from okg.substrate.runtime.worker import _sync_edge_precedence

    name = _deployment(tmp_path, monkeypatch, RANKING)
    _sync_edge_precedence(deployment=name, dsn=clean_runtime)
    store = _store(clean_runtime)
    try:
        _seed(store)
        store.create_branch("pair:ws1", from_branch="default", metadata=FROZEN)
        store.create_branch("pair:ws1-child", from_branch="pair:ws1")
    finally:
        store.close()
    _deployment(tmp_path, monkeypatch, DROPPED)
    with pytest.raises(SubstrateError) as refused:
        _sync_edge_precedence(deployment=name, dsn=clean_runtime)
    hint = str(refused.value.fix_hint)
    assert refused.value.data["branch"] == "pair:ws1"
    assert refused.value.data["child_branches"] == ["pair:ws1-child"]
    assert (
        f"okg branch delete pair:ws1 --deployment {name} --confirm --cascade" in hint
    )
    assert "every child branch" in hint


def test_an_unranked_edge_write_does_not_wait_for_a_ranking_change(
    clean_runtime: str,
    tmp_path: Path,
    monkeypatch,
) -> None:
    """Review round 4, note 6: only writes that can touch a declaration (a
    ranked edge type, or a delete) take the ranking lock; an edge write of a
    type the ranking does not list goes through while a change is held."""
    import dataclasses

    from okg.substrate.runtime.worker import _sync_edge_precedence

    name = _deployment(tmp_path, monkeypatch, RANKING)
    _sync_edge_precedence(deployment=name, dsn=clean_runtime)
    store = _store(clean_runtime)
    try:
        # Nothing stale for the change below, so it does not hold default's
        # branch lock either: only the ranking lock could hold the write up.
        store.apply_facts(
            [_node("sym:a"), _node("sym:b")], source=STRUCTURE, strict_endpoints=False,
        )
        store.apply_facts(
            [_edge("sym:a", "sym:b", "e1")], source=ENRICHER, strict_endpoints=False,
        )
    finally:
        store.close()
    _deployment(tmp_path, monkeypatch, DROPPED)
    changer, release, changed = _hold_ranking_change(
        monkeypatch,
        lambda: _sync_edge_precedence(deployment=name, dsn=clean_runtime),
    )
    unranked = dataclasses.replace(_edge("sym:b", "sym:a", "x1"), edge_type="contains")
    writer, wrote = _write_in_thread(clean_runtime, [unranked], STRUCTURE)
    writer.join(30)
    finished_while_held = not writer.is_alive()
    release.set()
    changer.join(60)
    writer.join(60)
    monkeypatch.undo()
    assert changed["change"] == "ok"
    assert finished_while_held
    assert wrote["write"] == "ok"


# --- round-5 review of 4e0c787ff6 ---------------------------------------------


def _waits_for_the_ranking_lock(dsn: str, action) -> tuple[bool, Any]:
    """Hold the edge ranking lock exclusively on another connection, as a
    ranking change does, and run ``action`` in a thread. Returns whether it
    was still waiting after 3 seconds, and what it returned once released."""
    import threading

    import psycopg

    out: dict[str, Any] = {}

    def run() -> None:
        try:
            out["result"] = action()
        except Exception as exc:  # noqa: BLE001 - asserted by the caller
            out["result"] = exc

    ranking = psycopg.connect(dsn)
    try:
        ranking.execute(
            "SELECT pg_advisory_xact_lock(hashtext('okg.edge_precedence'), "
            "hashtext('ranking'))"
        )
        thread = threading.Thread(target=run)
        thread.start()
        thread.join(3)
        waited = thread.is_alive()
    finally:
        ranking.rollback()
        ranking.close()
    thread.join(120)
    return waited, out.get("result")


@pytest.mark.parametrize("path", ["restore", "prune", "orphan_cleanup"])
def test_every_declaration_writer_waits_for_a_ranking_change(
    clean_runtime: str, path: str,
) -> None:
    """Review round 5, item 1a: restoring a branch to a generation, pruning
    dangling edges and the orphaned-edge cleanup all rewrite edge
    declarations, so each holds the ranking lock shared and waits while a
    ranking change holds it exclusively, then completes."""
    from okg.substrate.graph_store_internal.common import edge_id

    _rank(clean_runtime, RANKING)
    store = _store(clean_runtime)
    try:
        if path == "orphan_cleanup":
            store.apply_facts([_node("sym:a")], source=STRUCTURE, strict_endpoints=False)
            store.apply_facts(
                [_edge("sym:a", "sym:gone", "e1")],
                source=ENRICHER, strict_endpoints=False,
            )

            def action():
                return store.retract_orphaned_derived_edges()

            expected: Any = 1
        elif path == "prune":
            _seed(store)

            def action():
                return store.prune_edges([edge_id("sym:a", "calls", "sym:b")])

            expected = 1
        else:
            _seed(store)
            generation = store.checkpoint_generation(1, deployment_name=DEPLOYMENT)
            store.apply_facts(
                [_edge("sym:a", "sym:b", "s2")], source=STRUCTURE, strict_endpoints=False,
            )

            def action():
                store.restore_branch_to_generation(
                    generation, deployment_name=DEPLOYMENT,
                )
                return "restored"

            expected = "restored"
        waited, result = _waits_for_the_ranking_lock(clean_runtime, action)
        assert waited, result
        assert result == expected, result
        if path == "restore":
            assert _calls(store)[1] == [(ENRICHER, "e1"), (STRUCTURE, "s1")]
        else:
            assert _calls(store) == ([], [])
    finally:
        store.close()


def test_a_node_delete_waits_for_a_ranking_change(clean_runtime: str) -> None:
    """Review round 5, item 1b: a node delete takes the declarations of its
    ranked edges with it, so it holds the ranking lock shared even when the
    write carries no edge fact; it waits for a ranking change in progress,
    then completes."""
    _rank(clean_runtime, RANKING)
    store = _store(clean_runtime)
    try:
        _seed(store)

        def action():
            store.apply_facts(
                [_node("sym:b", op="R")], source=STRUCTURE, strict_endpoints=False,
            )
            return "deleted"

        waited, result = _waits_for_the_ranking_lock(clean_runtime, action)
        assert waited, result
        assert result == "deleted", result
        assert _calls(store) == ([], [])
    finally:
        store.close()


def test_incident_edge_lookup_for_a_node_delete_keeps_only_the_deleting_sources_edges(
    clean_runtime: str,
) -> None:
    """Issue 2383: the source filter applied to the dst side of the lookup
    only, so another source's edge that starts at the deleted node entered
    the touched-edge set. Both directions must be filtered by source."""
    from okg.substrate.graph_store import ChronosGraphStore

    other = "other-source"
    store = _store(clean_runtime)
    try:
        store.apply_facts(
            [_node("sym:a"), _node("sym:b"), _node("sym:c")],
            source=STRUCTURE, strict_endpoints=False,
        )
        store.apply_facts(
            [_edge("sym:a", "sym:b", "own-out"), _edge("sym:c", "sym:a", "own-in")],
            source=STRUCTURE, strict_endpoints=False,
        )
        store.apply_facts(
            [_edge("sym:a", "sym:c", "foreign-out"),
             _edge("sym:b", "sym:a", "foreign-in")],
            source=other, strict_endpoints=False,
        )
        by_source = {
            (row["source"], row["src"], row["dst"]): int(row["edge_id"])
            for row in store.query(
                "SELECT edge_id, source, src, dst FROM okg.graph_edges "
                "WHERE edge_type = 'calls'",
            )
        }
        assert len(by_source) == 4, by_source
        rows = ChronosGraphStore._incident_edges_for_node_deletes(
            store, store.checkout("default"), [{"node_id": "sym:a"}],
            source=STRUCTURE,
        )
        assert sorted(int(r["edge_id"]) for r in rows) == sorted([
            by_source[(STRUCTURE, "sym:a", "sym:b")],
            by_source[(STRUCTURE, "sym:c", "sym:a")],
        ])
    finally:
        store.close()
