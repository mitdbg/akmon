"""Coverage tests for `DefaultProjectMembershipEnricher`.

Per `cleanup-personal-okg-data-quality` C1 item 3 task 3.4.

Three layers:
1. Constructor validation (no DB).
2. enrich() emits zero edges when no eligible changes exist (DB).
3. enrich() back-fills the gap for the three documented cases:
   - change with `project: <other>` frontmatter → openspec source
     already emitted; this enricher SKIPS via the
     `NOT EXISTS member_of` filter.
   - change with `project: none` sentinel → enricher SKIPS (counted
     as `n_edges_skipped`).
   - change with no project frontmatter → enricher EMITS to
     deployment default.
"""
from __future__ import annotations

import logging

import psycopg
import pytest

from okg.substrate.library.enrichers.openspec.default_project import (
    DefaultProjectMembershipEnricher,
)

pytestmark = pytest.mark.postgres


# ---------- constructor validation ----------


def test_ctor_requires_default_project() -> None:
    with pytest.raises(ValueError, match="default_project"):
        DefaultProjectMembershipEnricher(default_project="")


def test_ctor_rejects_non_string() -> None:
    with pytest.raises(ValueError, match="default_project"):
        # type: ignore[arg-type]
        DefaultProjectMembershipEnricher(default_project=None)


def test_ctor_prefixes_proj_when_absent() -> None:
    enricher = DefaultProjectMembershipEnricher(
        default_project="personal-okg",
    )
    assert enricher.default_project == "personal-okg"
    assert enricher.default_project_node_id == "proj:personal-okg"


def test_ctor_preserves_existing_proj_prefix() -> None:
    enricher = DefaultProjectMembershipEnricher(
        default_project="proj:my-project",
    )
    assert enricher.default_project_node_id == "proj:my-project"


def test_ctor_declares_required_narrowing() -> None:
    enricher = DefaultProjectMembershipEnricher(
        default_project="personal-okg",
    )
    assert enricher.emits_edge_types == ("member_of",)
    assert enricher.reads_subtypes == ("open_spec_change",)
    assert (
        ("open_spec_change", "member_of", "project"),
    ) == enricher.requires_narrowings


# ---------- enrich() DB-using ----------


def _ensure_generation(
    conn: psycopg.Connection, generation_id: int,
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT max(catalog_version_id) FROM okg.catalog_versions "
            "WHERE status = 'applied'"
        )
        cv = cur.fetchone()[0] or 1
        cur.execute(
            "INSERT INTO okg.graph_generations "
            "  (generation_id, deployment_name, status, "
            "   catalog_version_id, published_at, correlation_id) "
            "VALUES (%s, 'default-project-test', 'published', %s, "
            "        now(), %s) "
            "ON CONFLICT (generation_id) DO NOTHING",
            (generation_id, cv, f"default-project-test:{generation_id}"),
        )


def _project_fact(node_id: str):
    """Build a `project` NodeFact for the Chronos graph seed."""
    from okg.substrate.library.sources.base import NodeFact

    return NodeFact(
        node_id, "project", {}, {"id": node_id}, {"v": 1},
    )


def _change_fact(node_id: str, attrs: dict):
    """Build an `open_spec_change` NodeFact for the Chronos graph seed."""
    from okg.substrate.library.sources.base import NodeFact

    return NodeFact(
        node_id, "open_spec_change", dict(attrs), {"id": node_id}, {"v": 1},
    )


def _seed_graph(
    conn: psycopg.Connection, *, facts, generation_id: int,
) -> None:
    """Write the given NodeFacts to the Chronos graph and publish a
    `generation:<id>` checkpoint so the enricher's
    `checkout_generation(generation_id)` resolves the seeded nodes."""
    from okg.substrate.graph_store import ChronosGraphStore

    store = ChronosGraphStore.from_connection(conn)
    try:
        if facts:
            store.apply_facts(list(facts), source="default")
        store.checkpoint_generation(
            generation_id,
            deployment_name="default-project-test",
        )
    finally:
        store.close()


def test_enrich_emits_zero_when_no_changes(clean_runtime: str) -> None:
    """No OpenSpecChange rows → zero emissions."""
    enricher = DefaultProjectMembershipEnricher(default_project="p1")
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        gen = 100
        _ensure_generation(conn, gen)
        _seed_graph(
            conn,
            facts=[_project_fact("proj:p1")],
            generation_id=gen,
        )
        result = enricher.enrich(conn, generation_id=gen)
    assert result.n_edges_emitted == 0
    assert result.n_edges_skipped == 0


def test_enrich_backfills_change_lacking_project_edge(
    clean_runtime: str,
) -> None:
    """Change with no project frontmatter + no inbound member_of →
    enricher emits the edge to the default project."""
    enricher = DefaultProjectMembershipEnricher(default_project="p1")
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        gen = 101
        _ensure_generation(conn, gen)
        _seed_graph(
            conn,
            facts=[
                _project_fact("proj:p1"),
                _change_fact(
                    "change:no-frontmatter",
                    {"label": "Some Change"},
                ),
            ],
            generation_id=gen,
        )
        result = enricher.enrich(conn, generation_id=gen)
    assert result.n_edges_emitted == 1
    assert result.n_edges_skipped == 0


def test_enrich_respects_project_none_sentinel(
    clean_runtime: str,
) -> None:
    """Change with `project: none` frontmatter → SKIP, count in
    n_edges_skipped."""
    enricher = DefaultProjectMembershipEnricher(default_project="p1")
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        gen = 102
        _ensure_generation(conn, gen)
        _seed_graph(
            conn,
            facts=[
                _project_fact("proj:p1"),
                _change_fact(
                    "change:opted-out",
                    {"label": "Opt-out", "project": "none"},
                ),
            ],
            generation_id=gen,
        )
        result = enricher.enrich(conn, generation_id=gen)
    assert result.n_edges_emitted == 0
    assert result.n_edges_skipped == 1


def test_enrich_warns_when_default_project_absent(
    clean_runtime: str, caplog,
) -> None:
    """If the default project node isn't in nodes_live, enricher
    refuses to emit (no silent fallback to dangling edge)."""
    enricher = DefaultProjectMembershipEnricher(
        default_project="missing-project",
    )
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        gen = 103
        _ensure_generation(conn, gen)
        # NB: no Project node seeded.
        _seed_graph(
            conn,
            facts=[
                _change_fact(
                    "change:orphan-candidate",
                    {"label": "No project to attach to"},
                ),
            ],
            generation_id=gen,
        )
        with caplog.at_level(
            logging.WARNING,
            logger=(
                "okg.substrate.library.enrichers.openspec"
                ".default_project"
            ),
        ):
            result = enricher.enrich(conn, generation_id=gen)
    assert result.n_edges_emitted == 0
    # n_edges_skipped should reflect the un-backfilled candidate.
    assert result.n_edges_skipped == 1
    assert any(
        "default project" in rec.message.lower()
        for rec in caplog.records
    )
