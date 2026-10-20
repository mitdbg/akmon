from __future__ import annotations

from pathlib import Path

import psycopg
import pytest
import yaml
from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer  # noqa: E402


pytestmark = pytest.mark.postgres


def _write(path: Path, text: str) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")
    return path


def test_acceptance_criteria_linker_publish_connects_tests_and_reports(
    clean_runtime: str,
    tmp_path: Path,
) -> None:
    pytest.importorskip("defusedxml")

    from okg.substrate.catalog.modules_apply import apply_composed_catalog
    from okg.substrate.catalog.modules_compose import compose_catalog
    from okg.substrate.ingest import SourceRunner
    from okg.substrate.library.linkers.acceptance_criteria import (
        AcceptanceCriteriaLinker,
    )
    from okg.substrate.library.sources.openspec import OpenSpecSource
    from okg.substrate.library.sources.test_reports import TestReportSource
    from okg.substrate.publisher import Publisher

    repo = tmp_path / "repo"
    openspec_root = repo / "openspec"
    _write(
        openspec_root / "changes" / "demo-change" / "proposal.md",
        """
# Demo Change

## Why

The graph needs test-backed acceptance criteria.

## What Changes

- Add a fixture acceptance criterion.
""".strip(),
    )
    _write(
        openspec_root
        / "changes"
        / "demo-change"
        / "specs"
        / "demo-capability"
        / "spec.md",
        """
## ADDED Requirements

### Requirement: Criteria are visible to tests

Tests SHALL be able to report stable OpenSpec criterion ids.

#### Scenario: Criteria marker appears in JUnit

- **GIVEN** a JUnit testcase cites a criterion id
- **WHEN** the acceptance criteria linker runs
- **THEN** the criterion is verified by the test case
""".strip(),
    )
    junit = _write(
        repo / "reports" / "junit.xml",
        """<?xml version="1.0" encoding="utf-8"?>
<testsuites>
  <testsuite name="pytest" tests="1" failures="0" errors="0" skipped="0" time="0.01">
    <testcase classname="tests.test_spec" name="test_criteria_link" time="0.01" file="tests/test_spec.py">
      <properties>
        <property name="openspec_criterion" value="crit:demo-change:0"/>
      </properties>
    </testcase>
  </testsuite>
</testsuites>
""",
    )

    dep = tmp_path / "dep"
    dep.mkdir()
    (dep / "deployment.yaml").write_text(yaml.safe_dump({
        "name": "test-acceptance-criteria",
        "description": "acceptance criteria linker publish fixture",
        "modules": [
            "git_graph",
            "openspec",
            "agent_sessions",
            "compute_env",
            "ci",
            "code_graph",
        ],
    }))
    (dep / "schemas").mkdir()

    composed = compose_catalog(dep)
    with psycopg.connect(clean_runtime) as conn:
        apply_composed_catalog(conn, composed)

    SourceRunner(clean_runtime, source_name="fixture.openspec").run(
        OpenSpecSource(
            repo="demo",
            openspec_root=openspec_root,
            include_archived=False,
        ),
        mode="scope_complete",
    )
    SourceRunner(clean_runtime, source_name="fixture.test-reports").run(
        TestReportSource(
            repo="demo",
            repo_root=repo,
            artifact_paths=[junit],
            infer_sha=False,
        ),
        mode="scope_complete",
    )

    out = Publisher(
        clean_runtime,
        "test-acceptance-criteria",
        enrichers=[AcceptanceCriteriaLinker()],
        catalog_identity=bind_test_catalog_writer(clean_runtime, "test-acceptance-criteria"),
    ).cycle()
    assert out["status"] == "published", out

    # The chronos-native cutover dropped okg.nodes_live / okg.edges_live; the
    # linker's verified_by edges + their endpoints are read from the Chronos
    # graph at the published generation through a checkout.
    from okg.substrate.graph_store import ChronosGraphStore

    store = ChronosGraphStore.connect(clean_runtime, ensure_schema=False)
    try:
        graph = store.checkout_generation(out["generation_id"])
        edge_counts = {
            (str(r["src_subtype"]), str(r["edge_type"]), str(r["dst_subtype"])): int(r["count"])
            for r in graph.query(
                """
                SELECT p.subtype AS src_subtype,
                       e.edge_type,
                       c.subtype AS dst_subtype,
                       count(*) AS count
                  FROM okg.graph_edges e
                  JOIN okg.graph_nodes p ON p.node_id = e.src
                  JOIN okg.graph_nodes c ON c.node_id = e.dst
                 WHERE e.edge_type = 'verified_by'
                 GROUP BY 1, 2, 3
                 ORDER BY 1, 2, 3
                """
            )
        }
    finally:
        store.close()

    with psycopg.connect(clean_runtime) as conn, conn.cursor() as cur:
        diagnostics = AcceptanceCriteriaLinker().diagnostics(conn)
        cur.execute(
            "SELECT count(*) FROM okg.edge_facts "
            "WHERE source = '_enricher:acceptance_criteria'"
        )
        before = int(cur.fetchone()[0])
        second = AcceptanceCriteriaLinker().enrich(conn, generation_id=999)
        cur.execute(
            "SELECT count(*) FROM okg.edge_facts "
            "WHERE source = '_enricher:acceptance_criteria'"
        )
        after = int(cur.fetchone()[0])

    assert edge_counts == {
        ("acceptance_criterion", "verified_by", "test_case"): 1,
        ("acceptance_criterion", "verified_by", "test_report"): 1,
    }
    assert second.n_edges_emitted == 0
    assert second.n_edges_skipped == 2
    assert after == before
    assert diagnostics.eligible_criteria == 1
    assert diagnostics.matched_criteria == 1
    assert diagnostics.matched_tests == 1
    assert diagnostics.unmatched_criterion_count == 0


def test_acceptance_criteria_linker_second_run_emits_zero_edge_facts(
    clean_runtime: str,
) -> None:
    from okg.substrate.graph_store import ChronosGraphStore
    from okg.substrate.library.linkers.acceptance_criteria import (
        AcceptanceCriteriaLinker,
    )
    from okg.substrate.library.sources.base import EdgeFact, NodeFact
    from tests.substrate._db_helpers import ensure_published_generation

    linker = AcceptanceCriteriaLinker()
    with psycopg.connect(clean_runtime, autocommit=True) as conn, \
         conn.cursor() as cur:
        ensure_published_generation(conn, 1)
        ChronosGraphStore.from_connection(conn).apply_facts(
            [
                NodeFact(
                    "crit:demo:0",
                    "acceptance_criterion",
                    {"criterion_id": "crit:demo:0", "title": "Covered"},
                    {"id": "crit:demo:0"},
                    {"v": 1},
                ),
                NodeFact(
                    "test-case:1",
                    "test_case",
                    {"criteria_refs": ["crit:demo:0"]},
                    {"id": "test-case:1"},
                    {"v": 1},
                ),
                NodeFact(
                    "test-report:1",
                    "test_report",
                    {"report_id": "test-report:1"},
                    {"id": "test-report:1"},
                    {"v": 1},
                ),
                EdgeFact(
                    "test-report:1",
                    "test-case:1",
                    "contains",
                    source_record_id={"id": "test-report:1->test-case:1"},
                    source_revision={"v": 1},
                ),
            ],
            source="fixture.test-reports",
            strict_endpoints=True,
        )

        first = linker.enrich(conn, generation_id=2)
        second = linker.enrich(conn, generation_id=3)
        cur.execute(
            "SELECT count(*) FROM okg.edge_facts "
            "WHERE source = '_enricher:acceptance_criteria'"
        )
        count = int(cur.fetchone()[0])

    assert first.n_edges_emitted == 2
    assert first.n_edges_skipped == 0
    assert second.n_edges_emitted == 0
    assert second.n_edges_skipped == 2
    assert count == 2


def test_acceptance_criteria_diagnostics_report_unmatched(
    clean_runtime: str,
) -> None:
    from okg.substrate.library.linkers.acceptance_criteria import (
        AcceptanceCriteriaLinker,
    )
    from okg.substrate.graph_store import ChronosGraphStore
    from okg.substrate.library.sources.base import NodeFact

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        ChronosGraphStore.from_connection(conn).apply_facts(
            [
                NodeFact(
                    "crit:demo:0",
                    "acceptance_criterion",
                    {"criterion_id": "crit:demo:0", "title": "Covered"},
                    {},
                    {},
                ),
                NodeFact(
                    "crit:demo:1",
                    "acceptance_criterion",
                    {"criterion_id": "crit:demo:1", "title": "Missing"},
                    {},
                    {},
                ),
                NodeFact(
                    "test-case:1",
                    "test_case",
                    {"criteria_refs": ["crit:demo:0"]},
                    {},
                    {},
                ),
            ],
            source="fixture.acceptance-criteria",
        )
        diagnostics = AcceptanceCriteriaLinker().diagnostics(conn)

    assert diagnostics.eligible_criteria == 2
    assert diagnostics.matched_criteria == 1
    assert diagnostics.unmatched_criterion_count == 1
    assert diagnostics.unmatched_criteria[0]["criterion_id"] == "crit:demo:1"
