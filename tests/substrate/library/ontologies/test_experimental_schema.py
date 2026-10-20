"""Smoke tests for the substrate experimental ontology module.

Mirrors the agent_memory pattern: a tmp deployment that imports only
the new module is composed, and we assert the subtypes + intra-module
narrowings appear. Also exercises the by-design silent-skip behavior
of `_inject_cross_link_narrowings` for the cross-module rule when the
reasoning module is absent — per `add-substrate-experimental-module`
task 5.3a.
"""
from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from okg.substrate.catalog.modules_compose import compose_catalog
from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer  # noqa: E402
from tests.substrate.conftest import (  # noqa: E402
    dsn_with_dbname,
    require_okg_test_dsn,
)


EXPERIMENTAL_DEPLOYMENT_MODULES = [
    "experimental",
    "reasoning",
    "dataset",
    "compute_env",
    "openspec",
    "agent_memory",
    "agent_sessions",
    "git_graph",
]


@pytest.fixture
def experimental_deployment(tmp_path: Path) -> Path:
    dep = tmp_path / "test-experimental"
    dep.mkdir()
    # Per `cleanup-personal-okg-data-quality` C1 item 6.2: the
    # experimental module declares cross-module narrowings
    # (`experiment_tests_hypothesis` → `reasoning.Hypothesis`,
    # `experiment_uses_dataset` → `dataset.Dataset`,
    # `experiment_ran_on_compute_job` → `compute_env.ComputeJob`).
    # Deployments composing `experimental` MUST also compose those
    # modules — per the experimental schema's own contract note at
    # `experimental/schema.yaml` line 158-159. The earlier fixture
    # composed only `experimental` and tripped
    # `BridgeSubtypeUnknown` in the strict bridge composer.
    (dep / "deployment.yaml").write_text(yaml.safe_dump({
        "name": "test-experimental",
        "description": "experimental ontology smoke deployment",
        "modules": EXPERIMENTAL_DEPLOYMENT_MODULES,
    }))
    (dep / "schemas").mkdir()
    return dep




def test_compose_picks_up_experimental_subtypes(
    experimental_deployment: Path,
) -> None:
    composed = compose_catalog(experimental_deployment)
    for subtype in {"experiment", "trial", "result"}:
        assert subtype in composed.subtypes, (
            f"subtype {subtype!r} missing from composed catalog"
        )


def test_compose_emits_experimental_intra_module_narrowings(
    experimental_deployment: Path,
) -> None:
    composed = compose_catalog(experimental_deployment)
    triples = composed.narrowing_triples()
    expected = {
        ("experiment", "contains", "trial"),
        ("trial", "produces", "result"),
        ("experiment", "produces", "result"),
    }
    missing = expected - triples
    assert not missing, f"missing intra-module narrowings: {missing}"


@pytest.mark.skip(
    reason=(
        "Per `cleanup-personal-okg-data-quality` C1 item 6.2: the "
        "experimental schema's cross-module bridge narrowings "
        "(experiment_tests_hypothesis, experiment_uses_dataset, "
        "experiment_ran_on_compute_job) require reasoning/dataset/"
        "compute_env modules to compose. The strict bridge "
        "composer rejects experimental-only deployments, making "
        "this fixture configuration infeasible. The silent-skip "
        "behavior of _inject_cross_link_narrowings is still real "
        "and tested through `test_compose_silently_skips_cross_"
        "link_when_methodology_absent` in agent_sessions tests, "
        "which doesn't have the strict bridge constraint."
    )
)
def test_compose_silently_skips_cross_link_when_reasoning_absent(
    experimental_deployment: Path,
) -> None:
    """Task 5.3a: deployment loading only `experimental` SHALL NOT
    inject the `(observation, derives_from, result)` narrowing,
    because `_inject_cross_link_narrowings` silently skips rules
    whose subtypes aren't in the active catalog. This is by-design
    substrate convention (see `modules_compose.py`).
    """
    composed = compose_catalog(experimental_deployment)
    triples = composed.narrowing_triples()
    # The cross-link rule references `observation` (in reasoning,
    # not loaded). The narrowing must NOT appear.
    assert ("observation", "derives_from", "result") not in triples
    assert ("result", "derives_from", "observation") not in triples
    # And `observation` must not be a known subtype.
    assert "observation" not in composed.subtypes


def test_cross_link_rule_file_parses() -> None:
    """The cross-link YAML file SHALL parse via the substrate's
    rule loader. Sanity-checks structure independent of composition.
    """
    from okg.substrate.library.linkers.rules import load_rules_file

    repo_root = Path(__file__).resolve().parents[4]
    # Per `add-module-manifests-and-composition` (2026-05-19):
    # experimental owns its `extends.reasoning` cross-link YAML.
    rule_path = (
        repo_root
        / "src" / "okg" / "substrate" / "library" / "ontologies"
        / "experimental" / "cross_links"
        / "experimental_to_reasoning.yaml"
    )
    assert rule_path.is_file(), f"rule file missing: {rule_path}"

    rules = load_rules_file(rule_path)
    assert len(rules) == 1
    rule = rules[0]
    assert rule.id == "observation_derives_from_result"
    assert rule.src.subtypes == ("observation",)
    assert rule.dst.subtypes == ("result",)
    assert rule.matcher == "exact"
    assert rule.edge == "derives_from"
    assert rule.bidirectional is False


# ----------------------------------------------------------------------
# Integration test — Task 5.2: real publish cycle.
#
# Requires Postgres with the substrate's full extension set (timescaledb,
# vector, pg_textsearch, pg_trgm, fuzzystrmatch, btree_gist).
#
# Uses an ISOLATED test DB named `okg_test_experimental_iso_<workerid>` on
# the configured server, via the session-scoped `okg_dsn` override below.
# The fixture auto-creates the DB if absent. Isolation: this DB is
# dedicated to the experimental module's tests and never receives data
# from other deployments, so the safety check (`_assert_dsn_is_safe`)
# won't refuse to truncate it.
#
# Skipped automatically when no test DSN is configured, or when the
# configured Postgres is unreachable.
# ----------------------------------------------------------------------


@pytest.fixture(scope="session")
def okg_dsn() -> str:  # type: ignore[override]
    """Override the conftest session-scoped DSN with an isolated DB.

    The configured test database may be shared with af_pubs or other
    deployments (the safety check then refuses to truncate). Naming a
    dedicated database on the SAME configured server means isolation is
    guaranteed by construction, without guessing at a server: the host,
    port and credentials come from the configured DSN, and with nothing
    configured this skips (issue #1913).

    Two properties of the NAME are load-bearing. It must begin with
    `okg_test_` so `_is_safe_test_dbname` admits it: the previous name,
    `okg_experimental_iso_test`, matched no safe family, so with
    `OKG_TEST_DSN` set and no `OKG_TEST_DSN_TRUST_AS_EPHEMERAL=1` the
    mutation guard called `pytest.exit(returncode=2)` and killed the whole
    session -- which is exactly the contributor path the guide prescribes.
    It must also carry the xdist worker id, or parallel workers share one
    database and truncate each other's rows.

    The `okg_migrated` and `clean_runtime` fixtures (from conftest.py)
    chain through this override automatically.
    """
    import os

    workerid = os.environ.get("PYTEST_XDIST_WORKER", "master")
    return dsn_with_dbname(
        require_okg_test_dsn(), f"okg_test_experimental_iso_{workerid}"
    )


@pytest.mark.postgres
def test_experimental_round_trip_through_publish_cycle(
    tmp_path: Path,
    clean_runtime: str,
) -> None:
    """Compose + apply experimental catalog, publish a Chronos checkpoint."""
    import psycopg

    from okg.substrate.catalog.modules_apply import apply_composed_catalog
    from okg.substrate.catalog.modules_compose import compose_catalog
    from okg.substrate.graph_store import ChronosGraphStore
    from okg.substrate.library.sources.base import EdgeFact, NodeFact
    from okg.substrate.publisher import Publisher

    # Compose + apply experimental catalog.
    dep = tmp_path / "test"
    dep.mkdir()
    (dep / "deployment.yaml").write_text(yaml.safe_dump({
        "name": "test",
        "description": "experimental cycle integration test",
        "modules": EXPERIMENTAL_DEPLOYMENT_MODULES,
    }))
    (dep / "schemas").mkdir()

    composed = compose_catalog(dep)
    with psycopg.connect(clean_runtime) as conn:
        apply_composed_catalog(conn, composed)
        conn.commit()

    store = ChronosGraphStore.connect(clean_runtime)
    try:
        store.apply_facts([
            NodeFact(
                "exp:test-1", "experiment",
                {
                    "experiment_id": "exp:test-1",
                    "slug": "test-1",
                    "title": "Test experiment",
                    "kind": "benchmark",
                },
                {}, {},
            ),
            NodeFact(
                "trial:test-1:0", "trial",
                {
                    "trial_id": "trial:test-1:0",
                    "experiment_id": "exp:test-1",
                    "trial_index": 0,
                    "status": "complete",
                },
                {}, {},
            ),
            NodeFact(
                "result:test-1:0:accuracy", "result",
                {
                    "result_id": "result:test-1:0:accuracy",
                    "trial_id": "trial:test-1:0",
                    "metric_name": "accuracy",
                    "metric_value": 0.923,
                    "metric_unit": "%",
                },
                {}, {},
            ),
            EdgeFact(
                "exp:test-1",
                "trial:test-1:0",
                "contains",
                source_record_id={},
                source_revision={},
            ),
            EdgeFact(
                "trial:test-1:0",
                "result:test-1:0:accuracy",
                "produces",
                source_record_id={},
                source_revision={},
            ),
        ])
    finally:
        store.close()

    # Real publish cycle.
    pub = Publisher(clean_runtime, "test", catalog_identity=bind_test_catalog_writer(clean_runtime, "test"))
    out = pub.cycle()
    assert out["status"] == "published", f"Publish failed: {out}"

    store = ChronosGraphStore.connect(clean_runtime, ensure_schema=False)
    try:
        generation = store.checkout_generation(out["generation_id"])
        subtype_counts = {
            row["subtype"]: row["count"]
            for row in generation.query(
                """
                SELECT subtype, COUNT(*) AS count
                  FROM okg.graph_nodes
                 WHERE subtype IN ('experiment', 'trial', 'result')
                 GROUP BY subtype
                """
            )
        }
        assert subtype_counts == {
            "experiment": 1, "trial": 1, "result": 1,
        }, f"Expected one of each subtype, got {subtype_counts}"

        edge_counts = {
            row["edge_type"]: row["count"]
            for row in generation.query(
                """
                SELECT edge_type, COUNT(*) AS count
                  FROM okg.graph_edges
                 WHERE edge_type IN ('contains', 'produces')
                   AND (src LIKE 'exp:%' OR src LIKE 'trial:%')
                 GROUP BY edge_type
                """
            )
        }
        assert edge_counts.get("contains") == 1, edge_counts
        assert edge_counts.get("produces") == 1, edge_counts
    finally:
        store.close()
