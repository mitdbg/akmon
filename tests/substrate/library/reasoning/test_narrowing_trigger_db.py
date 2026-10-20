"""DB-backed narrowing-conformance tests for the reasoning module.

Verifies that with the reasoning module's narrowings actually
installed in a real Postgres catalog:

  - Permitted reasoning ring triples are NOT flagged as narrowing
    violations.
  - Forbidden triples (right subtype family, wrong direction or
    wrong edge type) ARE flagged as narrowing violations.
  - New semantic labels (cites, predicts, instantiates) are classified
    correctly.
  - The cross-module narrowing `Hypothesis supported_by
    AcceptanceCriterion` is honored.

The chronos-native cutover dropped okg.nodes_live / okg.edges_live and
the inline `edges_live_narrowing_check` trigger that used to reject a
forbidden triple at write time. Narrowing is now enforced by the Chronos
publish gate (TransformConsistencyGate), which classifies each edge in a
checkpointed generation against the active catalog narrowings. So these
tests seed the Chronos graph via ChronosGraphStore.apply_facts on a
per-test branch, checkpoint the generation, and evaluate the gate:
a permitted triple is absent from narrowing_violation_edge_ids; a
forbidden triple is present.

Requires an isolated test DB (OKG_TEST_DSN). The shared
`okg_migrated` / `clean_runtime` fixtures in tests/substrate/conftest.py
handle DB provisioning, schema migration, and per-test truncation.
"""
from __future__ import annotations

import uuid

import psycopg
import pytest
import yaml

from okg.substrate.catalog.modules_apply import apply_composed_catalog
from okg.substrate.catalog.modules_compose import compose_catalog
from okg.substrate.consistency import TransformConsistencyGate
from okg.substrate.graph_store import ChronosGraphStore, _edge_id
from okg.substrate.library.sources.base import EdgeFact, NodeFact
from tests.pytest.chronos_reset import reset_chronos_graph


pytestmark = pytest.mark.postgres


# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def reasoning_catalog_base(
    okg_migrated: str, tmp_path_factory: pytest.TempPathFactory,
) -> tuple[str, int]:
    """Compose + apply a catalog containing the reasoning module
    (plus git_graph, openspec, agent_memory for cross-module
    narrowing dependencies) and mark it `applied` so the narrowing
    trigger activates.

    Returns ``(dsn, catalog_version_id)``.
    """
    reset_chronos_graph(okg_migrated)
    dep = tmp_path_factory.mktemp("test-reasoning-dep")
    (dep / "deployment.yaml").write_text(yaml.safe_dump({
        "name": "test-reasoning-dep",
        "description": "reasoning DB trigger smoke",
        "modules": [
            "git_graph",
            "openspec",
            "agent_memory",
            "reasoning",
        ],
    }))
    (dep / "schemas").mkdir()
    composed = compose_catalog(dep)

    with psycopg.connect(okg_migrated, autocommit=False) as conn:
        cv = apply_composed_catalog(conn, composed)
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE okg.catalog_versions "
                "   SET status = 'applied', applied_at = now() "
                " WHERE catalog_version_id = %s",
                (cv,),
            )
        conn.commit()

    return okg_migrated, cv


@pytest.fixture
def reasoning_catalog_cv(
    reasoning_catalog_base: tuple[str, int],
) -> tuple[str, int]:
    dsn, cv = reasoning_catalog_base
    reset_chronos_graph(dsn)
    return dsn, cv


def _is_narrowing_violation(
    dsn: str,
    cv: int,
    nodes: list[tuple[str, str]],
    edge: tuple[str, str, str],
) -> bool:
    """Apply `nodes` + the single `edge` to a fresh Chronos branch,
    checkpoint the generation, run the publish-time narrowing gate, and
    report whether the edge is flagged as a narrowing violation.

    Both endpoints are always seeded, so the only thing the gate can flag
    is the (src_subtype, edge_type, dst_subtype) triple against the active
    catalog narrowings — exactly what the retired inline trigger enforced.
    """
    src, dst, etype = edge
    sfx = uuid.uuid4().hex[:8]
    branch = f"narrow-{sfx}"
    with psycopg.connect(dsn, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn)
        store.ensure_branch(branch, from_branch="main")
        store.apply_facts(
            [NodeFact(nid, subtype, {"text": nid}, {"id": nid}, {"v": 1})
             for nid, subtype in nodes]
            + [EdgeFact(src, dst, etype,
                        source_record_id={"id": f"{src}->{dst}"},
                        source_revision={"v": 1})],
            branch=branch,
            strict_endpoints=True,
        )
        gen = store.checkpoint_generation(
            deployment_name="reasoning-test", branch=branch,
            catalog_version_id=cv, metadata={"correlation_id": f"reasoning:{sfx}"},
        )

    with psycopg.connect(dsn, autocommit=True) as conn:
        ev = TransformConsistencyGate().evaluate(
            conn, target_generation_id=gen, catalog_version_id=cv,
            deployment_name="reasoning-test",
        )
    # The reasoning-library catalog carries no at_most_one_target annotation,
    # so the cardinality family is inactive and the runtime basis discloses
    # the skip instead of naming a check that did not run.
    assert ev.details["runtime"] == (
        "chronos_transform_ir+skipped_families:cardinality"
    )
    return _edge_id(src, etype, dst) in ev.details["narrowing_violation_edge_ids"]


def _assert_accepted(dsn, cv, nodes, edge) -> None:
    assert not _is_narrowing_violation(dsn, cv, nodes, edge), (
        f"permitted triple {edge} should NOT be a narrowing violation"
    )


def _assert_rejected(dsn, cv, nodes, edge) -> None:
    assert _is_narrowing_violation(dsn, cv, nodes, edge), (
        f"forbidden triple {edge} SHALL be flagged as a narrowing violation"
    )


# ---------------------------------------------------------------------------
# Permitted triples — each ring edge SHALL pass the narrowing gate
# ---------------------------------------------------------------------------


def test_hypothesis_supported_by_observation_accepted(
    reasoning_catalog_cv: tuple[str, int],
) -> None:
    dsn, cv = reasoning_catalog_cv
    _assert_accepted(
        dsn, cv,
        [("hyp:h1", "hypothesis"), ("obs:o1", "observation")],
        ("hyp:h1", "obs:o1", "supported_by"),
    )


def test_hypothesis_contradicted_by_observation_accepted(
    reasoning_catalog_cv: tuple[str, int],
) -> None:
    dsn, cv = reasoning_catalog_cv
    _assert_accepted(
        dsn, cv,
        [("hyp:h2", "hypothesis"), ("obs:o2", "observation")],
        ("hyp:h2", "obs:o2", "contradicted_by"),
    )


def test_observation_instantiates_principle_accepted(
    reasoning_catalog_cv: tuple[str, int],
) -> None:
    dsn, cv = reasoning_catalog_cv
    _assert_accepted(
        dsn, cv,
        [("obs:o3", "observation"), ("princ:p1", "principle")],
        ("obs:o3", "princ:p1", "instantiates"),
    )


def test_principle_predicts_hypothesis_accepted(
    reasoning_catalog_cv: tuple[str, int],
) -> None:
    dsn, cv = reasoning_catalog_cv
    _assert_accepted(
        dsn, cv,
        [("princ:p2", "principle"), ("hyp:h3", "hypothesis")],
        ("princ:p2", "hyp:h3", "predicts"),
    )


def test_hypothesis_cites_principle_accepted(
    reasoning_catalog_cv: tuple[str, int],
) -> None:
    dsn, cv = reasoning_catalog_cv
    _assert_accepted(
        dsn, cv,
        [("hyp:h4", "hypothesis"), ("princ:p3", "principle")],
        ("hyp:h4", "princ:p3", "cites"),
    )


def test_hypothesis_supported_by_acceptance_criterion_accepted(
    reasoning_catalog_cv: tuple[str, int],
) -> None:
    """Cross-module narrowing (Pattern B target) from openspec into
    reasoning."""
    dsn, cv = reasoning_catalog_cv
    _assert_accepted(
        dsn, cv,
        [("hyp:h5", "hypothesis"), ("crit:c1", "acceptance_criterion")],
        ("hyp:h5", "crit:c1", "supported_by"),
    )


def test_agent_observation_references_observation_accepted(
    reasoning_catalog_cv: tuple[str, int],
) -> None:
    """Cross-module narrowing for the optional
    EpistemicObservationEnricher."""
    dsn, cv = reasoning_catalog_cv
    _assert_accepted(
        dsn, cv,
        [("agent_obs:ao1", "agent_observation"),
         ("obs:candidate-abc123", "observation")],
        ("agent_obs:ao1", "obs:candidate-abc123", "references"),
    )


def test_insight_references_connection_accepted(
    reasoning_catalog_cv: tuple[str, int],
) -> None:
    dsn, cv = reasoning_catalog_cv
    _assert_accepted(
        dsn, cv,
        [("insight:i1", "insight"), ("connect:c1", "connection")],
        ("insight:i1", "connect:c1", "references"),
    )


def test_insight_cites_observation_accepted(
    reasoning_catalog_cv: tuple[str, int],
) -> None:
    dsn, cv = reasoning_catalog_cv
    _assert_accepted(
        dsn, cv,
        [("insight:i2", "insight"), ("obs:o4", "observation")],
        ("insight:i2", "obs:o4", "cites"),
    )


def test_hypothesis_supersedes_hypothesis_accepted(
    reasoning_catalog_cv: tuple[str, int],
) -> None:
    dsn, cv = reasoning_catalog_cv
    _assert_accepted(
        dsn, cv,
        [("hyp:h_new", "hypothesis"), ("hyp:h_old", "hypothesis")],
        ("hyp:h_new", "hyp:h_old", "supersedes"),
    )


# ---------------------------------------------------------------------------
# Forbidden triples — the narrowing gate SHALL flag a violation
# ---------------------------------------------------------------------------


def test_observation_supported_by_hypothesis_rejected(
    reasoning_catalog_cv: tuple[str, int],
) -> None:
    """Reversed direction: Observation SHALL NOT be `supported_by`
    a Hypothesis. The narrowing has src=Hypothesis, dst=Observation,
    not the reverse.
    """
    dsn, cv = reasoning_catalog_cv
    _assert_rejected(
        dsn, cv,
        [("hyp:hbad1", "hypothesis"), ("obs:obad1", "observation")],
        ("obs:obad1", "hyp:hbad1", "supported_by"),
    )


def test_principle_supported_by_observation_rejected(
    reasoning_catalog_cv: tuple[str, int],
) -> None:
    """Principle is not `supported_by` an Observation — Observation
    instantiates Principle (different edge type), or Hypothesis is
    supported_by Observation.
    """
    dsn, cv = reasoning_catalog_cv
    _assert_rejected(
        dsn, cv,
        [("princ:pbad1", "principle"), ("obs:obad2", "observation")],
        ("princ:pbad1", "obs:obad2", "supported_by"),
    )


def test_hypothesis_instantiates_principle_rejected(
    reasoning_catalog_cv: tuple[str, int],
) -> None:
    """`instantiates` is from Observation, not from Hypothesis."""
    dsn, cv = reasoning_catalog_cv
    _assert_rejected(
        dsn, cv,
        [("hyp:hbad2", "hypothesis"), ("princ:pbad2", "principle")],
        ("hyp:hbad2", "princ:pbad2", "instantiates"),
    )


def test_acceptance_criterion_supports_hypothesis_rejected(
    reasoning_catalog_cv: tuple[str, int],
) -> None:
    """Direction matters: the narrowing is `Hypothesis supported_by
    AcceptanceCriterion`, NOT `AcceptanceCriterion supported_by
    Hypothesis`. The reversed triple SHALL be flagged.
    """
    dsn, cv = reasoning_catalog_cv
    _assert_rejected(
        dsn, cv,
        [("crit:cbad1", "acceptance_criterion"), ("hyp:hbad3", "hypothesis")],
        ("crit:cbad1", "hyp:hbad3", "supported_by"),
    )


def test_observation_cites_observation_rejected(
    reasoning_catalog_cv: tuple[str, int],
) -> None:
    """The `cites` narrowings are Hypothesis→Principle and
    Insight→Observation. Observation→Observation `cites` is not in
    the catalog and SHALL be flagged."""
    dsn, cv = reasoning_catalog_cv
    _assert_rejected(
        dsn, cv,
        [("obs:obad3", "observation"), ("obs:obad4", "observation")],
        ("obs:obad3", "obs:obad4", "cites"),
    )
