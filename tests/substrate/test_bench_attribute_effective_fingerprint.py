"""rebuild-proof-attribute-effective-digest (Task 1 / live verification).

Integration proof that the bench rebuild-equivalence attribute fingerprint
``bench.orchestrator._attribute_effective_fingerprint`` works against a REAL
published generation whose ``attribute_effective_view`` was materialized from
real attribute observations: count > 0 (non-vacuous, requirement
bench-emits-attributes) and deterministic, validated against the live schema and
materialization path -- not a fixture stub.

The volatile/partition-key exclusion (``materialized_at`` + ``generation_id``)
and the digest's divergence-discrimination are proven as fast unit tests in
``tests/bench/test_differential_replay_equivalence.py``; this test pins the
fingerprint query to the real ``attribute_effective_view`` and demonstrates the
vacuous (no-observation) vs non-vacuous (with-observation) states directly.
"""
from __future__ import annotations

import uuid

import pytest

psycopg = pytest.importorskip("psycopg")

import okg.bench.orchestrator as orchestrator  # noqa: E402
from okg.substrate.attributes import (  # noqa: E402
    materialize_effective_attribute_view_for_generation,
    materialize_effective_attributes_for_generation,
    record_attribute_observation,
)
from okg.substrate.graph_store import ChronosGraphStore  # noqa: E402
from okg.substrate.library.sources.base import NodeFact  # noqa: E402
from okg.substrate.publisher import Publisher  # noqa: E402
from tests.pytest.catalog_fixture_builders import seed_catalog  # noqa: E402
from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer  # noqa: E402

pytestmark = pytest.mark.postgres

_N = 12
_DEPLOYMENT = "default"


def _seed_published_generation(clean_runtime: str) -> tuple[str, str]:
    suffix = uuid.uuid4().hex[:8]
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog(conn, subtypes=["doc"])
        store = ChronosGraphStore.from_connection(conn)
        store.apply_facts(
            [
                NodeFact(
                    node_id=f"doc:{i}:{suffix}",
                    subtype="doc",
                    attrs={"k": f"v{i}"},
                    source_record_id={"id": f"doc:{i}:{suffix}"},
                    source_revision={"v": 1},
                )
                for i in range(_N)
            ],
            source="default",
            strict_endpoints=True,
        )
    return str(Publisher(clean_runtime, _DEPLOYMENT, catalog_identity=bind_test_catalog_writer(clean_runtime, _DEPLOYMENT)).cycle()["generation_id"]), suffix


def test_attribute_effective_fingerprint_nonvacuous_on_real_published_generation(
    clean_runtime: str,
) -> None:
    gen, suffix = _seed_published_generation(clean_runtime)

    # FINDING (from running this against the real publish path): the effective-
    # attribute view is NOT empty without observations -- the publish
    # materializes a raw-attribute row per node (attrs == raw graph attrs,
    # overlay empty). So requirement bench-emits-attributes is satisfied by raw
    # attrs alone: count > 0, and the rebuild-proof digest is non-vacuous out of
    # the box (it compares the per-node effective-attribute snapshot, not 0==0).
    # The only way the view goes empty is the empty-overlay fast path, so a
    # --prove-rebuild-equivalence experiment must not run that fast path.
    base = orchestrator._attribute_effective_fingerprint(clean_runtime, gen)
    assert base["present"] is True
    # The publish folded the generation into the interval table (the default),
    # so the proof reads the interval stab, not the changed-only snapshot
    # (issue #1539, decision 92a).
    assert base["representation"] == "interval", base
    assert base["count"] == _N, base
    assert base["digest"].startswith("sha256:")

    # Deterministic: re-reading the same materialized view digests identically,
    # so an incremental==rebuild comparison of this surface is meaningful.
    again = orchestrator._attribute_effective_fingerprint(clean_runtime, gen)
    assert again["digest"] == base["digest"]

    # An observation OVERLAY changes effective state: materialize the SAME graph
    # with 5 attribute observations under a derived overlay generation. The
    # fingerprint tracks it -- same node count, but a different digest -- proving
    # the surface reflects effective-attribute state (the thing the incremental
    # engine must reproduce), not merely node identity.
    overlay_gen = f"{gen}:ov"
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        for i in range(5):
            record_attribute_observation(
                conn,
                target_kind="node",
                target_id=f"doc:{i}:{suffix}",
                generation_id=overlay_gen,
                attr_key="bench_label",
                value=f"label-{i}",
                producer="bench:attr-emit",
            )
        materialize_effective_attributes_for_generation(
            conn, generation_id=overlay_gen
        )
        materialize_effective_attribute_view_for_generation(
            conn,
            generation_id=overlay_gen,
            previous_generation_id=None,
            empty_overlay_fast_path=False,
        )

    # The overlay generation has no checkpoint (it was materialized directly,
    # not published), so its surface is named explicitly: "auto" refuses it.
    with pytest.raises(ValueError, match="effective_interval_view_sync"):
        orchestrator._attribute_effective_fingerprint(clean_runtime, overlay_gen)
    overlaid = orchestrator._attribute_effective_fingerprint(
        clean_runtime, overlay_gen, representation="snapshot"
    )
    assert overlaid["count"] > 0, overlaid
    assert overlaid["digest"] != base["digest"]


def test_attribute_fingerprint_reads_interval_state_not_changed_only_snapshot(
    clean_runtime: str,
) -> None:
    """Decision 92a: after a second publish that changes ONE node, the second
    generation's snapshot holds only that node, while its full effective state
    (every node) lives in the interval table. The proof must read the full
    state; on the pre-fix bench it read the snapshot and counted 1."""
    gen1, suffix = _seed_published_generation(clean_runtime)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        ChronosGraphStore.from_connection(conn).apply_facts(
            [
                NodeFact(
                    node_id=f"doc:0:{suffix}",
                    subtype="doc",
                    attrs={"k": "changed"},
                    source_record_id={"id": f"doc:0:{suffix}"},
                    source_revision={"v": 2},
                )
            ],
            source="default",
            strict_endpoints=True,
        )
    gen2 = str(
        Publisher(
            clean_runtime,
            _DEPLOYMENT,
            catalog_identity=bind_test_catalog_writer(clean_runtime, _DEPLOYMENT),
        ).cycle()["generation_id"]
    )
    assert gen2 != gen1

    with psycopg.connect(clean_runtime, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT count(*) FROM okg.attribute_effective_view WHERE generation_id = %s",
            (gen2,),
        )
        snapshot_rows = cur.fetchone()[0]
    assert snapshot_rows < _N, snapshot_rows  # changed-only snapshot

    full = orchestrator._attribute_effective_fingerprint(clean_runtime, gen2)
    assert full["count"] == _N, full
    assert full["representation"] == "interval", full
    forced_snapshot = orchestrator._attribute_effective_fingerprint(
        clean_runtime, gen2, representation="snapshot"
    )
    assert forced_snapshot["count"] == snapshot_rows
