"""One resolution clears a dependent violation, end to end — detection reads
resolution across the former plane boundary.

PACT ``consistency-claim-graph-binding`` (task.binding.end-to-end,
req.binding.end-to-end-witness). The motivating scenario on the REAL
``consistency-fixture`` deployment stack (``build_deployment_publisher_stack`` —
deployment.yaml wires the claim derivations + the dependent claim-state IR
check; this test does not hand-assemble them), across TWO publishes:

* Publish 1 seeds a lone crawl-source claim whose preferred_target is a RETIRED
  endpoint. With no competitor it resolves canonical (sole claim, no
  bucket_resolution row), and the dependent check ``canonical_retired_target``
  — whose violation depends on WHICH claim is canonical — opens a finding.
* Publish 2 adds a registry-source claim with decisive ``source_authority``.
  Precedence flips the bucket canonical to the registry claim; the next
  evaluation reports the dependent violation RESOLVED with ZERO operator
  corrections. Every intermediate state is pinned by assertion: the conflict
  finding at publish 2, the bucket_resolution flip (winner + basis +
  superseded), the okg.repair_selections retraction of the crawl claim, and the
  observation lifecycle open (gen 1) -> resolved (gen 2).

Falsifiability: a variant that flips the authority signals so the crawl claim
STAYS canonical leaves the violation OPEN — the witness fails if the seam
breaks, and names it.

Seeding + publish wiring follow tests/substrate/consistency/test_claim_plane_wire.py.
"""
from __future__ import annotations

import json
import uuid
from pathlib import Path
from typing import Any

import pytest

psycopg = pytest.importorskip("psycopg")

import yaml  # noqa: E402

from okg.substrate.catalog.modules_apply import apply_composed_catalog  # noqa: E402
from okg.substrate.catalog.modules_compose import compose_catalog  # noqa: E402
from okg.substrate.consistency.claim_model.precedence import (  # noqa: E402
    SIGNAL_SOURCE_AUTHORITY,
)
from okg.substrate.derived_artifacts.claim_projection_derivation import (  # noqa: E402
    ROW_KIND_FINDING,
    ROW_KIND_RESOLUTION,
    bucket_row_key,
)
from okg.substrate.graph_store import ChronosGraphStore  # noqa: E402
from okg.substrate.library.sources.base import NodeFact  # noqa: E402
from okg.substrate.publisher import Publisher  # noqa: E402
from okg.substrate.publisher.deployment_stack import (  # noqa: E402
    build_deployment_publisher_stack,
)
from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer  # noqa: E402

_DEPLOYMENT = "consistency-fixture"
_CHECK_ID = "consistency-fixture.ir.claim_state.canonical_retired_target.v1"
_PREDICATE = "preferred_target"
_CONSTRAINT = "unique_preferred"
_SCOPE = "HCAL/Run3"
_RETIRED = "retired:old-cluster"   # the crawl source's bad extraction
_ACTIVE = "active:new-cluster"     # the registry source's compliant value
_DEP_DIR = Path(__file__).resolve().parents[3] / "deployments" / "consistency-fixture"


def _install_composed_catalog(dsn: str, tmp_path: Path) -> None:
    d = tmp_path / "scratch"
    d.mkdir()
    (d / "deployment.yaml").write_text(yaml.safe_dump({
        "name": "scratch", "schema_dir": "schemas",
        "modules": ["extraction", "consistency"],
    }))
    (d / "schemas").mkdir()
    composed = compose_catalog(d)
    with psycopg.connect(dsn, autocommit=True) as conn:
        apply_composed_catalog(conn, composed)


def _apply(dsn: str, facts: list[NodeFact]) -> None:
    with psycopg.connect(dsn, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn, ensure_schema=True)
        store.apply_facts(facts, source=_DEPLOYMENT, strict_endpoints=False)


def _claim_fact(node_id: str, value: str, *, key: str, source: str,
                source_class: str, source_authority: int) -> NodeFact:
    return NodeFact(
        node_id, "consistency_claim",
        {
            "predicate": _PREDICATE, "normalized_key": key, "value": value,
            "scope": _SCOPE, "scope_state": "known",
            "claim_provenance": "derived", "source_id": source,
            "source_class": source_class,
            "source_authority": str(source_authority),
        },
        {"id": node_id}, {"v": 1},
    )


def _functional_policy(node_id: str, *, rev: int = 1, **extra: Any) -> NodeFact:
    attrs = {"enabled": "true", "policy_id": _CONSTRAINT,
             "policy_kind": "functional", "predicate": _PREDICATE}
    attrs.update(extra)
    return NodeFact(node_id, "predicate_policy", attrs, {"id": node_id}, {"v": rev})


def _publish(dsn: str) -> dict[str, Any]:
    stack = build_deployment_publisher_stack(
        dsn=dsn, deployment_name=_DEPLOYMENT, deployment_dir=_DEP_DIR,
    )
    out = Publisher(dsn, _DEPLOYMENT, **stack.publisher_kwargs(), catalog_identity=bind_test_catalog_writer(dsn, _DEPLOYMENT)).cycle()
    assert out["status"] == "published", out
    return out


def _resolution_row(dsn: str, generation_id: Any, rk: str) -> tuple[Any, ...] | None:
    with psycopg.connect(dsn, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
        checkout = store.checkout_generation(generation_id)
        rows = checkout.query(
            "SELECT outcome, winner_claim_id, basis, ranked_claim_ids, "
            "superseded_claim_ids FROM okg.claim_projection "
            "WHERE row_kind = :k AND row_key = :rk",
            {"k": ROW_KIND_RESOLUTION, "rk": rk},
        )
        return dict(rows[0]) if rows else None


def _finding_count(dsn: str, generation_id: Any) -> int:
    with psycopg.connect(dsn, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
        checkout = store.checkout_generation(generation_id)
        rows = checkout.query(
            "SELECT count(*) AS n FROM okg.claim_projection WHERE row_kind = :k",
            {"k": ROW_KIND_FINDING},
        )
        return int(rows[0]["n"])


def _observation(dsn: str, bucket: str) -> dict[str, Any] | None:
    with psycopg.connect(dsn, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT observation_id, finding_status
                  FROM okg.observation_current
                 WHERE check_id = %s AND participant_roles->>'bucket' = %s
                """,
                (_CHECK_ID, bucket),
            )
            row = cur.fetchone()
    if row is None:
        return None
    return {"observation_id": row[0], "finding_status": row[1]}


def _observation_events(dsn: str) -> list[str]:
    """The finding-event history for the dependent check, ordered by event_seq."""
    with psycopg.connect(dsn, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT finding_event FROM okg.observation_events
                 WHERE check_id = %s ORDER BY event_seq
                """,
                (_CHECK_ID,),
            )
            return [r[0] for r in cur.fetchall()]


def _repair_selection(dsn: str, compat_gen: str, bucket_key: str) -> tuple[Any, ...] | None:
    with psycopg.connect(dsn, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT outcome, winner_claim_id, generator_id,
                       distrusted_claim_ids, unresolved_reason
                  FROM okg.repair_selections
                 WHERE deployment_name = %s AND generation_id = %s AND bucket_key = %s
                """,
                (_DEPLOYMENT, compat_gen, bucket_key),
            )
            return cur.fetchone()


@pytest.mark.postgres
def test_resolution_clears_dependent_violation(
    clean_runtime: str, tmp_path: Path
) -> None:
    _install_composed_catalog(clean_runtime, tmp_path)
    sfx = uuid.uuid4().hex[:8]
    key = f"pref|{sfx}"
    crawl, registry = f"claim:crawl:{sfx}", f"claim:registry:{sfx}"
    bucket = f"{_PREDICATE}|{key}"                 # the check's participant bucket
    rk = bucket_row_key(_PREDICATE, key)           # bucket_resolution row_key

    # ---- Publish 1: the crawl claim alone -> canonical (sole), violation opens
    _apply(clean_runtime, [
        _claim_fact(crawl, _RETIRED, key=key, source="crawl:page:1",
                    source_class="crawl", source_authority=1),
        _functional_policy(f"pol:{sfx}"),
    ])
    r1 = _publish(clean_runtime)
    gen1 = r1["generation_id"]
    print(f"\n[resolution-clears] publish1 generation_id={gen1}")

    # Sole claim -> no conflict finding, no bucket_resolution row.
    assert _finding_count(clean_runtime, gen1) == 0
    assert _resolution_row(clean_runtime, gen1, rk) is None
    # ...but the dependent check FIRES: the (sole) canonical claim is retired.
    obs1 = _observation(clean_runtime, bucket)
    print(f"[resolution-clears] publish1 observation={obs1}")
    assert obs1 is not None, "dependent check did not open a finding at publish 1"
    assert obs1["finding_status"] == "open"
    obs_id = obs1["observation_id"]

    # ---- Publish 2: registry claim with decisive authority -> canonical flips.
    # The operator re-affirms the uniqueness policy as the authoritative registry
    # source is onboarded; a Σ edit forces a FULL projection recompute so the
    # selected repair (r*) for the now-conflicted bucket is materialized into
    # okg.repair_selections. (The differential path DEFERS selection on
    # bucket-scoped incremental cycles — preserved exactly; carrying it forward
    # is task.repair.differential's obligation, a later child. The resolved state
    # is identical either way: oracle parity.)
    _apply(clean_runtime, [
        _claim_fact(registry, _ACTIVE, key=key, source="registry:rec:1",
                    source_class="registry", source_authority=10),
        _functional_policy(f"pol:{sfx}", rev=2, review_note="registry onboarded"),
    ])
    r2 = _publish(clean_runtime)
    gen2 = r2["generation_id"]
    compat2 = str(r2["compat_generation_id"])
    print(f"[resolution-clears] publish2 generation_id={gen2} compat={compat2}")

    # (a) the conflict finding exists at publish 2 (crawl vs registry, functional).
    assert _finding_count(clean_runtime, gen2) >= 1

    # (b) the bucket_resolution row shows the FLIP: registry wins, crawl superseded.
    res2 = _resolution_row(clean_runtime, gen2, rk)
    print(f"[resolution-clears] publish2 resolution={res2}")
    assert res2 is not None, "expected a bucket_resolution row at publish 2"
    assert res2["outcome"] == "canonical"
    assert res2["winner_claim_id"] == registry
    assert res2["basis"] == SIGNAL_SOURCE_AUTHORITY       # the decisive signal
    superseded = json.loads(res2["superseded_claim_ids"])
    ranked = json.loads(res2["ranked_claim_ids"])
    assert superseded == [crawl]                          # crawl superseded
    assert crawl in ranked and registry in ranked         # source-preserving

    # (c) okg.repair_selections records the RETRACTION of the crawl claim.
    sel = _repair_selection(clean_runtime, compat2, rk)
    print(f"[resolution-clears] publish2 repair_selection={sel}")
    assert sel is not None, "expected a repair_selections row for the bucket"
    outcome, winner, generator_id, distrusted, unresolved_reason = sel
    assert outcome == "selected"
    assert winner == registry
    assert generator_id == "precedence-retraction"
    assert list(distrusted) == [crawl]                    # crawl retracted
    assert unresolved_reason is None

    # (d) the dependent violation is RESOLVED with ZERO operator corrections
    #     (no claim_correction node was ever seeded) — the same observation id.
    obs2 = _observation(clean_runtime, bucket)
    print(f"[resolution-clears] publish2 observation={obs2}")
    assert obs2 is not None
    assert obs2["observation_id"] == obs_id               # same finding identity
    assert obs2["finding_status"] == "resolved"

    # observation lifecycle: observed (gen 1) then resolved (gen 2).
    events = _observation_events(clean_runtime)
    print(f"[resolution-clears] observation events={events}")
    assert events == ["observed", "resolved"], events


@pytest.mark.postgres
def test_falsifiability_crawl_stays_canonical_keeps_violation_open(
    clean_runtime: str, tmp_path: Path
) -> None:
    """The seam's falsifiability: flip the authority signals so the CRAWL claim
    stays canonical at publish 2. Detection still reads resolution — and because
    the canonical claim is STILL the retired one, the dependent violation stays
    OPEN. If this test ever passed with a resolved finding, the detection-reads-
    resolution seam would be broken (a false clear)."""
    _install_composed_catalog(clean_runtime, tmp_path)
    sfx = uuid.uuid4().hex[:8]
    key = f"pref|{sfx}"
    crawl, registry = f"claim:crawl:{sfx}", f"claim:registry:{sfx}"
    bucket = f"{_PREDICATE}|{key}"
    rk = bucket_row_key(_PREDICATE, key)

    _apply(clean_runtime, [
        _claim_fact(crawl, _RETIRED, key=key, source="crawl:page:1",
                    source_class="crawl", source_authority=10),   # crawl DECISIVE
        _functional_policy(f"pol:{sfx}"),
    ])
    _publish(clean_runtime)
    obs1 = _observation(clean_runtime, bucket)
    assert obs1 is not None and obs1["finding_status"] == "open", obs1

    # Registry claim with WEAKER authority -> loses; crawl stays canonical.
    _apply(clean_runtime, [
        _claim_fact(registry, _ACTIVE, key=key, source="registry:rec:1",
                    source_class="registry", source_authority=1),
    ])
    r2 = _publish(clean_runtime)
    gen2 = r2["generation_id"]

    # The resolution keeps the crawl claim canonical (the retired value wins).
    res2 = _resolution_row(clean_runtime, gen2, rk)
    print(f"\n[falsifiability] resolution={res2}")
    assert res2 is not None
    assert res2["outcome"] == "canonical"
    assert res2["winner_claim_id"] == crawl               # crawl STILL canonical

    # ...so the dependent violation stays OPEN (never a false clear).
    obs2 = _observation(clean_runtime, bucket)
    print(f"[falsifiability] observation={obs2}")
    assert obs2 is not None
    assert obs2["finding_status"] == "open", (
        "crawl stayed canonical yet the dependent violation was cleared — the "
        "detection-reads-resolution seam is broken"
    )
    events = _observation_events(clean_runtime)
    assert "resolved" not in events, events
