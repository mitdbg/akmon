"""Ring-0 fixture witnesses (task.bench.ring0-fixture).

A REAL publish cycle over the checked-in fixture corpus
(deployments/consistency-injection-fixture/corpus/), ingested by the
deterministic ClaimCorpusSource through its SourceAdapter contract:

* the generation publishes (status ``published``, not blocked);
* okg.claim_effective holds nonzero claim rows for the pinned generation,
  and the injector's ClaimSnapshot loads them with corpus document mapping;
* the baseline finding set B0 is recorded and is EXACTLY the one authored
  inconsistency (ops_legacy.md Version 3.9 vs 4.2 → contested version
  bucket) — the deliberately-conditional burst timeout does NOT flag
  (distinct heading-path scopes are disjoint);
* a second clean publish is finding-stable: B0 reproduces and the claim
  counts are unchanged.

Seeding + publish wiring follow tests/substrate/consistency/test_claim_plane_wire.py.
"""

from __future__ import annotations

import uuid
from pathlib import Path
from typing import Any

import pytest

psycopg = pytest.importorskip("psycopg")

import yaml  # noqa: E402

from okg.substrate.catalog.modules_apply import apply_composed_catalog  # noqa: E402
from okg.substrate.catalog.modules_compose import compose_catalog  # noqa: E402
from okg.bench.injection import ClaimSnapshot  # noqa: E402
from okg.bench.injection.claim_corpus_source import (  # noqa: E402
    ClaimCorpusSource,
)
from okg.substrate.derived_artifacts.claim_projection_derivation import (  # noqa: E402
    bucket_row_key,
)
from okg.substrate.graph_store import ChronosGraphStore  # noqa: E402
from okg.substrate.publisher.factory import build_deployment_publisher  # noqa: E402

_DEPLOYMENT = "consistency-injection-fixture"
_CHECK_ID = "consistency-injection-fixture.ir.claim_state.contested_bucket.v1"
_REPO_ROOT = Path(__file__).resolve().parents[3]
_DEP_DIR = _REPO_ROOT / "deployments" / "consistency-injection-fixture"
_CORPUS_DIR = _DEP_DIR / "corpus"

_POLICIES = (
    "backup_node",
    "certified",
    "endpoint",
    "memory",
    "owner",
    "primary_node",
    "released",
    "retry_limit",
    "reviewed",
    "runbook",
    "status",
    "step",
    "timeout_s",
    "upgrade",
    "uptime",
    "version",
)


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
    from tests.pytest.catalog_fixture_builders import apply_explicit_test_catalog
    apply_explicit_test_catalog(dsn, _DEP_DIR)


def _publish(dsn: str) -> dict[str, Any]:
    out = build_deployment_publisher(
        dsn=dsn,
        deployment_name=_DEPLOYMENT,
        deployment_dir=_DEP_DIR,
        command="pytest injection ring-0",
    ).cycle()
    assert out["status"] == "published", out
    return out


def _effective_claim_count(dsn: str, generation_id: Any) -> int:
    with psycopg.connect(dsn, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
        checkout = store.checkout_generation(generation_id)
        rows = checkout.query(
            "SELECT count(*) AS n FROM okg.claim_effective WHERE row_kind = :k",
            {"k": "effective_claim"},
        )
        return int(rows[0]["n"])


def _open_findings(dsn: str) -> set[str]:
    with psycopg.connect(dsn, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT participant_roles->>'bucket'
                  FROM okg.observation_current
                 WHERE check_id = %s AND finding_status = 'open'
                """,
                (_CHECK_ID,),
            )
            return {row[0] for row in cur.fetchall()}


def _bucket_resolution(dsn: str, generation: str, bucket: str) -> tuple[str, str]:
    """(outcome, basis) of one bucket's resolution row.

    Read through a generation CHECKOUT: ``okg.claim_projection`` is
    chronos-versioned, so a bare SELECT outside one returns no rows and any
    assertion against it would be vacuous.
    """
    with psycopg.connect(dsn, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
        checkout = store.checkout_generation(generation)
        rows = list(checkout.query(
            "SELECT outcome, basis FROM okg.claim_projection "
            "WHERE row_kind = 'bucket_resolution' AND row_key = :k",
            {"k": bucket},
        ))
    assert len(rows) == 1, rows
    return rows[0]["outcome"], rows[0]["basis"]


@pytest.mark.postgres
def test_ring0_fixture_publishes_claims_and_stable_b0(
    clean_runtime: str, tmp_path: Path
) -> None:
    _install_composed_catalog(clean_runtime, tmp_path)

    source = ClaimCorpusSource(repo_root=_CORPUS_DIR, policies=_POLICIES)
    facts = list(source.run(f"ring0:{uuid.uuid4().hex[:8]}").facts)
    claim_facts = [fact for fact in facts if fact.subtype == "consistency_claim"]
    policy_facts = [fact for fact in facts if fact.subtype == "predicate_policy"]
    assert len(claim_facts) >= 30, len(claim_facts)
    assert len(policy_facts) == len(_POLICIES)
    # The deterministic extractor stamps the corpus mapping + heading scopes.
    by_id = {fact.node_id: fact for fact in claim_facts}
    version_alpha = by_id["claim:ops_alpha.md:version:alpha_service:0"]
    assert version_alpha.attrs["value"] == "4.2"
    assert version_alpha.attrs["scope"] == "Alpha Service > Deployment"
    assert version_alpha.attrs["source_id"] == "ops_alpha.md"

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn, ensure_schema=True)
        store.apply_facts(facts, source=_DEPLOYMENT, strict_endpoints=False)

    out1 = _publish(clean_runtime)
    generation_1 = out1["generation_id"]
    print(f"\n[ring0] published generation_id={generation_1} "
          f"compat_generation_id={out1['compat_generation_id']}")

    n_claims_1 = _effective_claim_count(clean_runtime, generation_1)
    print(f"[ring0] okg.claim_effective effective_claim rows={n_claims_1}")
    assert n_claims_1 >= 30, n_claims_1

    # ---- B0: the authored inconsistency RESOLVES rather than staying open.
    #
    # version/alpha_service is 4.2 (ops_alpha.md + ops_mirror.md) against 3.9
    # (ops_legacy.md), so bucket-level corroboration settles it 2-vs-1 on the
    # current value and the finding closes. Before corroboration was computed
    # this bucket could never settle, and B0 was {version/alpha_service}.
    #
    # This fixture is also the repo's most concrete instance of the residual
    # risk: ops_mirror.md is a near-verbatim COPY of ops_alpha.md, and v1
    # counts it as an independent source. The outcome is right here only
    # because the copy carries the correct value — a mirror of ops_legacy.md
    # would have carried 3.9 to a 2-vs-1 win over the correct original. That
    # is the gap the signal's own name ("dependency_adjusted_support" —
    # copies count once) promises and v1 does not implement.
    expected_bucket = bucket_row_key("version", "alpha_service")
    b0 = _open_findings(clean_runtime)
    print(f"[ring0] B0 open findings (buckets)={sorted(b0)}")
    assert b0 == set(), b0
    # Assert the MECHANISM, not just the empty set: an empty B0 would also be
    # produced by the check silently failing to run at all. The bucket must
    # carry a resolution that settled ON CORROBORATION.
    assert _bucket_resolution(
        clean_runtime, generation_1, expected_bucket
    ) == ("canonical", "dependency_adjusted_support")

    # ---- The injector's targeting snapshot loads the SAME generation.
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
        checkout = store.checkout_generation(generation_1)
        snapshot = ClaimSnapshot.from_checkout(checkout, str(generation_1))
    assert len(snapshot.claims) == n_claims_1
    version_claims = [
        claim for claim in snapshot.claims
        if claim.predicate == "version" and claim.normalized_key == "alpha_service"
    ]
    assert {claim.value for claim in version_claims} == {"4.2", "3.9"}
    assert {claim.source_doc for claim in version_claims} == {
        "ops_alpha.md", "ops_mirror.md", "ops_legacy.md",
    }
    assert all(claim.scope == "Alpha Service > Deployment" for claim in version_claims)

    # ---- Second clean publish: finding-stable, claim counts unchanged. An
    # unchanged cycle may reuse the same generation checkpoint (idempotent
    # no-op publish) — either way the published state must reproduce B0.
    out2 = _publish(clean_runtime)
    generation_2 = out2["generation_id"]
    n_claims_2 = _effective_claim_count(clean_runtime, generation_2)
    b0_again = _open_findings(clean_runtime)
    print(f"[ring0] second publish generation_id={generation_2} "
          f"(reused={generation_2 == generation_1}) rows={n_claims_2} "
          f"findings={sorted(b0_again)}")
    assert n_claims_2 == n_claims_1
    assert b0_again == b0
