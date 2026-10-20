"""The first live claim-plane publish — derivations run, selection materializes,
detection reads resolution.

PACTs ``consistency-claim-graph-binding`` (task.binding.wire-deployment,
req.binding.derivations-live) and ``consistency-repair-objective``
(task.repair.materialize, req.repair.executed-cleaned-state). A REAL publish
cycle built from the in-tree ``consistency-fixture`` deployment stack
(``build_deployment_publisher_stack`` — deployment.yaml wires the maintained
derivations + the claim-state IR check, this test does not hand-assemble them):

* reports ``published``;
* the claim_effective / claim_projection kept tables hold rows for the pinned
  generation (counts > 0, asserted AND printed);
* okg.repair_selections holds the recorded selection r* for the generation — the
  decisive bucket ``selected`` with a basis + distrusted set, the contested
  bucket ``unresolved`` with reason contested; the loser stays present in the
  projection (source preservation);
* the claim-state IR check RAN as the read-side consumer (its coverage row
  records the claim-state surface AND it opened a finding on the contested
  bucket) — registration alone proves nothing.

Seeding + publish wiring follow tests/substrate/consistency/test_claim_state_surface.py
and tests/substrate/consistency/test_finding_identity.py.
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
_CHECK_ID = "consistency-fixture.ir.claim_state.contested_bucket.v1"
_PREDICATE = "preferred_target"
_CONSTRAINT = "unique_preferred"
_SCOPE = "HCAL/Run3"
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
                source_class: str, **signals: Any) -> NodeFact:
    attrs = {
        "predicate": _PREDICATE, "normalized_key": key, "value": value,
        "scope": _SCOPE, "scope_state": "known",
        "claim_provenance": "derived", "source_id": source,
        "source_class": source_class,
    }
    attrs.update({k: str(v) for k, v in signals.items()})
    return NodeFact(node_id, "consistency_claim", attrs, {"id": node_id}, {"v": 1})


def _functional_policy(node_id: str) -> NodeFact:
    return NodeFact(
        node_id, "predicate_policy",
        {"enabled": "true", "policy_id": _CONSTRAINT,
         "policy_kind": "functional", "predicate": _PREDICATE},
        {"id": node_id}, {"v": 1},
    )


def _publish(dsn: str) -> dict[str, Any]:
    stack = build_deployment_publisher_stack(
        dsn=dsn, deployment_name=_DEPLOYMENT, deployment_dir=_DEP_DIR,
    )
    out = Publisher(dsn, _DEPLOYMENT, **stack.publisher_kwargs(), catalog_identity=bind_test_catalog_writer(dsn, _DEPLOYMENT)).cycle()
    assert out["status"] == "published", out
    return out


def _kept_count(checkout: Any, table: str, row_kind: str) -> int:
    rows = checkout.query(
        f"SELECT count(*) AS n FROM {table} WHERE row_kind = :k", {"k": row_kind}
    )
    return int(rows[0]["n"])


@pytest.mark.postgres
def test_claim_plane_wire_publishes_materializes_and_is_consumed(
    clean_runtime: str, tmp_path: Path
) -> None:
    _install_composed_catalog(clean_runtime, tmp_path)
    sfx = uuid.uuid4().hex[:8]
    # Decisive bucket key1: A (authority 10) beats B (authority 1) — canonical A,
    # B distrusted. Contested bucket key2: C, D tie on every signal — unresolved.
    a, b = f"claim:a:{sfx}", f"claim:b:{sfx}"
    c, d = f"claim:c:{sfx}", f"claim:d:{sfx}"
    key1, key2 = f"key|one:{sfx}", f"key|two:{sfx}"
    rk1, rk2 = bucket_row_key(_PREDICATE, key1), bucket_row_key(_PREDICATE, key2)

    _apply(clean_runtime, [
        _claim_fact(a, "vX", key=key1, source="page:1",
                    source_class="release_notes", source_authority=10),
        _claim_fact(b, "vY", key=key1, source="page:2",
                    source_class="wiki", source_authority=1),
        _claim_fact(c, "vP", key=key2, source="page:3", source_class="wiki"),
        _claim_fact(d, "vQ", key=key2, source="page:4", source_class="wiki"),
        _functional_policy(f"pol:{sfx}"),
    ])

    out = _publish(clean_runtime)
    generation_id = out["generation_id"]
    compat_generation_id = str(out["compat_generation_id"])
    print(f"\n[claim-plane-wire] published generation_id={generation_id} "
          f"compat_generation_id={compat_generation_id}")

    # ---- kept tables hold rows for the pinned generation (req.binding.derivations-live)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
        checkout = store.checkout_generation(generation_id)
        n_effective = _kept_count(checkout, "okg.claim_effective", "effective_claim")
        n_resolution = _kept_count(checkout, "okg.claim_projection", ROW_KIND_RESOLUTION)
        n_finding = _kept_count(checkout, "okg.claim_projection", ROW_KIND_FINDING)
        print(f"[claim-plane-wire] okg.claim_effective effective_claim rows="
              f"{n_effective}")
        print(f"[claim-plane-wire] okg.claim_projection bucket_resolution rows="
              f"{n_resolution} finding rows={n_finding}")
        assert n_effective >= 4, n_effective          # a, b, c, d
        assert n_resolution >= 2, n_resolution        # key1 + key2 buckets
        assert n_finding >= 2, n_finding

    # ---- okg.repair_selections holds the recorded selection (task.repair.materialize)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT bucket_key, outcome, winner_claim_id, generator_id, basis,
                       distrusted_claim_ids, unresolved_reason, selection_digest
                  FROM okg.repair_selections
                 WHERE deployment_name = %s AND generation_id = %s
                 ORDER BY bucket_key
                """,
                (_DEPLOYMENT, compat_generation_id),
            )
            sel = {r[0]: r for r in cur.fetchall()}
        n_selected = sum(1 for r in sel.values() if r[1] == "selected")
        n_unresolved = sum(1 for r in sel.values() if r[1] == "unresolved")
        print(f"[claim-plane-wire] okg.repair_selections rows={len(sel)} "
              f"(selected={n_selected} unresolved={n_unresolved})")
        assert set(sel) == {rk1, rk2}, sorted(sel)

        # Decisive bucket key1: selected, precedence-retraction, winner a, distrust b.
        # distrusted_claim_ids is JSONB — psycopg returns it already parsed.
        s1 = sel[rk1]
        assert s1[1] == "selected"
        assert s1[3] == "precedence-retraction"
        assert s1[2] == a               # winner_claim_id
        assert s1[4]                     # a decisive basis, non-empty
        assert list(s1[5]) == [b]        # distrusted_claim_ids (JSONB -> list)
        assert s1[6] is None             # unresolved_reason
        print(f"[claim-plane-wire]   {rk1} -> selected basis={s1[4]!r} "
              f"winner={s1[2]} distrusted={list(s1[5])}")

        # Contested bucket key2: unresolved, empty repair, reason contested.
        s2 = sel[rk2]
        assert s2[1] == "unresolved"
        assert s2[3] == "empty-repair"
        assert s2[6] == "contested"
        assert list(s2[5]) == []
        print(f"[claim-plane-wire]   {rk2} -> unresolved reason={s2[6]!r}")

        # Every row carries the same byte-stable whole-selection digest.
        digests = {r[7] for r in sel.values()}
        assert len(digests) == 1 and next(iter(digests))
        print(f"[claim-plane-wire]   selection_digest={next(iter(digests))[:12]}...")

    # ---- source preservation: the distrusted loser stays present in the projection
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
        checkout = store.checkout_generation(generation_id)
        rows = checkout.query(
            "SELECT ranked_claim_ids, superseded_claim_ids FROM okg.claim_projection "
            "WHERE row_kind = :k AND row_key = :rk",
            {"k": ROW_KIND_RESOLUTION, "rk": rk1},
        )
        ranked = json.loads(rows[0]["ranked_claim_ids"])
        superseded = json.loads(rows[0]["superseded_claim_ids"])
        assert b in ranked and b in superseded  # distrusted, but still present
        assert a in ranked
        print(f"[claim-plane-wire] source preservation: {rk1} ranked={ranked} "
              f"superseded={superseded}")

    # ---- the claim-state IR check RAN as consumer (req.binding.derivations-live)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT payload FROM okg.consistency_candidate_coverage
                 WHERE check_id = %s ORDER BY created_at DESC LIMIT 1
                """,
                (_CHECK_ID,),
            )
            cov = cur.fetchone()
        assert cov is not None, "claim-state IR check produced no coverage row"
        payload = dict(cov[0])
        print(f"[claim-plane-wire] IR check coverage: "
              f"claim_state_surfaces={payload.get('claim_state_surfaces')} "
              f"scope_reason={payload.get('scope_reason')}")
        assert payload.get("claim_state_surfaces") == ["claim_projection_rows"]

        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT participant_roles->>'bucket'
                  FROM okg.observation_current
                 WHERE check_id = %s AND finding_status = 'open'
                """,
                (_CHECK_ID,),
            )
            open_buckets = {row[0] for row in cur.fetchall()}
        print(f"[claim-plane-wire] IR check open findings (buckets)={open_buckets}")
        # The consumer flags the contested bucket (key2), never the decisive one.
        assert rk2 in open_buckets
        assert rk1 not in open_buckets
