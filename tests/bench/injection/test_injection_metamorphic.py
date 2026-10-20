"""Repair-metamorphic witnesses on the ring-0 fixture (task.bench.metamorphic).

The full loop, live: clean publish (B0) → inject one status inversion →
sync the working directory → re-ingest + publish (expected observation
opens) → materialize the recorded inverse as a new corpus version → sync +
re-ingest + publish. Pins the verification clause: the expected observation
resolves; the restored claim is present in the post-repair claim set with
its ORIGINAL value (a vanished claim also "resolves" and must not pass);
and the finding-set delta contains no unrelated additions (open set == B0
exactly).
"""

from __future__ import annotations

import shutil
import uuid
from pathlib import Path
from typing import Any

import pytest

psycopg = pytest.importorskip("psycopg")

import yaml  # noqa: E402

from okg.substrate.catalog.modules_apply import apply_composed_catalog  # noqa: E402
from okg.substrate.catalog.modules_compose import compose_catalog  # noqa: E402
from okg.bench.injection import (  # noqa: E402
    ClaimSnapshot,
    CorpusSnapshot,
    RunConfig,
    apply_inverses,
    run_injection,
)
from okg.bench.injection.claim_corpus_source import (  # noqa: E402
    ClaimCorpusSource,
)
from okg.bench.injection.operators.library import (  # noqa: E402
    StatusInversionOperator,
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
_POLICIES = ("status", "version")


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


def _ingest_and_publish(dsn: str, working_dir: Path) -> dict[str, Any]:
    source = ClaimCorpusSource(repo_root=working_dir, policies=_POLICIES)
    facts = list(source.run(f"meta:{uuid.uuid4().hex[:8]}").facts)
    with psycopg.connect(dsn, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn, ensure_schema=True)
        store.apply_facts(facts, source=_DEPLOYMENT, strict_endpoints=False)
    out = build_deployment_publisher(
        dsn=dsn,
        deployment_name=_DEPLOYMENT,
        deployment_dir=_DEP_DIR,
        command="pytest injection metamorphic",
    ).cycle()
    assert out["status"] == "published", out
    return out


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


def _snapshot(dsn: str, generation_id: Any) -> ClaimSnapshot:
    with psycopg.connect(dsn, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
        checkout = store.checkout_generation(generation_id)
        return ClaimSnapshot.from_checkout(checkout, str(generation_id))


def _sync(source_dir: Path, working_dir: Path) -> None:
    for path in sorted(source_dir.rglob("*.md")):
        target = working_dir / path.relative_to(source_dir)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(path.read_bytes())


def _claim_value(snapshot: ClaimSnapshot, predicate: str, doc: str) -> str:
    matches = [
        claim for claim in snapshot.claims
        if claim.predicate == predicate
        and claim.normalized_key == "alpha_service"
        and claim.source_doc == doc
    ]
    assert len(matches) == 1, matches
    return matches[0].value


@pytest.mark.postgres
def test_inverse_application_resolves_the_expected_observation(
    clean_runtime: str, tmp_path: Path
) -> None:
    _install_composed_catalog(clean_runtime, tmp_path)
    working_dir = tmp_path / "working"
    working_dir.mkdir()
    _sync(_CORPUS_DIR, working_dir)

    # ---- clean baseline
    out_clean = _ingest_and_publish(clean_runtime, working_dir)
    b0 = _open_findings(clean_runtime)
    # Empty since bucket-level corroboration settles version/alpha_service
    # 2-vs-1 (ops_alpha.md + ops_mirror.md assert 4.2, ops_legacy.md 3.9); see
    # test_injection_ring0 for the mechanism and the mirror-counting caveat.
    # A clean baseline makes the "and only it" assertions below sharper, and
    # they read b0 as a variable so the experiment is unaffected either way.
    assert b0 == set(), b0
    clean_snapshot = _snapshot(clean_runtime, out_clean["generation_id"])
    assert _claim_value(clean_snapshot, "status", "ops_alpha.md") == "active"

    # ---- inject one status inversion, targeted from the pinned generation
    result = run_injection(
        corpus=CorpusSnapshot.load(working_dir),
        claims=clean_snapshot,
        operators=[
            StatusInversionOperator(
                predicates=("status",),
                source_docs=("ops_alpha.md",),
                counterpart_docs=("ops_mirror.md",),
            )
        ],
        config=RunConfig(
            seed=13,
            out_dir=tmp_path / "injection",
            b0_claim_ids=frozenset(
                claim.claim_id
                for claim in clean_snapshot.claims
                if claim.predicate == "version"
            ),
        ),
    )
    assert len(result.mutations) == 1
    mutation = result.mutations[0]
    assert mutation.pre_text == "active"

    # ---- mutated run: the expected observation opens (and only it)
    _sync(result.mutated_corpus_dir, working_dir)
    out_mutated = _ingest_and_publish(clean_runtime, working_dir)
    status_bucket = bucket_row_key("status", "alpha_service")
    open_after_injection = _open_findings(clean_runtime)
    print(f"\n[metamorphic] after injection open={sorted(open_after_injection)}")
    assert open_after_injection == b0 | {status_bucket}
    mutated_snapshot = _snapshot(clean_runtime, out_mutated["generation_id"])
    assert _claim_value(mutated_snapshot, "status", "ops_alpha.md") == mutation.post_text

    # ---- repair: materialize the inverse as a new corpus version, re-ingest
    repaired_dir = tmp_path / "repaired"
    shutil.copytree(result.mutated_corpus_dir, repaired_dir)
    mutated_doc = (repaired_dir / mutation.doc).read_text(encoding="utf-8")
    (repaired_dir / mutation.doc).write_text(
        apply_inverses(mutated_doc, [mutation]), encoding="utf-8"
    )
    _sync(repaired_dir, working_dir)
    out_repaired = _ingest_and_publish(clean_runtime, working_dir)

    open_after_repair = _open_findings(clean_runtime)
    print(f"[metamorphic] after repair open={sorted(open_after_repair)}")
    assert open_after_repair == b0  # resolved, no unrelated additions

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT finding_status FROM okg.observation_current
                 WHERE check_id = %s AND participant_roles->>'bucket' = %s
                """,
                (_CHECK_ID, status_bucket),
            )
            statuses = {row[0] for row in cur.fetchall()}
    assert statuses == {"resolved"}, statuses

    # The restored claim is PRESENT with its original value — a vanished
    # claim would also "resolve" and must not pass.
    repaired_snapshot = _snapshot(clean_runtime, out_repaired["generation_id"])
    assert _claim_value(repaired_snapshot, "status", "ops_alpha.md") == "active"
    restored = (working_dir / mutation.doc).read_text(encoding="utf-8")
    assert restored == (_CORPUS_DIR / mutation.doc).read_text(encoding="utf-8")
