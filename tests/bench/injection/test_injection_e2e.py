"""The end-to-end example (task.bench.e2e-example / ship-okg-bench CLI loop).

Drives the SAME shared loop the `okg bench inject` CLI uses
(okg.bench.injection.run_injection_benchmark) twice against real publish
cycles — clean baseline generation, injection targeted from the pinned
clean generation, mutated benchmark generation, leakage gate, and the
stage-attributed scorecard with raw denominators — and proves the second
invocation is byte-identical over the deterministic surface (injection
artifacts + scorecard core), while generation ids and latencies live only
in run-binding.json and the annex. The runbook's documented command is the
CLI verb (docs/runbooks/quality/benchmarks/okg-bench-ab.md).
"""

from __future__ import annotations

from pathlib import Path

import pytest

psycopg = pytest.importorskip("psycopg")

from okg.bench.injection import (  # noqa: E402
    SCORECARD_CORE_FILENAME,
    run_injection_benchmark,
)
from okg.substrate.catalog.modules_apply import apply_composed_catalog  # noqa: E402
from okg.substrate.catalog.modules_compose import compose_catalog  # noqa: E402
from okg.substrate.catalog.ownership import (  # noqa: E402
    catalog_identity_for,
    claim_catalog_ownership,
    ensure_catalog_write_allowed,
)
from okg.bench.injection import CorpusSnapshot  # noqa: E402

_REPO_ROOT = Path(__file__).resolve().parents[3]
_DEP_DIR = _REPO_ROOT / "deployments" / "consistency-injection-fixture"


def _install_composed_catalog(dsn: str, tmp_path: Path) -> None:
    del tmp_path
    composed = compose_catalog(_DEP_DIR)
    identity = catalog_identity_for(
        composed=composed,
        deployment_dir=_DEP_DIR,
        command="pytest injection benchmark catalog apply",
        writer_scope="deployment_root",
    )
    with psycopg.connect(dsn, autocommit=True) as conn:
        claim_catalog_ownership(conn, identity=identity, force=True)
        ensure_catalog_write_allowed(conn, identity=identity)
        apply_composed_catalog(conn, composed, identity=identity)


def _run_loop(dsn: str, base_dir: Path, seed: int) -> dict:
    return run_injection_benchmark(
        dsn=dsn,
        deployment_dir=_DEP_DIR,
        out_dir=base_dir / "injection",
        work_dir=base_dir / "working",
        seed=seed,
    )


@pytest.mark.postgres
def test_ring0_end_to_end_documented_example(clean_runtime: str, tmp_path: Path) -> None:
    _install_composed_catalog(clean_runtime, tmp_path)

    first = _run_loop(clean_runtime, tmp_path / "run1", seed=101)
    core = first["scorecard"]["core"]
    print(f"\n[e2e] clean generation={first['clean']['generation_id']}")
    print(f"[e2e] mutated generation={first['mutated']['generation_id']}")
    print(f"[e2e] gold={core['gold_count']} stages=" + ", ".join(
        f"{name}:{tally['recovered']}/{tally['denominator']}"
        for name, tally in core["stages"].items()
    ))
    print(f"[e2e] end_to_end={core['end_to_end']} "
          f"fp={core['false_positives']['count']} "
          f"hard_negatives={core['hard_negatives']['recovered']}/"
          f"{core['hard_negatives']['denominator']} "
          f"missed={core['hard_negatives']['missed']}")
    print(f"[e2e] hn fp_by_mechanism={core['hard_negatives']['fp_by_mechanism']}")

    # Raw denominators, live: every mutation is dispositioned; the pair
    # conflicts flow through all four stages; misses are attributed.
    assert core["gold_count"] == len(first["injection"].mutations) >= 12
    assert core["stages"]["extraction"]["denominator"] >= 10
    assert core["stages"]["extraction"]["recovered"] == core["stages"]["extraction"]["denominator"]
    assert core["stages"]["candidate"]["denominator"] >= 7
    assert core["stages"]["verdict"]["recovered"] == core["stages"]["verdict"]["denominator"]
    assert core["stages"]["routing"]["recovered"] == core["stages"]["routing"]["denominator"]
    # Ring-0 honesty: no malformed-value detector exists, so that family is
    # an attributed end-to-end miss, and the string-equality value rules
    # flag the rounding + alias twins — reported per mechanism.
    assert core["hard_negatives"]["fp_by_mechanism"], core["hard_negatives"]
    assert set(core["hard_negatives"]["fp_by_mechanism"]) <= {
        "value_compatibility", "canonicalization",
    }
    assert core["false_positives"]["count"] == 0
    assert core["end_to_end"]["graph_rate"] > 0.5

    # The volatile surface stays out of the core: run-binding carries the
    # generation ids; the core bytes carry none.
    core_bytes_1 = (first["injection"].out_dir / SCORECARD_CORE_FILENAME).read_bytes()
    binding = (first["injection"].out_dir / "run-binding.json").read_bytes()
    assert str(first["clean"]["generation_id"]).encode() in binding
    assert str(first["clean"]["generation_id"]).encode() not in core_bytes_1

    # Second documented invocation: byte-identical deterministic surface.
    second = _run_loop(clean_runtime, tmp_path / "run2", seed=101)
    for name in ("mutations.jsonl", "benchmark-manifest.json"):
        assert (first["injection"].out_dir / name).read_bytes() == (
            second["injection"].out_dir / name
        ).read_bytes()
    assert (
        CorpusSnapshot.load(first["injection"].mutated_corpus_dir).tree_digest
        == CorpusSnapshot.load(second["injection"].mutated_corpus_dir).tree_digest
    )
    core_bytes_2 = (second["injection"].out_dir / SCORECARD_CORE_FILENAME).read_bytes()
    assert core_bytes_1 == core_bytes_2
    print("[e2e] second invocation byte-identical over the deterministic surface")
