"""req.abx.corpus: ref-surface surgery + the fail-closed leakage audit.

The no-database half proves the checkout surface IS the cutoff and that
tampering is detected. The postgres-marked half runs the real
build->publish->audit path (gate 2's real-output check) against a dedicated
database; it needs OKG_BENCH_AB_TEST_DSN and network, so it is opt-in.
"""

from __future__ import annotations

import os
import subprocess
from pathlib import Path

import pytest

from okg.bench.ab import checkouts
from okg.bench.ab.errors import CorpusBuildError, LeakageViolation


def _git(cwd: Path, *args: str) -> str:
    return subprocess.run(
        ["git", "-C", str(cwd), *args], capture_output=True, text=True, check=True
    ).stdout


def test_surgical_checkout_postconditions(tiny_repo, tmp_path):
    data_root = tmp_path / "data"
    base = _git(tiny_repo, "rev-parse", "HEAD~1").strip()
    dest = tmp_path / "checkout"
    checkouts.surgical_checkout(str(tiny_repo), base, dest, data_root=data_root)
    refs = _git(dest, "for-each-ref", "--format=%(refname)").splitlines()
    assert refs == ["refs/heads/main"]
    assert _git(dest, "rev-parse", "HEAD").strip() == base
    assert _git(dest, "remote").strip() == ""
    assert _git(dest, "tag").strip() == "", "tags are a leakage surface"
    # The post-cutoff commit must not be reachable from any ref.
    head_of_source = _git(tiny_repo, "rev-parse", "HEAD").strip()
    reachable = _git(dest, "rev-list", "--all").splitlines()
    assert head_of_source not in reachable
    # Stronger (review F1): the post-cutoff OBJECT must not exist in the
    # object store at all — `git fsck --unreachable` must find nothing, and
    # cat-file on the gold-side commit must fail.
    fsck = subprocess.run(
        ["git", "-C", str(dest), "fsck", "--unreachable", "--no-progress"],
        capture_output=True, text=True, check=True,
    ).stdout
    assert "unreachable" not in fsck, fsck
    gone = subprocess.run(
        ["git", "-C", str(dest), "cat-file", "-e", head_of_source],
        capture_output=True, check=False,
    )
    assert gone.returncode != 0, "post-cutoff object still present in checkout"


def test_surgical_checkout_missing_commit_is_fatal(tiny_repo, tmp_path):
    with pytest.raises(CorpusBuildError, match="not found in mirror"):
        checkouts.surgical_checkout(
            str(tiny_repo), "1234567890abcdef1234567890abcdef12345678",
            tmp_path / "c", data_root=tmp_path / "data",
        )


def test_verify_ref_surface_detects_tampering(tiny_repo, tmp_path):
    data_root = tmp_path / "data"
    base = _git(tiny_repo, "rev-parse", "HEAD~1").strip()
    dest = tmp_path / "checkout"
    checkouts.surgical_checkout(str(tiny_repo), base, dest, data_root=data_root)
    checkouts.verify_ref_surface(dest, base)  # clean passes
    _git(dest, "tag", "sneaky")
    with pytest.raises(CorpusBuildError, match="extra refs"):
        checkouts.verify_ref_surface(dest, base)


def test_audit_verifies_checkout_before_touching_the_db(tiny_repo, tmp_path):
    """A tampered checkout fails the audit even with an unreachable DSN —
    assertion (c) runs first and fail-closed."""
    from okg.bench.ab.leakage import audit

    data_root = tmp_path / "data"
    base = _git(tiny_repo, "rev-parse", "HEAD~1").strip()
    dest = tmp_path / "checkout"
    checkouts.surgical_checkout(str(tiny_repo), base, dest, data_root=data_root)
    _git(dest, "tag", "sneaky")
    with pytest.raises(CorpusBuildError, match="extra refs"):
        audit(
            dsn="postgres://invalid.invalid/never",
            generation_id="1",
            cutoff="2023-05-01T00:00:00Z",
            gold_pr_number=1,
            checkout=dest,
            base_commit=base,
        )


@pytest.mark.postgres
@pytest.mark.slow
def test_real_instance_corpus_build_and_audit(tmp_path):
    """Gate 2 end-to-end: one real SWE-bench Verified instance -> published
    generation -> leakage audit PASS -> negative control fails.

    Opt-in: needs OKG_BENCH_AB_TEST_DSN (a dedicated, disposable database),
    network (github.com + gh auth), and several minutes.
    """
    dsn = os.environ.get("OKG_BENCH_AB_TEST_DSN")
    if not dsn:
        pytest.skip("OKG_BENCH_AB_TEST_DSN not set (dedicated corpus DB required)")
    from okg.bench.ab import corpus as corpus_mod
    from okg.bench.ab.adapters import get_adapter
    from okg.bench.ab.leakage import audit
    from okg.bench.ab.model import load_json

    adapter = get_adapter("swebench-verified")
    data_root = tmp_path / "data"
    adapter.fetch(data_root=data_root)
    instances = adapter.load_instances(data_root=data_root)
    # Smallest-history repo in Verified: psf/requests.
    inst = adapter.subset(instances, "repo:psf/requests")[0]
    spec = corpus_mod.CorpusSpec(
        repo=inst.repo,
        base_commit=inst.base_commit,
        cutoff=inst.cutoff,
        gold_pr_number=inst.extra["gold_pr_number"],
        deployment_name=f"bench-{inst.instance_id.replace('/', '-')}",
        dsn=dsn,
        corpus_root=tmp_path / "corpus",
    )
    corpus_json = corpus_mod.build(spec, data_root=data_root)
    cfg = load_json(corpus_json)
    assert cfg["leakage_audit"] == "pass"
    # Negative control: an artificially earlier cutoff MUST fail the audit.
    with pytest.raises(LeakageViolation):
        audit(
            dsn=dsn,
            generation_id=cfg["generation_id"],
            cutoff="2000-01-01T00:00:00Z",
            gold_pr_number=cfg["gold_pr_number"],
            checkout=Path(cfg["corpus_root"]) / "checkout",
            base_commit=cfg["base_commit"],
        )
