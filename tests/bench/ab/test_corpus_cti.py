"""req.cti.corpus: the CTI corpus contract, now expressed as one vertical.

The generic spec-driven behaviour (scaffold shape, per-vertical zero-family
refusal, immutability, override plumbing) lives in test_corpus_domain.py.
These witnesses stay CTI-specific on purpose: PACT okg-bench-ctibench binds
its published-generation evidence to them, so they must keep asserting the
CTI deployment's own guarantees — three sources, the three expected
families, and the artifact set NVD/CWE/ATT&CK requires.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest
import yaml

from okg.bench.ab import corpus_cti, corpus_domain as cd
from okg.bench.ab.errors import CorpusBuildError


def _snapshot(tmp_path: Path) -> Path:
    snap = tmp_path / "snapshot"
    snap.mkdir()
    (snap / "nvdcve-2.0-2024.json").write_text('{"vulnerabilities": []}')
    (snap / "cwec_v4.14.xml").write_text("<Weakness_Catalog/>")
    (snap / "enterprise-attack.json").write_text('{"type": "bundle"}')
    # A snapshot that declares no provenance is refused, fixtures included.
    (snap / cd.PROVENANCE_FILENAME).write_text(
        '{"artifacts": {"fixture": "stand-in"},'
        ' "note": "synthetic test fixture"}'
    )
    return snap


def _spec(tmp_path: Path, snap: Path) -> cd.DomainCorpusSpec:
    return cd.DomainCorpusSpec(
        vertical="cti",
        snapshot_dir=snap,
        snapshot_date="2026-07-27",
        deployment_name="bench-cti",
        dsn="postgres://x/y",
        corpus_root=tmp_path / "corpus",
    )


def test_inventory_digests_every_file(tmp_path):
    inv = cd.inventory(_snapshot(tmp_path), cd.CTI)
    # 3 artifacts + the provenance declaration, which is digested like any
    # other snapshot file: the claim about where the data came from is part
    # of the content-addressed snapshot, not metadata floating beside it.
    assert inv["file_count"] == 4
    for entry in inv["files"].values():
        assert len(entry["sha256"]) == 64
        assert entry["bytes"] > 0


def test_inventory_refuses_missing_artifacts(tmp_path):
    snap = tmp_path / "snap"
    snap.mkdir()
    (snap / "nvdcve-2.0-2024.json").write_text("{}")
    with pytest.raises(CorpusBuildError, match="lacks cti artifacts"):
        cd.inventory(snap, cd.CTI)
    empty = tmp_path / "empty"
    empty.mkdir()
    with pytest.raises(CorpusBuildError, match="is empty"):
        cd.inventory(empty, cd.CTI)


def test_deployment_written_with_three_sources(tmp_path):
    snap = _snapshot(tmp_path)
    deployment_dir = cd.write_deployment(_spec(tmp_path, snap), cd.CTI)
    manifest = yaml.safe_load((deployment_dir / "deployment.yaml").read_text())
    assert manifest["modules"] == ["cti"]
    registry = yaml.safe_load(
        (deployment_dir / "source_registry.yaml").read_text()
    )
    assert set(registry["sources"]) == {"nvd-feeds", "cwe-catalog", "attack-stix"}
    nvd = registry["sources"]["nvd-feeds"]
    assert nvd["params"]["snapshot_glob"].endswith("nvd*.json*")
    assert registry["sources"]["cwe-catalog"]["params"]["snapshot_path"].endswith(
        "cwec_v4.14.xml"
    )


def test_ambiguous_single_artifacts_refuse(tmp_path):
    snap = _snapshot(tmp_path)
    (snap / "mobile-attack.json").write_text('{"type": "bundle"}')
    with pytest.raises(CorpusBuildError, match="exactly one ATT&CK bundle"):
        cd.write_deployment(_spec(tmp_path, snap), cd.CTI)


def test_zero_family_refusal(tmp_path, monkeypatch):
    monkeypatch.setattr(
        cd, "provision_and_publish",
        lambda *a, **k: ("gen-1", {"cve": 100, "cwe": 50, "attack_technique": 0}),
    )
    with pytest.raises(CorpusBuildError, match="ZERO nodes for families"):
        cd.build(_spec(tmp_path, _snapshot(tmp_path)))


def test_build_writes_freeze_manifest(tmp_path, monkeypatch):
    spec = _spec(tmp_path, _snapshot(tmp_path))
    monkeypatch.setattr(
        cd, "provision_and_publish",
        lambda *a, **k: ("gen-1", {"cve": 100, "cwe": 50, "attack_technique": 20,
                                   "attack_tactic": 14}),
    )
    # The contract gate reads the published generation; this unit test has no
    # database, so stub the evaluation with a satisfied report. The gate
    # itself is covered against real corpora in test_corpus_contract.py.
    from okg.bench.ab.contract import ContractReport

    passing = ContractReport()
    passing.node_counts = {"cve": 100}
    passing.total_nodes = 100
    passing.total_edges = 90
    passing.connected_nodes = 100
    monkeypatch.setattr(cd, "evaluate_contract", lambda **k: passing)
    corpus_json = cd.build(spec)
    cfg = json.loads(corpus_json.read_text())
    assert cfg["vertical"] == "cti"
    assert cfg["snapshot"]["file_count"] == 4  # 3 artifacts + provenance
    assert cfg["provenance"] == "stand-in", (
        "the fixture declares a synthetic artifact, so the corpus must say "
        "stand-in rather than inherit a default of real"
    )
    assert cfg["snapshot_date"] == "2026-07-27"
    assert cfg["family_counts"]["cve"] == 100
    # The okg arm needs deployment identity, not just a snapshot dir.
    for key in ("deployment_name", "deployment_dir", "dsn", "generation_id"):
        assert cfg[key], f"corpus.json missing {key}"
    with pytest.raises(CorpusBuildError, match="immutable"):
        cd.build(spec)


def test_empty_snapshot_family_fails_loudly(tmp_path, monkeypatch):
    """PACT anchor (req.cti.corpus): the zero-family gate is load-bearing —
    a publish that produced nothing must never be recorded as a corpus."""
    monkeypatch.setattr(cd, "provision_and_publish", lambda *a, **k: ("g", {}))
    with pytest.raises(CorpusBuildError, match="ZERO nodes"):
        cd.build(_spec(tmp_path, _snapshot(tmp_path)))


def test_cti_cli_spec_still_builds_through_the_shared_path(tmp_path, monkeypatch):
    captured: dict = {}
    monkeypatch.setattr(
        corpus_cti, "_build_vertical",
        lambda spec: captured.setdefault("spec", spec) and None
        or tmp_path / "corpus.json",
    )
    corpus_cti.build(corpus_cti.CtiCorpusSpec(
        snapshot_dir=_snapshot(tmp_path), snapshot_date="2026-07-27",
        deployment_name="bench-cti", dsn="postgres://x/y",
        corpus_root=tmp_path / "c",
    ))
    assert captured["spec"].vertical == "cti"
    assert captured["spec"].deployment_name == "bench-cti"
