"""Artifact sources resolve to a delete-detecting ContentHashProbe (#323).

security_analysis + profile_measurements import CI-generated (non-git) artifacts,
so the content-hash floor is the sound probe. DB-free: only the probe wiring.
"""
from __future__ import annotations

import os

from okg.substrate.library.sources.change_probe import tokens_equal
from okg.substrate.library.sources.content_hash_probe import ContentHashProbe
from okg.substrate.library.sources.profile_measurements import ProfileMeasurementSource
from okg.substrate.library.sources.security_analysis import SecurityAnalysisSource


def test_security_analysis_uses_content_hash_probe_and_detects_delete(tmp_path):
    (tmp_path / "a.sarif").write_text('{"runs": []}')
    (tmp_path / "b.sarif").write_text('{"runs": [1]}')
    assert SecurityAnalysisSource.change_probe_kind == "content_hash"
    s = SecurityAnalysisSource(repo="demo", repo_root=str(tmp_path), artifact_globs=("*.sarif",))
    assert isinstance(s.change_probe, ContentHashProbe)
    t0 = s.change_probe.build_token()
    os.remove(tmp_path / "b.sarif")
    assert not tokens_equal(t0, s.change_probe.build_token())


def test_security_analysis_finding_policy_change_busts_token(tmp_path):
    (tmp_path / "a.sarif").write_text('{"runs": []}')
    a = SecurityAnalysisSource(repo="demo", repo_root=str(tmp_path), artifact_globs=("*.sarif",))
    t0 = a.change_probe.build_token()
    # A different finding-exclude policy must change the token (same artifacts).
    b = SecurityAnalysisSource(
        repo="demo", repo_root=str(tmp_path), artifact_globs=("*.sarif",),
        finding_exclude_globs=("**/vendor/**",),
    )
    assert not tokens_equal(t0, b.change_probe.build_token())


def test_profile_measurements_uses_content_hash_probe_and_detects_delete(tmp_path):
    (tmp_path / "p1.json").write_text('{"hotspots": []}')
    (tmp_path / "p2.json").write_text('{"hotspots": [1]}')
    assert ProfileMeasurementSource.change_probe_kind == "content_hash"
    s = ProfileMeasurementSource(
        deployment="dep", repo="demo", repo_root=str(tmp_path), artifact_globs=("*.json",),
    )
    assert isinstance(s.change_probe, ContentHashProbe)
    t0 = s.change_probe.build_token()
    os.remove(tmp_path / "p2.json")
    assert not tokens_equal(t0, s.change_probe.build_token())


def test_profile_measurements_includes_benchmark_scope(tmp_path):
    (tmp_path / "p.json").write_text('{"hotspots": []}')
    (tmp_path / "bench.json").write_text('{"benchmarks": []}')
    s = ProfileMeasurementSource(
        deployment="dep", repo="demo", repo_root=str(tmp_path),
        artifact_globs=("p.json",), benchmark_globs=("bench.json",),
    )
    t0 = s.change_probe.build_token()
    (tmp_path / "bench.json").write_text('{"benchmarks": [1]}')  # benchmark edit
    assert not tokens_equal(t0, s.change_probe.build_token())
