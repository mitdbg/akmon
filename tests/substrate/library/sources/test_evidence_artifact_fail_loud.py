from __future__ import annotations

import json
from pathlib import Path

import pytest

from okg.substrate.errors import SubstrateError

pytest.importorskip("defusedxml")


def _write(path: Path, text: str) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")
    return path


@pytest.mark.parametrize(
    ("kind", "artifact_rel", "contents", "source_factory", "expected_message"),
    [
        (
            "test-report-xml",
            "reports/junit.xml",
            "{",
            lambda repo, artifact: _test_report_source(
                repo,
                artifact_paths=[artifact],
            ),
            "malformed test-report XML artifact",
        ),
        (
            "test-report-json",
            "reports/coverage.json",
            "{",
            lambda repo, artifact: _test_report_source(
                repo,
                coverage_paths=[artifact],
            ),
            "malformed test-report JSON artifact",
        ),
        (
            "profile-json",
            "profiles/run/profile.json",
            "{",
            lambda repo, artifact: _profile_source(
                repo,
                artifact_paths=[artifact],
            ),
            "malformed profile-measurement JSON artifact",
        ),
        (
            "profile-yaml",
            "deployment.yaml",
            "{",
            lambda repo, artifact: _profile_source(
                repo,
                deployment_config_path=artifact,
            ),
            "malformed profile-measurement budget YAML",
        ),
        (
            "profile-frontmatter-yaml",
            "openspec/change.md",
            "---\nperformance_budgets: [\n---\nbody\n",
            lambda repo, artifact: _profile_source(
                repo,
                openspec_roots=[artifact.parent],
            ),
            "malformed profile-measurement budget front matter",
        ),
    ],
)
def test_finding_13_malformed_artifacts_fail_loud(
    tmp_path: Path,
    kind: str,
    artifact_rel: str,
    contents: str,
    source_factory,
    expected_message: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo = tmp_path / "repo"
    artifact = _write(repo / artifact_rel, contents)
    source = source_factory(repo, artifact)
    if kind in {"test-report-json", "profile-json"}:
        class StaleJSONDecodeError(ValueError):
            pass

        monkeypatch.setattr(json, "JSONDecodeError", StaleJSONDecodeError)

    with pytest.raises(SubstrateError, match=expected_message) as exc_info:
        list(source.run(f"run-{kind}", mode="scope_complete").facts)

    assert exc_info.value.data["source"] in {
        "test_reports",
        "profile_measurements",
    }
    assert exc_info.value.data["artifact"]


def _test_report_source(repo: Path, **kwargs):
    from okg.substrate.library.sources.test_reports import TestReportSource

    return TestReportSource(
        repo="demo",
        repo_root=repo,
        **kwargs,
    )


def _profile_source(repo: Path, **kwargs):
    from okg.substrate.library.sources.profile_measurements import (
        ProfileMeasurementSource,
    )

    return ProfileMeasurementSource(
        deployment="okg-workspace",
        repo="demo",
        repo_root=repo,
        **kwargs,
    )
