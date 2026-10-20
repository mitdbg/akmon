from __future__ import annotations

import json
import os
from pathlib import Path

import psycopg
import pytest
import yaml

from okg.substrate.library.sources.base import EdgeFact, NodeFact
from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer  # noqa: E402


def _write_json(path: Path, payload: object) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return path


def test_security_analysis_declares_verified_empty_artifact_scope(
    tmp_path: Path,
) -> None:
    from okg.substrate.library.sources.security_analysis import (
        SecurityAnalysisSource,
    )

    repo = tmp_path / "repo"
    repo.mkdir()
    run = SecurityAnalysisSource(
        repo="demo",
        repo_root=repo,
    ).run("run-empty", mode="scope_complete")

    assert list(run.facts) == []
    assert run.completed_scope is True
    assert run.record_set is not None
    assert run.record_set.records == {}


def test_security_facts_ignore_mtime_and_omit_missing_sbom_time(
    tmp_path: Path,
) -> None:
    from okg.substrate.library.sources.security_analysis import (
        SecurityAnalysisSource,
    )

    repo = tmp_path / "repo"
    artifact = _write_json(
        repo / "reports" / "sbom.json",
        {
            "bomFormat": "CycloneDX",
            "metadata": {"tools": [{"name": "syft"}]},
            "components": [],
        },
    )
    source = SecurityAnalysisSource(
        repo="demo",
        repo_root=repo,
        artifact_paths=[artifact],
    )

    first_run = source.run("acquisition-a", mode="scope_complete")
    before = list(first_run.facts)
    assert first_run.record_set is not None
    assert set(first_run.record_set.records) == {"reports/sbom.json"}
    assert first_run.applied_token_scope == "global"
    assert first_run.applied_token_authority == "exact"
    assert first_run.applied_raw_token == source.change_probe.build_token()
    os.utime(artifact, (2_000_000_000, 2_000_000_000))
    second_run = source.run("acquisition-b", mode="scope_complete")
    after = list(second_run.facts)
    assert second_run.applied_raw_token == source.change_probe.build_token()

    assert after == before
    assert all("mtime" not in fact.source_revision for fact in after)
    sbom = next(
        fact
        for fact in after
        if isinstance(fact, NodeFact) and fact.subtype == "sbom_artifact"
    )
    assert "mtime" not in sbom.attrs
    assert "observed_at" not in sbom.attrs
    assert "generated_at" not in sbom.attrs


def test_security_analysis_mixed_malformed_artifact_fails_closed(
    tmp_path: Path,
) -> None:
    from okg.substrate.library.sources.security_analysis import (
        SecurityAnalysisSource,
    )

    repo = tmp_path / "repo"
    valid = _write_json(
        repo / "reports" / "gitleaks.json",
        [
            {
                "RuleID": "generic-api-key",
                "File": "src/config.py",
                "StartLine": 7,
                "Fingerprint": "fixture-fingerprint",
                "Match": "TOKEN=fixture-secret",
                "Secret": "fixture-secret",
            }
        ],
    )
    malformed = repo / "reports" / "malformed.json"
    malformed.write_text("{not-json", encoding="utf-8")
    prior_cursor = {"run_id": "prior-run"}

    run = SecurityAnalysisSource(
        repo="demo",
        repo_root=repo,
        artifact_paths=[valid, malformed],
    ).run(
        "run-mixed",
        mode="scope_complete",
        cursor=prior_cursor,
    )

    assert list(run.facts)
    assert run.completed_scope is False
    assert run.record_set is None
    assert run.next_cursor == prior_cursor
    assert run.health is not None
    assert run.health.status == "acquire_failed"


def test_security_analysis_source_imports_scanner_artifacts(tmp_path: Path) -> None:
    from okg.substrate.library.sources.security_analysis import (
        SecurityAnalysisSource,
    )

    repo = tmp_path / "repo"
    (repo / "src").mkdir(parents=True)
    (repo / "src" / "db.py").write_text("query = input()\n")
    artifacts = _security_artifacts(repo)

    source = SecurityAnalysisSource(
        repo="demo",
        repo_root=repo,
        artifact_paths=artifacts,
    )
    facts = list(source.run("run-1", mode="scope_complete").facts)
    nodes = [fact for fact in facts if isinstance(fact, NodeFact)]
    subtypes = {node.subtype for node in nodes}

    assert {
        "security_finding",
        "secret_finding",
        "dependency_vulnerability",
        "sbom_artifact",
    } <= subtypes

    security = [node for node in nodes if node.subtype == "security_finding"]
    assert {node.attrs["tool"] for node in security} >= {
        "codeql",
        "semgrep",
        "bandit",
        "trivy",
    }
    assert any(node.attrs["rule_id"] == "py/sql-injection" for node in security)
    assert any(node.attrs["path"] == "src/db.py" for node in security)

    secrets = [node for node in nodes if node.subtype == "secret_finding"]
    assert secrets
    secret_attrs = json.dumps([node.attrs for node in secrets], sort_keys=True)
    assert "raw-super-secret-token" not in secret_attrs
    assert "[REDACTED]" in secret_attrs

    vulns = [node for node in nodes if node.subtype == "dependency_vulnerability"]
    vuln_ids = {node.attrs["vuln_id"] for node in vulns}
    assert {"PYSEC-2026-1", "GHSA-xxxx-yyyy-zzzz", "CVE-2026-0001"} <= vuln_ids
    assert any(node.attrs["package_name"] == "pydantic" for node in vulns)
    assert any(node.attrs["fix_versions"] == ["2.9.0"] for node in vulns)
    pydantic = next(node for node in vulns if node.attrs["package_name"] == "pydantic")
    left_pad = next(node for node in vulns if node.attrs["package_name"] == "left-pad")
    requests = next(node for node in vulns if node.attrs["package_name"] == "requests")
    assert pydantic.attrs["ecosystem"] == "python"
    assert pydantic.attrs["package_id"] == "dep:python:pydantic"
    assert pydantic.attrs["aliases"] == ["CVE-2026-0001"]
    assert pydantic.attrs["cvss"] == 8.1
    assert pydantic.attrs["published_at"] == "2026-04-01T00:00:00Z"
    assert pydantic.attrs["modified_at"] == "2026-04-02T00:00:00Z"
    assert left_pad.attrs["ecosystem"] == "npm"
    assert left_pad.attrs["package_id"] == "dep:npm:left-pad"
    assert left_pad.attrs["cvss"] == 7.5
    assert left_pad.attrs["fix_versions"] == ["1.1.0"]
    assert requests.attrs["ecosystem"] == "python"
    assert requests.attrs["package_id"] == "dep:python:requests"
    assert requests.attrs["cvss"] == 9.8

    sboms = [node for node in nodes if node.subtype == "sbom_artifact"]
    assert len(sboms) == 1
    assert sboms[0].attrs["format"] == "cyclonedx_json"
    assert sboms[0].attrs["component_count"] == 1
    edges = [fact for fact in facts if isinstance(fact, EdgeFact)]
    assert {
        ("affects", "dep:python:pydantic"),
        ("affects", "dep:npm:left-pad"),
        ("affects", "dep:python:requests"),
        ("references", "dep:python:pydantic"),
    } <= {(edge.edge_type, edge.dst) for edge in edges}


def test_secret_findings_use_allowlisted_redacted_attrs(tmp_path: Path) -> None:
    from okg.substrate.library.sources.security_analysis import (
        SECRET_FINDING_ALLOWED_ATTRS,
        SecurityAnalysisSource,
    )

    repo = tmp_path / "repo"
    (repo / "src").mkdir(parents=True)
    raw_secret = "raw-super-secret-token"
    artifact = _write_json(
        repo / "reports" / "gitleaks.json",
        [
            {
                "RuleID": "generic-api-key",
                "File": "src/config.py",
                "StartLine": 7,
                "EndLine": 7,
                "Fingerprint": f"fingerprint:{raw_secret}",
                "Entropy": 4.2,
                "Match": f"TOKEN={raw_secret}",
                "Secret": raw_secret,
                "Commit": "abc123",
                "Branch": "main",
                "Author": "should-not-be-stored",
                "Email": "should-not-be-stored@example.test",
            }
        ],
    )

    facts = list(SecurityAnalysisSource(
        repo="demo",
        repo_root=repo,
        artifact_paths=[artifact],
    ).run("run-1", mode="scope_complete").facts)
    secrets = [
        fact for fact in facts
        if isinstance(fact, NodeFact) and fact.subtype == "secret_finding"
    ]
    assert len(secrets) == 1
    attrs = secrets[0].attrs
    serialized = json.dumps(attrs, sort_keys=True)

    assert set(attrs) <= SECRET_FINDING_ALLOWED_ATTRS
    assert raw_secret not in serialized
    assert "should-not-be-stored" not in serialized
    assert attrs["redacted_context"] == "TOKEN=[REDACTED]"
    assert attrs["fingerprint"] != f"fingerprint:{raw_secret}"
    assert attrs["commit"] == "abc123"
    assert attrs["ref"] == "main"


def test_security_analysis_applies_finding_excludes_and_suppressions(
    tmp_path: Path,
) -> None:
    from okg.substrate.library.sources.security_analysis import (
        SecurityAnalysisSource,
    )

    repo = tmp_path / "repo"
    (repo / "tests" / "cms" / "__pycache__").mkdir(parents=True)
    artifact = _write_json(
        repo / "reports" / "gitleaks.json",
        [
            {
                "RuleID": "generic-api-key",
                "File": "tests/cms/__pycache__/fixture.pyc",
                "StartLine": 1,
                "Fingerprint": "fp-generated",
                "Match": "token=raw-generated-secret",
                "Secret": "raw-generated-secret",
            },
            {
                "RuleID": "generic-api-key",
                "File": "tests/cms/test_fixture.py",
                "StartLine": 2,
                "Fingerprint": "fp-fixture",
                "Match": "token=raw-fixture-secret",
                "Secret": "raw-fixture-secret",
            },
            {
                "RuleID": "generic-api-key",
                "File": "src/app.py",
                "StartLine": 3,
                "Fingerprint": "fp-active",
                "Match": "token=raw-active-secret",
                "Secret": "raw-active-secret",
            },
        ],
    )
    suppressions = tmp_path / "suppressions.yaml"
    suppressions.write_text(
        yaml.safe_dump({
            "version": 1,
            "suppressions": [
                {
                    "id": "fixture-secret",
                    "reason": "synthetic CMS fixture token",
                    "subtypes": ["secret_finding"],
                    "tools": ["gitleaks"],
                    "rule_ids": ["generic-api-key"],
                    "path_globs": ["tests/cms/test_*.py"],
                    "start_lines": [2],
                }
            ],
        }),
        encoding="utf-8",
    )

    facts = list(SecurityAnalysisSource(
        repo="demo",
        repo_root=repo,
        artifact_paths=[artifact],
        finding_exclude_globs=["**/__pycache__/**", "**/*.pyc"],
        suppression_paths=[suppressions],
    ).run("run-1", mode="scope_complete").facts)
    secrets = [
        fact for fact in facts
        if isinstance(fact, NodeFact) and fact.subtype == "secret_finding"
    ]
    attrs_by_path = {node.attrs["path"]: node.attrs for node in secrets}

    assert set(attrs_by_path) == {"tests/cms/test_fixture.py", "src/app.py"}
    assert attrs_by_path["tests/cms/test_fixture.py"]["status"] == "suppressed"
    assert attrs_by_path["tests/cms/test_fixture.py"]["baseline_state"] == "accepted"
    assert "fixture-secret" in attrs_by_path["tests/cms/test_fixture.py"]["suppression"]
    assert attrs_by_path["src/app.py"]["status"] == "open"
    assert all(
        node.source_revision.get("finding_policy_hash")
        for node in secrets
    )


def test_security_analysis_source_scopes_to_configured_artifact(
    tmp_path: Path,
) -> None:
    from okg.substrate.library.sources.security_analysis import (
        SecurityAnalysisSource,
    )

    repo = tmp_path / "repo"
    (repo / "src").mkdir(parents=True)
    (repo / "src" / "db.py").write_text("query = input()\n")
    artifacts = _security_artifacts(repo)
    semgrep = repo / "reports" / "semgrep.json"
    gitleaks = repo / "reports" / "gitleaks.json"
    source = SecurityAnalysisSource(
        repo="demo",
        repo_root=repo,
        artifact_paths=artifacts,
    )

    security_run = source.run(
        "run-security",
        mode="scope_incomplete",
        sync_scope={
            "scope_keys": [
                semgrep.relative_to(repo).as_posix(),
                "../escape.json",
            ],
        },
    )
    security_nodes = [
        fact for fact in security_run.facts
        if isinstance(fact, NodeFact)
    ]
    assert security_run.completed_scope is False
    assert {node.subtype for node in security_nodes} == {"security_finding"}
    assert {
        node.attrs["artifact"] for node in security_nodes
    } == {"reports/semgrep.json"}

    secret_run = source.run(
        "run-secret",
        mode="scope_incomplete",
        sync_scope={"scope_keys": [gitleaks.relative_to(repo).as_posix()]},
    )
    secret_nodes = [
        fact for fact in secret_run.facts
        if isinstance(fact, NodeFact)
    ]
    assert secret_run.completed_scope is False
    assert {node.subtype for node in secret_nodes} == {"secret_finding"}
    assert {
        node.attrs["artifact"] for node in secret_nodes
    } == {"reports/gitleaks.json"}


def test_security_analysis_source_ignores_unconfigured_scoped_artifact(
    tmp_path: Path,
) -> None:
    from okg.substrate.library.sources.security_analysis import (
        SecurityAnalysisSource,
    )

    repo = tmp_path / "repo"
    configured = _write_json(
        repo / "reports" / "semgrep.json",
        {"version": "1.0", "results": []},
    )
    unconfigured = _write_json(
        repo / "other" / "semgrep.json",
        {"version": "1.0", "results": [{"check_id": "x", "path": "x.py"}]},
    )
    source = SecurityAnalysisSource(
        repo="demo",
        repo_root=repo,
        artifact_paths=[configured],
    )

    run = source.run(
        "run-unconfigured",
        mode="scope_incomplete",
        sync_scope={"scope_keys": [unconfigured.relative_to(repo).as_posix()]},
    )

    assert list(run.facts) == []
    assert run.completed_scope is False


def test_security_analysis_imports_normalized_code_quality_artifact(
    tmp_path: Path,
) -> None:
    from okg.substrate.library.sources.security_analysis import (
        SecurityAnalysisSource,
    )

    repo = tmp_path / "repo"
    (repo / "src").mkdir(parents=True)
    (repo / "src" / "app.py").write_text("import os\n")
    lane = repo / "runs" / ("a" * 40) / "code-quality"
    lane.mkdir(parents=True)
    (lane / ".okg-ci-artifact.json").write_text(
        json.dumps({
            "schema": "okg-ci.artifact-manifest.v1",
            "repo": "example-org/example-repo",
            "sha": "a" * 40,
            "run_id": 123,
            "run_attempt": 2,
            "workflow_name": "Tests",
            "artifact_name": "ci-code-quality",
            "artifact_id": 678,
            "artifact_node_id": "ci-artifact:github:example-org/example-repo:678",
            "lane": "code-quality",
            "content_kinds": ["code_quality"],
        }),
        encoding="utf-8",
    )
    artifact = _write_json(
        lane / "code-quality" / "code-quality-findings.json",
        {
            "schema": "okg.code_quality.findings.v1",
            "repo": "example-org/example-repo",
            "sha": "a" * 40,
            "ref": "refs/heads/dev",
            "branch": "dev",
            "workflow_name": "Tests",
            "workflow_run_id": "123",
            "run_attempt": "2",
            "findings": [
                {
                    "finding_id": "code-quality-finding:codeql:fixture",
                    "tool": "codeql",
                    "tool_version": "2.21.0",
                    "language": "python",
                    "rule_id": "py/unused-import",
                    "rule_name": "Unused import",
                    "category": "maintainability",
                    "severity": "warning",
                    "status": "open",
                    "path": "src/app.py",
                    "start_line": 1,
                    "end_line": 1,
                    "message": "Import is unused",
                    "fingerprint": "fp-quality",
                    "source_sarif": "python.quality.sarif",
                }
            ],
        },
    )

    facts = list(SecurityAnalysisSource(
        repo="okg",
        repo_root=repo,
        artifact_paths=[artifact],
    ).run("run-1", mode="scope_complete").facts)
    nodes = [fact for fact in facts if isinstance(fact, NodeFact)]
    edges = [fact for fact in facts if isinstance(fact, EdgeFact)]
    quality = [node for node in nodes if node.subtype == "code_quality_finding"]

    assert len(quality) == 1
    attrs = quality[0].attrs
    assert attrs["repo"] == "okg"
    assert attrs["rule_id"] == "py/unused-import"
    assert attrs["path"] == "src/app.py"
    assert attrs["sha"] == "a" * 40
    assert attrs["ci_artifact_node_id"] == "ci-artifact:github:example-org/example-repo:678"
    assert {
        ("affects", "file:okg:src/app.py"),
        ("affects", f"commit:okg:{'a' * 40}"),
    } <= {(edge.edge_type, edge.dst) for edge in edges}


def test_security_analysis_routes_quality_sarif_to_code_quality_finding(
    tmp_path: Path,
) -> None:
    from okg.substrate.library.sources.security_analysis import (
        SecurityAnalysisSource,
    )

    repo = tmp_path / "repo"
    (repo / "src").mkdir(parents=True)
    artifact = _write_json(
        repo / "reports" / "python.quality.sarif",
        {
            "version": "2.1.0",
            "runs": [
                {
                    "tool": {
                        "driver": {
                            "name": "CodeQL",
                            "semanticVersion": "2.21.0",
                            "rules": [
                                {
                                    "id": "py/unused-import",
                                    "name": "Unused import",
                                    "properties": {"tags": ["maintainability"]},
                                }
                            ],
                        }
                    },
                    "results": [
                        {
                            "ruleId": "py/unused-import",
                            "level": "warning",
                            "message": {"text": "Import is unused"},
                            "partialFingerprints": {"primaryLocationLineHash": "fp-quality"},
                            "locations": [
                                {
                                    "physicalLocation": {
                                        "artifactLocation": {"uri": "src/app.py"},
                                        "region": {"startLine": 1, "endLine": 1},
                                    }
                                }
                            ],
                        }
                    ],
                }
            ],
        },
    )

    facts = list(SecurityAnalysisSource(
        repo="okg",
        repo_root=repo,
        artifact_paths=[artifact],
    ).run("run-1", mode="scope_complete").facts)
    nodes = [fact for fact in facts if isinstance(fact, NodeFact)]

    assert {node.subtype for node in nodes} == {"code_quality_finding"}
    assert nodes[0].attrs["category"] == "maintainability"
    assert nodes[0].attrs["fingerprint"] == "fp-quality"


def test_security_analysis_skips_quality_sarif_when_normalized_sibling_exists(
    tmp_path: Path,
) -> None:
    from okg.substrate.library.sources.security_analysis import (
        SecurityAnalysisSource,
    )

    repo = tmp_path / "repo"
    artifact_dir = repo / "reports" / "code-quality"
    json_artifact = _write_json(
        artifact_dir / "code-quality-findings.json",
        {
            "schema": "okg.code_quality.findings.v1",
            "sha": "a" * 40,
            "findings": [
                {
                    "tool": "codeql",
                    "rule_id": "py/unused-import",
                    "rule_name": "Unused import",
                    "category": "maintainability",
                    "severity": "warning",
                    "status": "open",
                    "path": "src/app.py",
                    "start_line": 1,
                    "end_line": 1,
                    "message": "Import is unused",
                    "fingerprint": "fp-quality",
                }
            ],
        },
    )
    sarif_artifact = _write_json(
        artifact_dir / "python.quality.sarif",
        {
            "version": "2.1.0",
            "runs": [
                {
                    "tool": {
                        "driver": {
                            "name": "CodeQL",
                            "rules": [
                                {
                                    "id": "py/unused-import",
                                    "name": "Unused import",
                                    "properties": {"tags": ["maintainability"]},
                                }
                            ],
                        }
                    },
                    "results": [
                        {
                            "ruleId": "py/unused-import",
                            "message": {"text": "Import is unused"},
                            "partialFingerprints": {"primaryLocationLineHash": "fp-quality"},
                            "locations": [
                                {
                                    "physicalLocation": {
                                        "artifactLocation": {"uri": "src/app.py"},
                                        "region": {"startLine": 1},
                                    }
                                }
                            ],
                        }
                    ],
                }
            ],
        },
    )

    facts = list(SecurityAnalysisSource(
        repo="okg",
        repo_root=repo,
        artifact_paths=[json_artifact, sarif_artifact],
    ).run("run-1", mode="scope_complete").facts)
    quality_nodes = [
        fact for fact in facts
        if isinstance(fact, NodeFact) and fact.subtype == "code_quality_finding"
    ]

    assert len(quality_nodes) == 1
    assert quality_nodes[0].attrs["sha"] == "a" * 40


def test_security_analysis_keeps_security_tagged_codeql_sarif_as_security(
    tmp_path: Path,
) -> None:
    from okg.substrate.library.sources.security_analysis import (
        SecurityAnalysisSource,
    )

    repo = tmp_path / "repo"
    artifact = _write_json(
        repo / "reports" / "code-quality" / "python.quality.sarif",
        {
            "version": "2.1.0",
            "runs": [
                {
                    "tool": {
                        "driver": {
                            "name": "CodeQL",
                            "rules": [
                                {
                                    "id": "py/sql-injection",
                                    "name": "SQL injection",
                                    "properties": {
                                        "tags": ["security", "cwe-089"],
                                        "security-severity": "8.8",
                                    },
                                }
                            ],
                        }
                    },
                    "results": [
                        {
                            "ruleId": "py/sql-injection",
                            "level": "error",
                            "message": {"text": "SQL built from user input"},
                            "partialFingerprints": {"primaryLocationLineHash": "fp-security"},
                            "locations": [
                                {
                                    "physicalLocation": {
                                        "artifactLocation": {"uri": "src/db.py"},
                                        "region": {"startLine": 7},
                                    }
                                }
                            ],
                        }
                    ],
                }
            ],
        },
    )

    facts = list(SecurityAnalysisSource(
        repo="okg",
        repo_root=repo,
        artifact_paths=[artifact],
    ).run("run-1", mode="scope_complete").facts)
    nodes = [fact for fact in facts if isinstance(fact, NodeFact)]

    assert {node.subtype for node in nodes} == {"security_finding"}
    assert nodes[0].attrs["rule_id"] == "py/sql-injection"


_CYCLONEDX_XML = """<?xml version="1.0" encoding="UTF-8"?>
<bom xmlns="http://cyclonedx.org/schema/bom/1.5" version="1">
  <metadata>
    <timestamp>2026-04-01T00:00:00Z</timestamp>
  </metadata>
  <components>
    <component type="library">
      <name>left-pad</name>
      <version>1.0.0</version>
      <purl>pkg:npm/left-pad@1.0.0</purl>
    </component>
  </components>
</bom>
"""


def _xml_sbom_source(tmp_path: Path):
    from okg.substrate.library.sources.security_analysis import (
        SecurityAnalysisSource,
    )

    repo = tmp_path / "repo"
    artifact = repo / "reports" / "sbom.xml"
    artifact.parent.mkdir(parents=True)
    artifact.write_text(_CYCLONEDX_XML, encoding="utf-8")
    return SecurityAnalysisSource(
        repo="demo",
        repo_root=repo,
        artifact_paths=[artifact],
    )


def test_xml_sbom_import_fails_loud_when_defusedxml_missing(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import sys

    from okg.substrate.errors import SubstrateError

    source = _xml_sbom_source(tmp_path)
    # None in sys.modules makes `from defusedxml import ...` raise
    # ImportError, simulating an install without the quality extra.
    monkeypatch.setitem(sys.modules, "defusedxml", None)

    with pytest.raises(SubstrateError, match="requires defusedxml") as exc_info:
        list(source.run("run-xml", mode="scope_complete").facts)

    assert exc_info.value.fix_hint
    assert "okg[quality]" in exc_info.value.fix_hint


def test_xml_sbom_artifact_imports_when_defusedxml_present(tmp_path: Path) -> None:
    pytest.importorskip("defusedxml")

    source = _xml_sbom_source(tmp_path)
    facts = list(source.run("run-xml", mode="scope_complete").facts)
    nodes = [fact for fact in facts if isinstance(fact, NodeFact)]
    sboms = [node for node in nodes if node.subtype == "sbom_artifact"]

    assert len(sboms) == 1
    assert sboms[0].attrs["format"] == "cyclonedx_xml"
    assert sboms[0].attrs["component_count"] == 1
    assert sboms[0].attrs["components"] == [{
        "name": "left-pad",
        "version": "1.0.0",
        "purl": "pkg:npm/left-pad@1.0.0",
    }]
    assert sboms[0].attrs["generated_at"] == "2026-04-01T00:00:00Z"


@pytest.mark.postgres
def test_security_analysis_source_publish_projects_quality_nodes(
    clean_runtime: str,
    tmp_path: Path,
) -> None:
    from okg.substrate.catalog.modules_apply import apply_composed_catalog
    from okg.substrate.catalog.modules_compose import compose_catalog
    from okg.substrate.ingest import SourceRunner
    from okg.substrate.library.sources.code_graph import PackageManifestSource
    from okg.substrate.library.sources.git_graph_files import GitFilesSource
    from okg.substrate.library.sources.security_analysis import (
        SecurityAnalysisSource,
    )
    from okg.substrate.publisher import Publisher

    repo = tmp_path / "repo"
    (repo / "src").mkdir(parents=True)
    (repo / "src" / "db.py").write_text("query = input()\n")
    (repo / "pyproject.toml").write_text(
        """
[project]
name = "demo"
dependencies = ["pydantic>=2", "requests==2.0.0"]
""".strip(),
        encoding="utf-8",
    )
    _write_json(
        repo / "package.json",
        {
            "name": "demo",
            "dependencies": {
                "left-pad": "1.0.0",
            },
        },
    )
    artifacts = _security_artifacts(repo)

    dep = tmp_path / "dep"
    dep.mkdir()
    (dep / "deployment.yaml").write_text(yaml.safe_dump({
        "name": "test-security-analysis",
        "description": "security evidence publish fixture",
        "modules": [
            "git_graph",
            "openspec",
            "agent_sessions",
            "compute_env",
            "code_graph",
            "ci",
            "quality_evidence",
        ],
    }))
    (dep / "schemas").mkdir()

    composed = compose_catalog(dep)
    with psycopg.connect(clean_runtime) as conn:
        apply_composed_catalog(conn, composed)

    SourceRunner(clean_runtime, source_name="fixture.git-files").run(
        GitFilesSource(repo="demo", repo_root=repo),
        mode="scope_complete",
    )
    SourceRunner(clean_runtime, source_name="fixture.package-manifests").run(
        PackageManifestSource(repo="demo", repo_root=repo),
        mode="scope_complete",
    )
    SourceRunner(clean_runtime, source_name="fixture.security-analysis").run(
        SecurityAnalysisSource(
            repo="demo",
            repo_root=repo,
            artifact_paths=artifacts,
        ),
        mode="scope_complete",
    )
    out = Publisher(clean_runtime, "test-security-analysis", catalog_identity=bind_test_catalog_writer(clean_runtime, "test-security-analysis")).cycle()
    assert out["status"] == "published", out

    from okg.substrate.graph_store import ChronosGraphStore

    store = ChronosGraphStore.connect(clean_runtime, ensure_schema=False)
    try:
        graph = store.checkout_generation(out["generation_id"])
        counts = {
            str(row["subtype"]): int(row["count"])
            for row in graph.query(
                """
                SELECT subtype, count(*) AS count
                  FROM okg.graph_nodes
                 WHERE subtype = ANY(:subtypes)
                 GROUP BY subtype
                """,
                {"subtypes": [
                    "security_finding",
                    "secret_finding",
                    "dependency_vulnerability",
                    "sbom_artifact",
                ]},
            )
        }
        secret_attrs = [
            dict(row["attrs"] or {})
            for row in graph.query(
                """
                SELECT attrs
                  FROM okg.graph_nodes
                 WHERE subtype = 'secret_finding'
                """
            )
        ]
        dependency_edges = {
            (str(row["edge_type"]), str(row["dst"]))
            for row in graph.query(
                """
                SELECT e.edge_type, e.dst
                  FROM okg.graph_edges e
                  JOIN okg.graph_nodes s ON s.node_id = e.src
                  JOIN okg.graph_nodes d ON d.node_id = e.dst
                 WHERE s.subtype IN (
                         'dependency_vulnerability',
                         'sbom_artifact'
                       )
                   AND d.subtype = 'dependency_package'
                """
            )
        }
    finally:
        store.close()

    assert counts["security_finding"] >= 4
    assert counts["secret_finding"] >= 1
    assert counts["dependency_vulnerability"] >= 3
    assert counts["sbom_artifact"] == 1
    serialized_secrets = json.dumps(secret_attrs, sort_keys=True)
    assert "raw-super-secret-token" not in serialized_secrets
    assert "[REDACTED]" in serialized_secrets
    assert {
        ("affects", "dep:python:pydantic"),
        ("affects", "dep:npm:left-pad"),
        ("affects", "dep:python:requests"),
        ("references", "dep:python:pydantic"),
    } <= dependency_edges


def _security_artifacts(repo: Path) -> list[Path]:
    return [
        _write_json(
            repo / "reports" / "codeql.sarif",
            {
                "version": "2.1.0",
                "runs": [
                    {
                        "tool": {
                            "driver": {
                                "name": "CodeQL",
                                "semanticVersion": "2.17.0",
                                "rules": [
                                    {
                                        "id": "py/sql-injection",
                                        "version": "1.0",
                                        "name": "SQL injection",
                                        "helpUri": "https://example.test/rules/sql",
                                        "properties": {
                                            "tags": ["security", "cwe-089"],
                                            "security-severity": "8.8",
                                        },
                                    }
                                ],
                            }
                        },
                        "results": [
                            {
                                "ruleId": "py/sql-injection",
                                "level": "error",
                                "message": {"text": "SQL built from user input"},
                                "baselineState": "new",
                                "partialFingerprints": {"primaryLocationLineHash": "abc123"},
                                "locations": [
                                    {
                                        "physicalLocation": {
                                            "artifactLocation": {"uri": "src/db.py"},
                                            "region": {"startLine": 1, "endLine": 1},
                                        }
                                    }
                                ],
                            }
                        ],
                    }
                ],
            },
        ),
        _write_json(
            repo / "reports" / "semgrep.json",
            {
                "version": "1.2.3",
                "results": [
                    {
                        "check_id": "python.lang.security.audit.dangerous-subprocess",
                        "path": "src/db.py",
                        "start": {"line": 1},
                        "end": {"line": 1},
                        "extra": {
                            "severity": "WARNING",
                            "message": "Dangerous subprocess call",
                            "fingerprint": "sg-1",
                            "metadata": {
                                "cwe": ["CWE-078"],
                                "confidence": "high",
                            },
                        },
                    }
                ],
            },
        ),
        _write_json(
            repo / "reports" / "bandit.json",
            {
                "generated_at": "2026-05-02T00:00:00Z",
                "results": [
                    {
                        "filename": "src/db.py",
                        "line_number": 1,
                        "test_id": "B608",
                        "issue_severity": "MEDIUM",
                        "issue_confidence": "HIGH",
                        "issue_text": "Possible SQL injection vector",
                        "issue_cwe": {"id": 89},
                    }
                ],
                "metrics": {},
            },
        ),
        _write_json(
            repo / "reports" / "gitleaks.json",
            [
                {
                    "RuleID": "generic-api-key",
                    "File": "src/db.py",
                    "StartLine": 1,
                    "Fingerprint": "fp-secret",
                    "Entropy": 4.2,
                    "Match": "token=raw-super-secret-token",
                    "Secret": "raw-super-secret-token",
                    "Commit": "abc",
                }
            ],
        ),
        _write_json(
            repo / "reports" / "pip-audit.json",
            {
                "dependencies": [
                    {
                        "name": "pydantic",
                        "version": "2.8.0",
                        "vulns": [
                            {
                                "id": "PYSEC-2026-1",
                                "aliases": ["CVE-2026-0001"],
                                "fix_versions": ["2.9.0"],
                                "severity": "high",
                                "cvss": 8.1,
                                "published_at": "2026-04-01T00:00:00Z",
                                "modified_at": "2026-04-02T00:00:00Z",
                                "description": "demo vuln",
                            }
                        ],
                    }
                ]
            },
        ),
        _write_json(
            repo / "reports" / "osv.json",
            {
                "results": [
                    {
                        "packages": [
                            {
                                "package": {
                                    "name": "left-pad",
                                    "ecosystem": "npm",
                                    "version": "1.0.0",
                                },
                                "vulnerabilities": [
                                    {
                                        "id": "GHSA-xxxx-yyyy-zzzz",
                                        "aliases": ["CVE-2026-1111"],
                                        "summary": "demo npm vuln",
                                        "published": "2026-04-03T00:00:00Z",
                                        "modified": "2026-04-04T00:00:00Z",
                                        "severity": [{"type": "CVSS_V3", "score": "7.5"}],
                                        "affected": [
                                            {
                                                "ranges": [
                                                    {
                                                        "type": "SEMVER",
                                                        "events": [{"fixed": "1.1.0"}],
                                                    }
                                                ]
                                            }
                                        ],
                                    }
                                ],
                            }
                        ]
                    }
                ]
            },
        ),
        _write_json(
            repo / "reports" / "trivy.json",
            {
                "Metadata": {"Version": "0.50.0"},
                "Results": [
                    {
                        "Target": "requirements.txt",
                        "Type": "python-pkg",
                        "Vulnerabilities": [
                            {
                                "VulnerabilityID": "CVE-2026-0001",
                                "PkgName": "requests",
                                "InstalledVersion": "2.0.0",
                                "FixedVersion": "2.1.0",
                                "Severity": "CRITICAL",
                                "CVSS": {"nvd": {"V3Score": 9.8}},
                                "PublishedDate": "2026-04-05T00:00:00Z",
                                "LastModifiedDate": "2026-04-06T00:00:00Z",
                                "Title": "demo trivy vuln",
                            }
                        ],
                        "Misconfigurations": [
                            {
                                "ID": "AVD-DS-0001",
                                "FilePath": "src/db.py",
                                "Severity": "HIGH",
                                "Message": "Dockerfile misconfig",
                                "CauseMetadata": {"StartLine": 1},
                            }
                        ],
                    }
                ],
            },
        ),
        _write_json(
            repo / "reports" / "bom.json",
            {
                "bomFormat": "CycloneDX",
                "metadata": {
                    "timestamp": "2026-05-02T00:00:00Z",
                    "tools": [{"name": "cyclonedx-py", "version": "1.0.0"}],
                },
                "components": [
                    {
                        "name": "pydantic",
                        "version": "2.8.0",
                        "purl": "pkg:pypi/pydantic@2.8.0",
                    }
                ],
            },
        ),
    ]
