"""Two focused runs, ingested separately, must still be able to prove a
requirement between them.

The normal loop runs a subset, ingests, runs another subset, ingests. If a
partial report could never yield proof, that loop would stop working — which
would be a worse defect than the one the named-test check exists to close.
"""

from __future__ import annotations

from pathlib import Path

import yaml

from okg.substrate.pact.operations import ingest_pytest_report, load_manifest
from okg.substrate.pact import validation
from okg.substrate.pact.evidence_kinds import DEFAULT_REGISTRY
from okg.substrate.pact.models import AcceptanceCriterion

REQ = "req.acc.r1"


def _seed(root: Path) -> None:
    change = root / "pact" / "changes" / "acc"
    change.mkdir(parents=True)
    manifest = {
        "pact_version": 0,
        "change": {
            "id": "acc",
            "title": "acc",
            "status": "proposal",
            "rationale": "test",
            "root_cause": "test",
        },
        "requirements": [
            {
                "id": REQ,
                "title": "r1",
                "statement": "MUST test.",
                "evidence_policy": {"required": ["executed_test"]},
            }
        ],
        "tasks": [
            {
                "id": "task.acc.t1",
                "title": "t1",
                "status": "todo",
                "verification": "pytest",
                "requirements": [REQ],
                "evidence": [],
            }
        ],
        "test_plan": [
            {
                "id": "test.acc.a",
                "requirement": REQ,
                "test_id": "tests/a.py::test_a",
                "evidence": ["pytest"],
            },
            {
                "id": "test.acc.b",
                "requirement": REQ,
                "test_id": "tests/b.py::test_b",
                "evidence": ["pytest"],
            },
        ],
        "evidence": [],
    }
    (change / "pact.yaml").write_text(yaml.safe_dump(manifest), encoding="utf-8")


def _report(path: Path, classname: str, name: str) -> Path:
    path.write_text(
        f'<testsuite tests="1"><testcase classname="{classname}" name="{name}"'
        ' time="0.1"/></testsuite>',
        encoding="utf-8",
    )
    return path


def test_a_report_covering_every_named_test_proves_the_requirement(
    tmp_path: Path,
) -> None:
    # A requirement whose named tests span two files is proven by ONE report
    # that ran them both. Splitting them across reports is not sufficient: a
    # report that speaks to the requirement at all is answerable for every test
    # it names, because a file contributing no case may have been dropped
    # entirely rather than left out on purpose.
    _seed(tmp_path)
    report = tmp_path / "both.xml"
    report.write_text(
        '<testsuite tests="2">'
        '<testcase classname="tests/a.py" name="test_a" time="0.1"/>'
        '<testcase classname="tests/b.py" name="test_b" time="0.1"/>'
        "</testsuite>",
        encoding="utf-8",
    )

    ingest_pytest_report(tmp_path, "acc", report)

    manifest = load_manifest(tmp_path, "acc")
    rows = [row for row in manifest.evidence if row.kind == "pytest"]
    assert [row.status for row in rows] == ["pass"]
    assert validation._has_evidence(manifest, REQ, {"pytest"}) is True


def test_alignment_reuses_the_resolved_evidence_registry(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _seed(tmp_path)
    report = tmp_path / "both.xml"
    report.write_text(
        '<testsuite tests="2">'
        '<testcase classname="tests/a.py" name="test_a" time="0.1"/>'
        '<testcase classname="tests/b.py" name="test_b" time="0.1"/>'
        "</testsuite>",
        encoding="utf-8",
    )
    ingest_pytest_report(tmp_path, "acc", report)
    manifest = load_manifest(tmp_path, "acc")
    manifest.requirements[0].acceptance = [
        AcceptanceCriterion(
            id="acc.registry",
            description="The report-backed proof is reusable.",
            evidence=["executed_test"],
        )
    ]

    def unexpected_registry_reload(*_args, **_kwargs):
        raise AssertionError("the resolved registry must flow through matching")

    monkeypatch.setattr(validation, "_kind_registry", unexpected_registry_reload)

    assert validation._has_evidence(
        manifest,
        REQ,
        {"pytest"},
        registry=DEFAULT_REGISTRY,
    )
    alignment = validation._acceptance_alignment(
        manifest,
        registry=DEFAULT_REGISTRY,
    )
    assert alignment["acc.registry"]["satisfaction_status"] == "satisfied"


def test_a_report_that_ran_only_half_the_named_tests_does_not_prove_it(
    tmp_path: Path,
) -> None:
    _seed(tmp_path)
    first = _report(tmp_path / "a.xml", "tests/a.py", "test_a")

    ingest_pytest_report(tmp_path, "acc", first)

    manifest = load_manifest(tmp_path, "acc")
    assert validation._has_evidence(manifest, REQ, {"pytest"}) is False


def test_a_report_about_unrelated_work_leaves_the_requirement_alone(
    tmp_path: Path,
) -> None:
    _seed(tmp_path)
    other = _report(tmp_path / "o.xml", "tests/elsewhere.py", "test_other")

    ingest_pytest_report(tmp_path, "acc", other)

    manifest = load_manifest(tmp_path, "acc")
    assert validation._has_evidence(manifest, REQ, {"pytest"}) is False


def test_a_focused_report_still_names_a_test_its_own_file_dropped(
    tmp_path: Path,
) -> None:
    # Scope does not become an excuse: within a file the report DID run, a named
    # test that did not run is still a broken promise.
    _seed(tmp_path)
    report = _report(tmp_path / "a.xml", "tests/a.py", "test_something_else")

    ingest_pytest_report(tmp_path, "acc", report)

    manifest = load_manifest(tmp_path, "acc")
    rows = [row for row in manifest.evidence if row.kind == "pytest"]
    if rows:
        assert all(row.status != "pass" for row in rows)
    assert validation._has_evidence(manifest, REQ, {"pytest"}) is False
