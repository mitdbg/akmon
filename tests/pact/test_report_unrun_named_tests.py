"""The report over already-recorded evidence must find a named test that did not
run, and must not invent one."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import yaml

SCRIPT = (
    Path(__file__).resolve().parents[2]
    / "scripts"
    / "pact"
    / "report_unrun_named_tests.py"
)
spec = importlib.util.spec_from_file_location("report_unrun_named_tests", SCRIPT)
assert spec is not None and spec.loader is not None
reporter = importlib.util.module_from_spec(spec)
sys.modules["report_unrun_named_tests"] = reporter
spec.loader.exec_module(reporter)


def _write(
    root: Path,
    *,
    planned: str,
    junit: list[str],
    skipped: list[str],
    kind: str = "pytest",
    status: str = "pass",
    legacy_junit: str | None = None,
) -> None:
    change = root / "pact" / "changes" / "c"
    change.mkdir(parents=True)
    manifest = {
        "change": {"id": "c"},
        "test_plan": [
            {"id": "test.c.named", "requirement": "req.c.r1", "test_id": planned}
        ],
        "evidence": [
            {
                "id": "ev.pytest.c",
                "kind": kind,
                "status": status,
                "refs": ["req.c.r1"],
                "data": {
                    "junit_test_ids": junit,
                    "skipped_test_ids": skipped,
                    "planned_tests": ["test.c.named"],
                    **({"junit_test_id": legacy_junit} if legacy_junit else {}),
                },
            }
        ],
    }
    (change / "pact.yaml").write_text(yaml.safe_dump(manifest), encoding="utf-8")


def test_evidence_whose_named_test_never_appeared_is_reported(tmp_path: Path) -> None:
    _write(
        tmp_path,
        planned="tests/t.py::test_promised",
        junit=["tests.t::test_something_else"],
        skipped=[],
    )

    findings = reporter.scan(tmp_path)

    assert [item["reason"] for item in findings] == ["absent"]
    assert findings[0]["named_test"] == "tests/t.py::test_promised"


def test_evidence_whose_named_test_was_skipped_is_reported_as_skipped(
    tmp_path: Path,
) -> None:
    _write(
        tmp_path,
        planned="tests/t.py::test_promised",
        junit=["tests.t::test_promised"],
        skipped=["tests.t::test_promised"],
    )

    findings = reporter.scan(tmp_path)

    assert [item["reason"] for item in findings] == ["skipped"]


def test_evidence_that_ran_its_named_test_is_not_reported(tmp_path: Path) -> None:
    # The report must not manufacture findings; a differently-spelled junit id
    # for the same test is still that test.
    _write(
        tmp_path,
        planned="tests/t.py::test_promised",
        junit=["tests.t::test_promised"],
        skipped=[],
    )

    assert reporter.scan(tmp_path) == []


def test_hand_attached_evidence_with_no_report_is_reported_unattested(
    tmp_path: Path,
) -> None:
    # Evidence carrying no junit at all says nothing about whether a test ran.
    # It must be visibly unproven rather than omitted from the report.
    _write(tmp_path, planned="tests/t.py::test_promised", junit=[], skipped=[])

    assert [item["reason"] for item in reporter.scan(tmp_path)] == ["unattested"]


def test_legacy_singular_junit_identity_is_not_reported_missing(tmp_path: Path) -> None:
    _write(
        tmp_path,
        planned="tests/t.py::test_promised",
        junit=[],
        skipped=[],
        legacy_junit="tests.t::test_promised",
    )

    assert reporter.scan(tmp_path) == []


def test_project_executed_test_alias_is_scanned(tmp_path: Path) -> None:
    (tmp_path / "pact").mkdir()
    (tmp_path / "pact" / "project.yaml").write_text(
        "evidence_kinds:\n  aliases:\n    proof_run: executed_test\n",
        encoding="utf-8",
    )
    _write(
        tmp_path,
        planned="tests/t.py::test_promised",
        junit=[],
        skipped=[],
        kind="proof_run",
    )

    assert [item["reason"] for item in reporter.scan(tmp_path)] == ["unattested"]


def test_waived_test_row_without_junit_is_still_reported(tmp_path: Path) -> None:
    _write(
        tmp_path,
        planned="tests/t.py::test_promised",
        junit=[],
        skipped=[],
        status="waived",
    )

    assert [item["reason"] for item in reporter.scan(tmp_path)] == ["unattested"]
