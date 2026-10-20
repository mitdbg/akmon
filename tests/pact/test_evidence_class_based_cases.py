"""Regression: a plan entry must bind class-based junit cases.

Issue #1461. A file-level plan entry used to compare its file key against the
junit `classname` by equality. pytest spells a class-based case as
`classname="tests.pact.test_x.TestThing"`, so every such case fell through and
was counted as neither pass, fail nor skip: a file naming a failing class-based
test produced evidence byte-identical to a clean run. Class-level entries
(`file.py::TestThing`) bound nothing at all.
"""

from __future__ import annotations

from pathlib import Path

from okg.substrate.cli.main import main
from okg.substrate.pact import operations as pact_operations
from okg.substrate.pact.store import load_manifest


def _seed_root(tmp_path: Path) -> Path:
    root = tmp_path
    (root / "pact" / "schemas").mkdir(parents=True)
    (root / "pact" / "AGENTS.md").write_text("# PACT\n", encoding="utf-8")
    (root / "pact" / "project.yaml").write_text(
        "pact_version: 4\n"
        "project: okg\n"
        "pact_defaults:\n"
        "  artifact_format_version: 4\n"
        "  target_pact_version: 4\n",
        encoding="utf-8",
    )
    (root / "pact" / "schemas" / "pact.schema.yaml").write_text(
        "pact_schema_version: 0\n", encoding="utf-8"
    )
    assert (
        main(
            [
                "pact",
                "--root",
                str(root),
                "new-change",
                "demo",
                "--template",
                "protocol",
                "--json",
            ]
        )
        == 0
    )
    pact_operations.add_requirement(
        root,
        "demo",
        req_id="req.demo",
        title="Demo",
        statement="Demo must validate.",
        evidence=["pytest"],
    )
    return root


def _write_report(path: Path, cases: str) -> Path:
    path.write_text(f"<testsuite>{cases}</testsuite>", encoding="utf-8")
    return path


_MODULE_CASE = (
    '<testcase classname="tests.pact.test_target" name="test_module_level"/>'
)
_CLASS_CASE = (
    '<testcase classname="tests.pact.test_target.TestThing" name="test_in_class"/>'
)
_NESTED_CASE = (
    '<testcase classname="tests.pact.test_target.TestThing.TestInner"'
    ' name="test_nested"/>'
)


class TestFileLevelPlanEntry:
    """A file entry covers every case in the file."""

    def test_counts_module_class_and_nested_cases(self, tmp_path: Path) -> None:
        root = _seed_root(tmp_path)
        pact_operations.require_test(
            root, "demo", "req.demo", "tests/pact/test_target.py"
        )
        report = _write_report(
            tmp_path / "junit.xml", _MODULE_CASE + _CLASS_CASE + _NESTED_CASE
        )

        pact_operations.ingest_pytest_report(root, "demo", report)

        rows = load_manifest(root, "demo").evidence
        assert len(rows) == 1
        assert rows[0].status == "pass"
        assert rows[0].data["counts"] == {"pass": 3, "fail": 0, "skip": 0}

    def test_failing_class_based_case_fails_the_evidence(self, tmp_path: Path) -> None:
        root = _seed_root(tmp_path)
        pact_operations.require_test(
            root, "demo", "req.demo", "tests/pact/test_target.py"
        )
        failing = (
            '<testcase classname="tests.pact.test_target.TestThing"'
            ' name="test_in_class"><failure message="boom"/></testcase>'
        )
        report = _write_report(tmp_path / "junit.xml", _MODULE_CASE + failing)

        pact_operations.ingest_pytest_report(root, "demo", report)

        rows = load_manifest(root, "demo").evidence
        assert rows[0].status == "fail"
        assert rows[0].data["counts"] == {"pass": 1, "fail": 1, "skip": 0}
        assert "tests.pact.test_target.TestThing::test_in_class" in (
            rows[0].data["failed_test_ids"]
        )

    def test_skipped_class_based_case_is_counted(self, tmp_path: Path) -> None:
        root = _seed_root(tmp_path)
        pact_operations.require_test(
            root, "demo", "req.demo", "tests/pact/test_target.py"
        )
        skipped = (
            '<testcase classname="tests.pact.test_target.TestThing"'
            ' name="test_in_class"><skipped/></testcase>'
        )
        report = _write_report(tmp_path / "junit.xml", _MODULE_CASE + skipped)

        pact_operations.ingest_pytest_report(root, "demo", report)

        rows = load_manifest(root, "demo").evidence
        assert rows[0].data["counts"] == {"pass": 1, "fail": 0, "skip": 1}

    def test_case_from_another_file_is_not_counted(self, tmp_path: Path) -> None:
        root = _seed_root(tmp_path)
        pact_operations.require_test(
            root, "demo", "req.demo", "tests/pact/test_target.py"
        )
        other = (
            '<testcase classname="tests.pact.test_target_extra.TestThing"'
            ' name="test_in_class"/>'
        )
        report = _write_report(tmp_path / "junit.xml", _MODULE_CASE + other)

        pact_operations.ingest_pytest_report(root, "demo", report)

        rows = load_manifest(root, "demo").evidence
        assert rows[0].data["counts"] == {"pass": 1, "fail": 0, "skip": 0}


class TestClassLevelPlanEntry:
    """`file.py::TestThing` binds the class, not nothing."""

    def test_class_entry_binds_its_methods(self, tmp_path: Path) -> None:
        root = _seed_root(tmp_path)
        pact_operations.require_test(
            root, "demo", "req.demo", "tests/pact/test_target.py::TestThing"
        )
        report = _write_report(
            tmp_path / "junit.xml", _MODULE_CASE + _CLASS_CASE + _NESTED_CASE
        )

        pact_operations.ingest_pytest_report(root, "demo", report)

        rows = load_manifest(root, "demo").evidence
        assert rows[0].status == "pass"
        # The module-level case is outside the class and must not be counted.
        assert rows[0].data["counts"] == {"pass": 2, "fail": 0, "skip": 0}

    def test_class_entry_that_never_ran_is_not_proven(self, tmp_path: Path) -> None:
        root = _seed_root(tmp_path)
        pact_operations.require_test(
            root, "demo", "req.demo", "tests/pact/test_target.py::TestThing"
        )
        report = _write_report(tmp_path / "junit.xml", _MODULE_CASE)

        pact_operations.ingest_pytest_report(root, "demo", report)

        rows = load_manifest(root, "demo").evidence
        assert rows == [] or all(row.status != "pass" for row in rows)


class TestMethodEntryStaysStrict:
    def test_same_named_method_in_another_class_does_not_satisfy(
        self, tmp_path: Path
    ) -> None:
        root = _seed_root(tmp_path)
        pact_operations.require_test(
            root,
            "demo",
            "req.demo",
            "tests/pact/test_target.py::TestThing::test_in_class",
        )
        wrong_class = (
            '<testcase classname="tests.pact.test_target.TestOther"'
            ' name="test_in_class"/>'
        )
        report = _write_report(tmp_path / "junit.xml", wrong_class)

        pact_operations.ingest_pytest_report(root, "demo", report)

        rows = load_manifest(root, "demo").evidence
        assert all(row.status != "pass" for row in rows)

    def test_a_parametrised_case_satisfies_its_unparametrised_entry(
        self, tmp_path: Path
    ) -> None:
        root = _seed_root(tmp_path)
        pact_operations.require_test(
            root,
            "demo",
            "req.demo",
            "tests/pact/test_target.py::TestThing::test_in_class",
        )
        parametrised = (
            '<testcase classname="tests.pact.test_target.TestThing"'
            ' name="test_in_class[a::b]"/>'
        )
        report = _write_report(tmp_path / "junit.xml", parametrised)

        pact_operations.ingest_pytest_report(root, "demo", report)

        rows = load_manifest(root, "demo").evidence
        assert rows[0].status == "pass"

    def test_the_named_method_satisfies_its_entry(self, tmp_path: Path) -> None:
        root = _seed_root(tmp_path)
        pact_operations.require_test(
            root,
            "demo",
            "req.demo",
            "tests/pact/test_target.py::TestThing::test_in_class",
        )
        report = _write_report(tmp_path / "junit.xml", _CLASS_CASE)

        pact_operations.ingest_pytest_report(root, "demo", report)

        rows = load_manifest(root, "demo").evidence
        assert rows[0].status == "pass"


class TestUnmatchedCaseVisibility:
    def test_unmatched_cases_are_reported(self, tmp_path: Path) -> None:
        root = _seed_root(tmp_path)
        pact_operations.require_test(
            root, "demo", "req.demo", "tests/pact/test_target.py"
        )
        stranger = (
            '<testcase classname="tests.pact.test_stranger" name="test_stranger">'
            '<properties><property name="pact_requirement" value="req.demo"/>'
            "</properties></testcase>"
        )
        report = _write_report(tmp_path / "junit.xml", _CLASS_CASE + stranger)

        pact_operations.ingest_pytest_report(root, "demo", report)

        rows = load_manifest(root, "demo").evidence
        assert rows[0].data["report_cases"] == 2
        assert rows[0].data["report_cases_unmatched_by_plan"] == 1
        assert "whole report: 1 of 2 cases matched no plan entry" in rows[0].summary

    def test_fully_matched_report_says_nothing_extra(self, tmp_path: Path) -> None:
        root = _seed_root(tmp_path)
        pact_operations.require_test(
            root, "demo", "req.demo", "tests/pact/test_target.py"
        )
        report = _write_report(tmp_path / "junit.xml", _MODULE_CASE + _CLASS_CASE)

        pact_operations.ingest_pytest_report(root, "demo", report)

        rows = load_manifest(root, "demo").evidence
        assert rows[0].data["report_cases_unmatched_by_plan"] == 0
        assert "matched no plan entry" not in rows[0].summary
