"""A requirement is not proven by a report in which the test its plan names
never ran.

The plan entry names one assertion as the proof. A report can fail to contain it
two ways: the test ran and was skipped, or it never reached the report at all —
deselected by a marker filter, dropped by a collect-ignore, or never collected
for a missing fixture. The second is the dangerous one, because nothing in a
rollup over what the report DOES contain can notice what it does not.
"""

from __future__ import annotations

from pathlib import Path

import yaml

from okg.substrate.pact.operations import ingest_pytest_report, load_manifest

REQ = "req.named.r1"


def _seed(root: Path, *, planned: list[str]) -> None:
    change_dir = root / "pact" / "changes" / "named"
    change_dir.mkdir(parents=True)
    manifest = {
        "pact_version": 0,
        "change": {
            "id": "named",
            "title": "named",
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
                "id": "task.named.t1",
                "title": "t1",
                "status": "todo",
                "verification": "pytest",
                "requirements": [REQ],
                "evidence": [],
            }
        ],
        "test_plan": [
            {
                "id": f"test.named.{index}",
                "requirement": REQ,
                "test_id": test_id,
                "evidence": ["pytest"],
            }
            for index, test_id in enumerate(planned)
        ],
        "evidence": [],
    }
    (change_dir / "pact.yaml").write_text(yaml.safe_dump(manifest), encoding="utf-8")


def _report(root: Path, cases: list[tuple[str, str, str]]) -> Path:
    """cases: (classname, name, outcome) where outcome is pass/skip/fail."""
    body = []
    for classname, name, outcome in cases:
        inner = {
            "pass": "",
            "skip": "<skipped message='no model'/>",
            "fail": "<failure message='boom'/>",
        }[outcome]
        body.append(
            f'<testcase classname="{classname}" name="{name}" time="0.1">{inner}</testcase>'
        )
    path = root / "junit.xml"
    path.write_text(
        f'<testsuite tests="{len(cases)}">{"".join(body)}</testsuite>', encoding="utf-8"
    )
    return path


def _ingest(root: Path) -> dict | None:
    ingest_pytest_report(root, "named", root / "junit.xml")
    manifest = load_manifest(root, "named")
    rows = [row for row in manifest.evidence if row.kind == "pytest"]
    if not rows:
        # No row at all is also "not proven" — the report said nothing about
        # this requirement.
        return None
    assert len(rows) == 1, rows
    return {
        "status": rows[0].status,
        "refs": list(rows[0].refs),
        "summary": rows[0].summary,
        "unrun": rows[0].data.get("unrun_planned_tests", []),
    }


def test_a_deselected_named_test_does_not_satisfy_its_plan_entry(tmp_path: Path) -> None:
    # The real shape: `-m "not slow"` drops the named test, so it is absent from
    # the report entirely while its file's other tests pass.
    _seed(tmp_path, planned=["tests/t.py::test_slow_one", "tests/t.py::test_fast_one"])
    _report(tmp_path, [("tests/t.py", "test_fast_one", "pass")])

    result = _ingest(tmp_path)

    assert result["status"] == "missing"
    assert "test.named.0" not in result["refs"]
    assert [item["reason"] for item in result["unrun"]] == ["absent"]
    assert "tests/t.py::test_slow_one (absent)" in result["summary"]


def test_a_skipped_named_test_does_not_satisfy_its_plan_entry(tmp_path: Path) -> None:
    _seed(tmp_path, planned=["tests/t.py::test_slow_one", "tests/t.py::test_fast_one"])
    _report(
        tmp_path,
        [
            ("tests/t.py", "test_slow_one", "skip"),
            ("tests/t.py", "test_fast_one", "pass"),
        ],
    )

    result = _ingest(tmp_path)

    assert result["status"] == "missing"
    assert "test.named.0" not in result["refs"]
    # Skipped and absent are both unrun, and are reported differently so a reader
    # can tell whether the test was even collected.
    assert [item["reason"] for item in result["unrun"]] == ["skipped"]


def test_a_passing_sibling_does_not_cover_an_unrun_named_test(tmp_path: Path) -> None:
    _seed(tmp_path, planned=["tests/t.py::test_promised"])
    _report(tmp_path, [("tests/t.py", "test_unrelated_but_passing", "pass")])

    result = _ingest(tmp_path)

    # A test the plan never named cannot prove the requirement, so the report
    # yields no passing evidence for it at all.
    assert result is None or result["status"] != "pass"


def test_a_report_that_ran_every_named_test_still_proves_the_requirement(
    tmp_path: Path,
) -> None:
    # The change must not simply refuse everything.
    _seed(tmp_path, planned=["tests/t.py::test_slow_one", "tests/t.py::test_fast_one"])
    _report(
        tmp_path,
        [
            ("tests/t.py", "test_slow_one", "pass"),
            ("tests/t.py", "test_fast_one", "pass"),
        ],
    )

    result = _ingest(tmp_path)

    assert result["status"] == "pass"
    assert result["unrun"] == []
    assert {"test.named.0", "test.named.1"} <= set(result["refs"])


def test_a_failing_named_test_still_fails_rather_than_reading_as_unrun(
    tmp_path: Path,
) -> None:
    # The pre-existing fail-dominant rule must survive: a failure is a failure,
    # not merely an unkept promise.
    _seed(tmp_path, planned=["tests/t.py::test_promised"])
    _report(tmp_path, [("tests/t.py", "test_promised", "fail")])

    result = _ingest(tmp_path)

    assert result["status"] == "fail"


def test_a_non_pytest_plan_entry_never_counts_as_an_unrun_test(tmp_path: Path) -> None:
    # Plan entries also name design reviews, graph reports and browser checks with
    # a `scheme::` spelling. No pytest report can ever contain one, so treating
    # them as tests a report failed to run would make their requirements
    # permanently unprovable by any pytest run at all.
    _seed(tmp_path, planned=["design::some-decision", "tests/t.py::test_real"])
    _report(tmp_path, [("tests/t.py", "test_real", "pass")])

    result = _ingest(tmp_path)

    assert result is not None
    assert result["status"] == "pass"
    assert result["unrun"] == []


def test_a_same_named_test_in_another_file_does_not_satisfy_the_named_one(
    tmp_path: Path,
) -> None:
    # Same-named test functions across files are routine. Accepting one for the
    # other would let a test that never ran be recorded as the proof.
    _seed(tmp_path, planned=["tests/t.py::test_roundtrip"])
    _report(tmp_path, [("tests.other", "test_roundtrip", "pass")])

    result = _ingest(tmp_path)

    assert result is None or result["status"] != "pass"


def test_a_report_about_other_work_does_not_demote_this_requirement(
    tmp_path: Path,
) -> None:
    # A report touching none of the requirement's named files says nothing about
    # it, and must not be turned into a verdict on it.
    _seed(tmp_path, planned=["tests/a.py::test_here"])
    _report(tmp_path, [("tests/unrelated.py", "test_other", "pass")])

    result = _ingest(tmp_path)

    assert result is None or result["status"] != "pass"


def test_a_file_whose_every_test_was_dropped_is_still_caught(tmp_path: Path) -> None:
    # The whole-file version of the defect: a module where every test is
    # slow-marked (or an importorskip, or a collect-ignore) contributes NO case,
    # so scoping by file would excuse exactly the case this check exists for.
    _seed(tmp_path, planned=["tests/slow.py::test_slow_one", "tests/fast.py::test_fast_one"])
    _report(tmp_path, [("tests/fast.py", "test_fast_one", "pass")])

    result = _ingest(tmp_path)

    assert result is not None
    assert result["status"] == "missing"
    assert [item["reason"] for item in result["unrun"]] == ["absent"]


def test_a_class_qualified_plan_entry_is_matched(tmp_path: Path) -> None:
    # junit puts the class in the classname and the bare function in the name.
    _seed(tmp_path, planned=["tests/t.py::TestThing::test_y"])
    _report(tmp_path, [("tests.t.TestThing", "test_y", "pass")])

    result = _ingest(tmp_path)

    assert result is not None
    assert result["status"] == "pass"
    assert result["unrun"] == []


def test_a_parametrised_named_test_is_matched_by_its_base_name(tmp_path: Path) -> None:
    # junit emits `test_p[a]`; a plan entry spelled without the parameter must
    # still be recognised as having run.
    _seed(tmp_path, planned=["tests/t.py::test_p"])
    _report(tmp_path, [("tests/t.py", "test_p[a]", "pass")])

    result = _ingest(tmp_path)

    assert result is not None
    assert result["status"] == "pass"
    assert result["unrun"] == []


def test_a_plan_entry_naming_a_whole_file_is_not_treated_as_a_broken_promise(
    tmp_path: Path,
) -> None:
    # Only an entry naming ONE test promises a specific assertion. A file-level
    # entry makes no such claim, so it must not demote a report that otherwise
    # ran everything it named.
    _seed(tmp_path, planned=["tests/t.py", "tests/t.py::test_promised"])
    _report(tmp_path, [("tests/t.py", "test_promised", "pass")])

    result = _ingest(tmp_path)

    assert result is not None
    assert result["status"] == "pass"
    assert result["unrun"] == []
