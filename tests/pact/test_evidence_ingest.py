from __future__ import annotations

import json
from pathlib import Path

import yaml

from okg.substrate.cli.main import main
from okg.substrate.pact import operations as pact_operations
from okg.substrate.pact.store import file_digest, load_manifest
from okg.substrate.pact.validation import validate_manifest


def _json_out(capsys):
    lines = [line for line in capsys.readouterr().out.splitlines() if line.strip()]
    return json.loads(lines[-1])


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


def test_pytest_report_attaches_requirement_evidence(
    tmp_path: Path,
    capsys,
) -> None:
    root = _seed_root(tmp_path)
    report = tmp_path / "junit.xml"
    report.write_text(
        """
        <testsuite>
          <testcase classname="tests.pact.test_demo" name="test_demo">
            <properties>
              <property name="pact_requirement" value="req.demo"/>
            </properties>
          </testcase>
        </testsuite>
        """,
        encoding="utf-8",
    )

    rc = main(
        [
            "pact",
            "--root",
            str(root),
            "evidence",
            "ingest",
            "demo",
            "--pytest-report",
            str(report),
            "--json",
        ]
    )
    payload = _json_out(capsys)

    # no reachable ledger here, so the closure is queued: deferred, exit 3.
    # The evidence still landed -- that half is never discarded.
    assert rc == 3, payload
    assert payload["verdict"] == "deferred"
    assert payload["evidence"] == 1

    report = validate_manifest(load_manifest(root, "demo"))
    assert report.alignment["requirements"]["req.demo"]["status"] == "satisfied"


def test_whole_file_plan_is_attributed_from_junit_classname(tmp_path: Path) -> None:
    root = _seed_root(tmp_path)
    pact_operations.require_test(
        root,
        "demo",
        "req.demo",
        "tests/pact/test_evidence_ingest.py",
    )
    report = tmp_path / "whole-file.xml"
    report.write_text(
        '<testsuite><testcase classname="tests.pact.test_evidence_ingest" '
        'name="test_whole_file_plan_is_attributed_from_junit_classname"/>'
        "</testsuite>",
        encoding="utf-8",
    )

    pact_operations.ingest_pytest_report(root, "demo", report)

    evidence = load_manifest(root, "demo").evidence
    assert len(evidence) == 1
    assert evidence[0].status == "pass"
    assert "req.demo" in evidence[0].refs


def test_manual_graph_evidence_attachment(tmp_path: Path, capsys) -> None:
    root = _seed_root(tmp_path)

    rc = main(
        [
            "pact",
            "--root",
            str(root),
            "evidence",
            "attach",
            "demo",
            "req.demo",
            "--kind",
            "pytest",
            "--ref",
            "manual",
            "--status",
            "pass",
            "--generation-id",
            "42",
            "--data-json",
            '{"nodes_delta": 1, "edges_delta": 2, "doctor": "clean", "queue_depth": 0, "dlq": 0}',
            "--json",
        ]
    )
    payload = _json_out(capsys)

    # attach with no reachable ledger queues the closure: deferred, exit 3
    assert rc == 3, payload
    assert payload["verdict"] == "deferred"
    assert payload["evidence"] == 1
    data = yaml.safe_load(
        (root / "pact" / "changes" / "demo" / "pact.yaml").read_text()
    )
    evidence = data["evidence"][0]
    assert evidence["generation_id"] == 42
    assert evidence["data"]["nodes_delta"] == 1
    assert evidence["data"]["queue_depth"] == 0


def test_formal_report_attaches_property_evidence(
    tmp_path: Path,
    capsys,
) -> None:
    root = _seed_root(tmp_path)
    (root / "formal" / "demo").mkdir(parents=True)
    (root / "formal" / "demo" / "Demo.tla").write_text(
        "---- MODULE Demo ----\n====\n",
        encoding="utf-8",
    )
    (root / "formal" / "demo" / "Demo.cfg").write_text(
        "INVARIANT DemoInvariant\n",
        encoding="utf-8",
    )
    (root / "tests" / "pact").mkdir(parents=True)
    (root / "tests" / "pact" / "test_evidence_ingest.py").write_text(
        "def test_formal_report_attaches_property_evidence():\n    pass\n",
        encoding="utf-8",
    )
    assert (
        main(
            [
                "pact",
                "--root",
                str(root),
                "evidence",
                "bind-tla",
                "demo",
                "--requirement",
                "req.demo",
                "--model",
                "formal/demo/Demo.tla",
                "--property-id",
                "DemoInvariant",
                "--config",
                "formal/demo/Demo.cfg",
                "--checker-command",
                ("tlc2.TLC -config formal/demo/Demo.cfg formal/demo/Demo.tla"),
                "--refinement-test",
                "tests/pact/test_evidence_ingest.py::test_formal_report_attaches_property_evidence",
                "--json",
            ]
        )
        == 0
    )
    artifact = tmp_path / "tlc.stdout.log"
    artifact.write_text(
        "TLC finished without invariant violations.\n", encoding="utf-8"
    )
    report = tmp_path / "formal-report.yaml"
    report.write_text(
        "results:\n"
        "  - property_id: DemoInvariant\n"
        "    status: pass\n"
        "    fresh: true\n"
        "    command: tlc2.TLC formal/demo/Demo.tla\n"
        "    checker_command: tlc2.TLC -config formal/demo/Demo.cfg formal/demo/Demo.tla\n"
        f"    artifact: {artifact}\n"
        "    refinement_tests:\n"
        "      - tests/pact/test_evidence_ingest.py::test_formal_report_attaches_property_evidence\n"
        f"    module_digest: {file_digest(root / 'formal' / 'demo' / 'Demo.tla')}\n"
        f"    config_digest: {file_digest(root / 'formal' / 'demo' / 'Demo.cfg')}\n",
        encoding="utf-8",
    )

    rc = main(
        [
            "pact",
            "--root",
            str(root),
            "evidence",
            "ingest",
            "demo",
            "--formal-report",
            str(report),
            "--json",
        ]
    )
    payload = _json_out(capsys)

    # the formal row landed; only the ledger write is queued -> deferred
    assert rc == 3, payload
    assert payload["verdict"] == "deferred"
    assert payload["evidence"] == 1
    evidence = load_manifest(root, "demo").evidence[0]
    assert evidence.data["module_digest"].startswith("sha256:")
    assert evidence.data["config_digest"].startswith("sha256:")


# Per `polish-pact-agent-ux` req.polish.ingest-rollup: ingest writes one
# evidence row per report per requirement, not one per test case.

_TWO_REQ_JUNIT = """
<testsuite>
  <testcase classname="tests.t" name="test_a1">
    <properties><property name="pact_requirement" value="req.demo"/></properties>
  </testcase>
  <testcase classname="tests.t" name="test_a2">
    <properties><property name="pact_requirement" value="req.demo"/></properties>
  </testcase>
  <testcase classname="tests.t" name="test_a3">
    <properties><property name="pact_requirement" value="req.demo"/></properties>
    <skipped/>
  </testcase>
  <testcase classname="tests.t" name="test_b1">
    <properties><property name="pact_requirement" value="req.other"/></properties>
  </testcase>
  <testcase classname="tests.t" name="test_b2">
    <properties><property name="pact_requirement" value="req.other"/></properties>
    <failure>boom</failure>
  </testcase>
</testsuite>
"""


def _seed_two_req_root(tmp_path: Path) -> Path:
    root = _seed_root(tmp_path)
    pact_operations.add_requirement(
        root,
        "demo",
        req_id="req.other",
        title="Other",
        statement="Other must validate.",
        evidence=["pytest"],
    )
    return root


def test_ingest_rolls_up_one_row_per_requirement(tmp_path: Path, capsys) -> None:
    root = _seed_two_req_root(tmp_path)
    report = tmp_path / "junit.xml"
    report.write_text(_TWO_REQ_JUNIT, encoding="utf-8")
    # rollup rows are written locally; the ledger write is queued -> deferred
    rc = main(
        [
            "pact",
            "--root",
            str(root),
            "evidence",
            "ingest",
            "demo",
            "--pytest-report",
            str(report),
            "--json",
        ]
    )
    payload = _json_out(capsys)
    assert rc == 3, payload
    assert payload["verdict"] == "deferred"
    rows = [ev for ev in load_manifest(root, "demo").evidence if ev.kind == "pytest"]
    assert len(rows) == 2, "5 cases across 2 requirements -> exactly 2 rows"
    by_ref = {ev.refs[0]: ev for ev in rows}
    demo = by_ref["req.demo"]
    assert demo.status == "pass"
    assert demo.data["counts"] == {"pass": 2, "fail": 0, "skip": 1}
    # artifact is the canonical (repo-relative) path, not the argv spelling
    assert demo.artifact == "junit.xml"
    assert demo.command
    other = by_ref["req.other"]
    # Fail-dominant: a report with a failed case for the ref must not
    # prove the requirement.
    assert other.status == "fail"
    assert other.data["counts"] == {"pass": 1, "fail": 1, "skip": 0}
    assert other.data["failed_test_ids"] == ["tests.t::test_b2"]
    # req.demo (all-pass) derives satisfied; req.other derives unsatisfied
    # because the rollup is fail-dominant. NOTE: the per-case shape would
    # have satisfied req.other here (test_b1's pass row counted despite
    # test_b2 failing) — the flip is the deliberate strictening stated in
    # req.polish.ingest-rollup.
    validation = validate_manifest(load_manifest(root, "demo"), repo_root=root)
    reqs = validation.alignment["requirements"]
    assert reqs["req.demo"]["satisfaction_status"] == "satisfied"
    assert reqs["req.other"]["satisfaction_status"] == "unsatisfied"


def test_reingest_refreshes_instead_of_stacking(tmp_path: Path, capsys) -> None:
    root = _seed_two_req_root(tmp_path)
    report = tmp_path / "junit.xml"
    report.write_text(_TWO_REQ_JUNIT, encoding="utf-8")
    for _ in range(2):
        # both passes defer on the unreachable ledger; the refresh itself
        # is local, so the row count is still the thing under test
        rc = main(
            [
                "pact",
                "--root",
                str(root),
                "evidence",
                "ingest",
                "demo",
                "--pytest-report",
                str(report),
                "--json",
            ]
        )
        payload = _json_out(capsys)
        assert rc == 3, payload
        assert payload["verdict"] == "deferred"
    rows = [ev for ev in load_manifest(root, "demo").evidence if ev.kind == "pytest"]
    assert len(rows) == 2, "re-ingesting the same report must not stack rows"


def test_manual_attach_survives_reingest(tmp_path: Path, capsys) -> None:
    # A manually attached pytest row citing the same report must not be
    # deleted by the re-ingest refresh (only ingest-written rows are).
    root = _seed_two_req_root(tmp_path)
    report = tmp_path / "junit.xml"
    report.write_text(_TWO_REQ_JUNIT, encoding="utf-8")
    pact_operations.attach_evidence(
        root,
        "demo",
        "req.demo",
        kind="pytest",
        ref=str(report),
        status="pass",
        summary="hand-recorded run of the same report",
    )
    manual_ids = {
        ev.id
        for ev in load_manifest(root, "demo").evidence
        if ev.summary == "hand-recorded run of the same report"
    }
    assert manual_ids
    # deferred on the unreachable ledger; the refresh still ran locally
    rc = main(
        [
            "pact",
            "--root",
            str(root),
            "evidence",
            "ingest",
            "demo",
            "--pytest-report",
            str(report),
            "--json",
        ]
    )
    payload = _json_out(capsys)
    assert rc == 3, payload
    assert payload["verdict"] == "deferred"
    surviving = {ev.id for ev in load_manifest(root, "demo").evidence}
    assert manual_ids <= surviving


def test_two_path_spellings_are_one_report(tmp_path: Path, capsys, monkeypatch) -> None:
    # Report identity is the canonical path: ingesting via a relative and
    # an absolute spelling of the same file must not double the rows.
    root = _seed_two_req_root(tmp_path)
    report = root / "junit.xml"
    report.write_text(_TWO_REQ_JUNIT, encoding="utf-8")
    monkeypatch.chdir(root)
    assert (
        main(
            [
                "pact",
                "--root",
                str(root),
                "evidence",
                "ingest",
                "demo",
                "--pytest-report",
                "junit.xml",
                "--json",
            ]
        )
        == 3
    )
    assert (
        main(
            [
                "pact",
                "--root",
                str(root),
                "evidence",
                "ingest",
                "demo",
                "--pytest-report",
                str(report),
                "--json",
            ]
        )
        == 3
    )
    rows = [ev for ev in load_manifest(root, "demo").evidence if ev.kind == "pytest"]
    assert len(rows) == 2, "same file via two spellings must stay 2 rollup rows"


def test_legacy_per_case_rows_still_validate_and_satisfy(tmp_path: Path) -> None:
    # The corpus carries per-case rows from the old ingest shape; they must
    # remain valid and keep satisfying their policy untouched.
    root = _seed_root(tmp_path)
    path = root / "pact" / "changes" / "demo" / "pact.yaml"
    doc = yaml.safe_load(path.read_text(encoding="utf-8"))
    doc["evidence"] = [
        {
            "id": "ev.pytest.req-demo.tests-t-test-legacy",
            "kind": "pytest",
            "status": "pass",
            "refs": ["req.demo"],
            "artifact": "old-junit.xml",
            "command": "pytest",
            "summary": "tests.t::test_legacy",
            "data": {"junit_test_id": "tests.t::test_legacy"},
        }
    ]
    path.write_text(yaml.safe_dump(doc, sort_keys=False), encoding="utf-8")
    validation = validate_manifest(load_manifest(root, "demo"), repo_root=root)
    assert validation.ok
    reqs = validation.alignment["requirements"]
    assert reqs["req.demo"]["satisfaction_status"] == "satisfied"
