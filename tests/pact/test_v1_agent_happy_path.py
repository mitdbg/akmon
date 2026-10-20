from __future__ import annotations

import json
from pathlib import Path

from okg.substrate.cli.main import main
from okg.substrate.pact import operations as pact_operations
from okg.substrate.pact.markdown_view import check_markdown_view
from okg.substrate.pact.store import load_manifest
from okg.substrate.pact.validation import validate_manifest
from tests.pact.test_cli import _seed_root


def _json_out(capsys):
    lines = [line for line in capsys.readouterr().out.splitlines() if line.strip()]
    return json.loads(lines[-1])


def test_agent_can_complete_routine_change_without_yaml_edits(
    tmp_path: Path,
    capsys,
) -> None:
    root = _seed_root(tmp_path)

    assert main([
        "pact",
        "--root",
        str(root),
        "new-change",
        "demo",
        "--template",
        "protocol",
        "--title",
        "Demo",
        "--json",
    ]) == 0
    assert main([
        "pact", "--root", str(root), "migrate-version", "demo",
        "--target", "1", "--json",
    ]) == 0
    pact_operations.add_requirement(
        root, "demo", req_id="req.demo", title="Demo requirement",
        statement="Demo must validate.", evidence=["planned_test"],
    )
    pact_operations.add_task(
        root, "demo", task_id="task.demo", title="Do demo",
        verification="uv run --extra dev pytest tests/pact/test_v1_agent_happy_path.py -q",
        requirements=["req.demo"],
    )
    pact_operations.require_test(
        root, "demo", "req.demo",
        "tests/pact/test_v1_agent_happy_path.py::test_agent_can_complete_routine_change_without_yaml_edits",
        command="uv run --extra dev pytest tests/pact/test_v1_agent_happy_path.py -q",
    )
    assert main([
        "pact", "--root", str(root), "view", "demo",
        "--format=markdown", "--json",
    ]) == 0
    _json_out(capsys)
    assert main([
        "pact", "--root", str(root), "evidence", "approve", "demo",
        "--reviewer", "reviewer@example.com",
        "--notes", "intent accepted", "--json",
    ]) == 0
    assert main([
        "pact", "--root", str(root), "gate", "pre-implementation", "demo",
        "--json",
    ]) == 0

    report = tmp_path / "junit.xml"
    report.write_text(
        """
        <testsuite>
          <testcase classname="tests.pact.test_v1_agent_happy_path" name="test_agent_can_complete_routine_change_without_yaml_edits"/>
        </testsuite>
        """,
        encoding="utf-8",
    )
    # the junit rollup lands locally; the ledger write is queued -> deferred
    rc = main([
        "pact",
        "--root",
        str(root),
        "evidence",
        "ingest",
        "demo",
        "--pytest-report",
        str(report),
        "--json",
    ])
    assert rc == 3
    assert _json_out(capsys)["verdict"] == "deferred"
    assert main([
        "pact", "--root", str(root), "gate", "task-done", "demo",
        "task.demo", "--json",
    ]) == 0

    report = validate_manifest(
        load_manifest(root, "demo"), lifecycle_gate="verification",
    )
    assert report.alignment["lifecycle"]["verification"] is True

    checked = check_markdown_view(root, "demo")
    assert checked["status"] == "fresh"
