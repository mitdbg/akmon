"""The CLI must never emit an evidence row its own validator refuses.

pact-vacuous-gate-close req.vacuous.presence. Once `unknown_evidence_kind` is
an ERROR, any verb that writes a kind outside the registry breaks the manifest
it was asked to prove — which is exactly what `evidence approve` did, writing
`intent_review` rows no registry knew about. This drives every evidence-writing
verb against a BARE root (no project-level registry block, so only the code
defaults apply) and re-validates afterwards.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest
import yaml

from okg.substrate.cli.main import main
from okg.substrate.pact import operations as pact_operations
from okg.substrate.pact.evidence_kinds import DEFAULT_REGISTRY
from okg.substrate.pact.store import load_manifest
from okg.substrate.pact.validation import validate_manifest


def _json_out(capsys):
    lines = [line for line in capsys.readouterr().out.splitlines() if line.strip()]
    return json.loads(lines[-1])


def _bare_root(tmp_path: Path) -> Path:
    """A root with NO `evidence_kinds:` block: code defaults only.

    The repo's own `pact/project.yaml` extends the registry, so a fixture that
    copied it would hide a verb writing a kind the code defaults lack — the
    defect would survive everywhere except this repo.
    """
    root = tmp_path
    (root / "pact" / "schemas").mkdir(parents=True)
    (root / "pact" / "AGENTS.md").write_text("# PACT\n", encoding="utf-8")
    (root / "pact" / "project.yaml").write_text(
        "pact_version: 4\n"
        "project: roundtrip\n"
        "pact_defaults:\n"
        "  artifact_format_version: 4\n"
        "  target_pact_version: 4\n",
        encoding="utf-8",
    )
    (root / "pact" / "schemas" / "pact.schema.yaml").write_text(
        "pact_schema_version: 0\n", encoding="utf-8"
    )
    assert main([
        "pact", "--root", str(root), "new-change", "demo",
        "--template", "protocol", "--json",
    ]) == 0
    pact_operations.add_requirement(
        root, "demo", req_id="req.demo", title="Demo",
        statement="Demo must validate.", evidence=["pytest"],
    )
    pact_operations.add_task(
        root, "demo", task_id="task.demo", title="Prove demo",
        verification="uv run pytest tests/pact/test_evidence_write_roundtrip.py -q",
        requirements=["req.demo"],
    )
    return root


def _errors(root: Path, change_id: str = "demo") -> list[dict[str, str]]:
    report = validate_manifest(load_manifest(root, change_id), repo_root=root)
    return [issue.to_dict() for issue in report.issues if issue.severity == "error"]


def test_attach_ingest_and_approve_leave_zero_refusals(
    tmp_path: Path, capsys
) -> None:
    root = _bare_root(tmp_path)
    assert _errors(root) == []

    # 1. attach
    assert main([
        "pact", "--root", str(root), "evidence", "attach", "demo", "req.demo",
        "--kind", "executed_test",
        "--ref", "tests/pact/test_evidence_write_roundtrip.py",
        "--status", "pass",
        "--command", "uv run pytest tests/pact/test_evidence_write_roundtrip.py -q",
        "--summary", "round-trip probe", "--json",
    ]) in (0, 3)
    _json_out(capsys)
    assert _errors(root) == []

    # 2. ingest
    report = tmp_path / "junit.xml"
    report.write_text(
        "<testsuite><testcase classname='tests.pact.rt' name='test_rt'>"
        "<properties><property name='pact_requirement' value='req.demo'/>"
        "</properties></testcase></testsuite>",
        encoding="utf-8",
    )
    assert main([
        "pact", "--root", str(root), "evidence", "ingest", "demo",
        "--pytest-report", str(report), "--json",
    ]) in (0, 3)
    _json_out(capsys)
    assert _errors(root) == []

    # 3. the review-approve path, which writes `intent_review`
    assert main([
        "pact", "--root", str(root), "view", "demo", "--format=markdown", "--json",
    ]) == 0
    _json_out(capsys)
    assert main([
        "pact", "--root", str(root), "evidence", "approve", "demo",
        "--reviewer", "reviewer@example.com", "--notes", "intent accepted",
        "--json",
    ]) == 0
    _json_out(capsys)

    document = yaml.safe_load(
        (root / "pact" / "changes" / "demo" / "pact.yaml").read_text(encoding="utf-8")
    )
    kinds = {row["kind"] for row in document["evidence"]}
    assert "intent_review" in kinds
    assert _errors(root) == []
    # And every kind that landed resolves — not merely "no error was raised".
    for kind in kinds:
        assert DEFAULT_REGISTRY.is_known(kind), kind


@pytest.mark.parametrize(
    "kind",
    # `loop_record` carries its own required `data` fields
    # (LOOP_RECORD_REQUIRED_FIELDS), so a bare attach of that kind is
    # legitimately incomplete — a content contract, not a registry one.
    sorted(DEFAULT_REGISTRY.canonical - {"loop_record"}),
)
def test_every_canonical_kind_can_be_attached_and_still_validates(
    tmp_path: Path, kind: str
) -> None:
    """No canonical kind is write-only.

    A kind in the registry that the validator refuses on arrival would be the
    same defect in the other direction.
    """
    root = _bare_root(tmp_path)
    pact_operations.attach_evidence(
        root, "demo", "req.demo", kind=kind, ref="artifact", status="pass",
        summary="round-trip probe",
    )
    assert _errors(root) == []


def test_a_kind_outside_the_registry_is_refused_at_the_write(
    tmp_path: Path,
) -> None:
    """The other half of the round trip: the write is where it stops.

    Recording the row and reporting a warning is what let unusable proof
    accumulate; the verb must refuse instead.
    """
    from okg.substrate.errors import SubstrateError

    root = _bare_root(tmp_path)
    with pytest.raises(SubstrateError) as excinfo:
        pact_operations.attach_evidence(
            root, "demo", "req.demo", kind="kind_the_registry_does_not_know",
            ref="artifact", status="pass", summary="rogue",
        )
    codes = {
        row["code"] for row in (excinfo.value.data or {}).get("errors", [])
    }
    assert "unknown_evidence_kind" in codes
    # Nothing was persisted, so the manifest is still clean.
    assert _errors(root) == []
