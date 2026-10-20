from __future__ import annotations

import re
from pathlib import Path

import yaml


REPO_ROOT = Path(__file__).resolve().parents[2]
FIXTURE = REPO_ROOT / "tests/docs/fixtures/human_report_examples.yaml"
RUNBOOK = REPO_ROOT / "docs/runbooks/agents/reporting.md"

REPORT_KINDS = {
    "status",
    "blocker",
    "decision",
    "handoff",
    "review",
    "closeout",
    "child_rollup",
}
OBSERVED_FAILURES = {
    "jargon_first",
    "passive_voice",
    "padded",
    "undefined_term",
    "machine_state_first",
    "work_history",
    "unsupported_claim",
}
INTERNAL_OPENERS = (
    "pact ",
    "report_kind",
    "subagent ",
    "the agent-os",
    "the substrate",
    "graph readiness",
)


def _fixture() -> dict:
    return yaml.safe_load(FIXTURE.read_text(encoding="utf-8"))


def test_redacted_fixture_covers_common_human_reports() -> None:
    payload = _fixture()
    metadata = payload["metadata"]
    assert "Redacted patterns" in metadata["provenance"]
    assert "No transcript text" in metadata["privacy"]

    examples = payload["examples"]
    for kind in REPORT_KINDS:
        qualities = {row["quality"] for row in examples if row["kind"] == kind}
        assert qualities == {"good", "bad"}, f"{kind} needs a good and bad example"

    serialized = FIXTURE.read_text(encoding="utf-8")
    assert not re.search(r"/(Users|home)/[^\s]+", serialized)
    assert not re.search(r"(?:postgres|https?)://[^<\s]+", serialized)
    assert not re.search(r"\b[0-9a-f]{32,}\b", serialized)

    for row in examples:
        assert isinstance(row["observed_facts"], list)
        assert isinstance(row["hypotheses"], list)
        assert row["measurement"].startswith(("Measured:", "Not measured:"))


def test_good_examples_are_result_first_plain_and_short() -> None:
    good = [row for row in _fixture()["examples"] if row["quality"] == "good"]
    assert good
    for row in good:
        text = row["text"].strip()
        first = text.splitlines()[0].lower()
        assert not first.startswith("i first")
        assert not first.startswith(INTERNAL_OPENERS)
        assert len(text.split()) <= 120, f"{row['id']} exceeds one-screen budget"
        assert row["problems"] == []


def test_bad_examples_cover_observed_failure_modes() -> None:
    bad = [row for row in _fixture()["examples"] if row["quality"] == "bad"]
    covered = {problem for row in bad for problem in row["problems"]}
    assert OBSERVED_FAILURES <= covered
    assert all(row["problems"] for row in bad)


def test_runbook_makes_plain_prose_the_default_surface() -> None:
    text = RUNBOOK.read_text(encoding="utf-8")
    assert "Lead with the result" in text
    assert "Before → after" in text
    assert "Agent-to-agent" in text
    assert "tests/docs/fixtures/human_report_examples.yaml" in text
    assert "raw YAML by default" not in text
    assert len(text.splitlines()) <= 220
