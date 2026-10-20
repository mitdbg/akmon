from __future__ import annotations

import pytest
from pydantic import ValidationError

from okg.substrate.pact.markdown_view import (
    PactMarkdownViewFrontmatter,
    check_markdown_view,
    dump_frontmatter,
    parse_frontmatter,
    render_markdown_view,
    write_markdown_view,
)
from okg.substrate.pact.models import Change, Evidence, PactManifest
from okg.substrate.pact.store import save_manifest

from tests.private_policy import private_policy_path


def test_view_frontmatter_model() -> None:
    frontmatter = PactMarkdownViewFrontmatter(
        change="demo",
        source_digest="sha256:" + "a" * 64,
        requirement_count=2,
        task_count=3,
        test_plan_count=4,
        blocker_count=1,
    )

    text = dump_frontmatter(frontmatter) + "# Demo\n"
    parsed = parse_frontmatter(text)

    assert parsed.change == "demo"
    assert parsed.source == "pact.yaml"
    assert parsed.canonical is False
    assert parsed.generator == "okg-pact-markdown-v0"
    assert parsed.requirement_count == 2
    assert parsed.blocker_count == 1


def test_view_frontmatter_requires_digest() -> None:
    with pytest.raises(ValidationError):
        PactMarkdownViewFrontmatter(
            change="demo",
            source_digest="not-a-digest",
            requirement_count=0,
            task_count=0,
            test_plan_count=0,
            blocker_count=0,
        )


def test_render_writes_required_sections() -> None:
    manifest = _demo_manifest()

    text = render_markdown_view(manifest)

    assert "# Demo Review" in text
    assert "canonical intended state is `pact.yaml`" in text
    assert "## Operator Brief" in text
    assert "## Decision" in text
    assert "## Why" in text
    assert "## Non-Goals" in text
    assert "## Requirements" in text
    assert "## Acceptance And Evidence" in text
    assert "## Task Graph" in text
    assert "## Blockers" in text
    assert "## Waivers" in text
    assert "## Loop Records" in text
    assert "## Risks And Review Questions" in text
    assert "## Validation State" in text
    assert "## Next Work" in text
    assert "`req.demo`" in text
    assert "`task.demo`" in text


def test_render_operator_brief_leads_with_human_choices() -> None:
    manifest = _demo_manifest()

    text = render_markdown_view(manifest)

    assert text.index("## Operator Brief") < text.index("## Decision")
    assert "Decision needed:" in text
    assert "Current state:" in text
    assert "Next action:" in text
    assert "Human choices:" in text
    # The brief offers the verdicts that are LEGAL in the derived state, not
    # the whole vocabulary. It used to print all five everywhere, so a change
    # in `in_progress` was told to run `revise`, which `decide` then refused
    # (issue #1476). The legal set is read from the same transition table.
    from okg.substrate.pact.fsm import HUMAN_VERDICTS, legal_verdicts
    from okg.substrate.pact.markdown_view import operator_brief
    from okg.substrate.pact.validation import derive_alignment

    brief = operator_brief(manifest, derive_alignment(manifest))
    legal = set(legal_verdicts(brief["human_choices_state"]))
    assert brief["human_choices"]
    for verdict in HUMAN_VERDICTS:
        if verdict in legal:
            assert f"`{verdict}`" in text, verdict
        else:
            assert f"`{verdict}`" not in text.split("Proof state:")[0], verdict
    assert "Proof state:" in text


def test_render_writes_loop_records() -> None:
    manifest = _demo_manifest()
    manifest.evidence.append(
        Evidence(
            id="ev.loop-record.demo",
            kind="loop_record",
            status="pass",
            refs=["demo"],
            summary="PACT improvement loop: visible records",
            data={
                "observation": "Loop record was hidden in YAML.",
                "failure_mode": "intent_gap",
                "ranked_improvement": "Render loop records in pact.md.",
                "decision": "Put the audit trail in the generated review view.",
                "verification": "pytest tests/pact/test_markdown_view.py -q",
                "smallest_affected_surface": "src/okg/substrate/pact/markdown_view.py",
                "next_observation": "Check whether humans can see the trace.",
                "friction_ledger": [
                    {
                        "observation": "The generated review view hid loop details.",
                        "failure_mode": "intent_gap",
                        "severity": "p1",
                        "frequency": "likely common",
                        "candidate_improvement": "Render loop record details in pact.md.",
                        "verification": "pytest",
                    }
                ],
            },
        )
    )

    text = render_markdown_view(manifest)

    assert "## Loop Records" in text
    assert "`ev.loop-record.demo`" in text
    assert "Loop record was hidden in YAML." in text
    assert "intent_gap" in text
    assert "Render loop records in pact.md." in text
    assert "### Loop Record Details" in text
    assert "#### `ev.loop-record.demo`" in text
    assert "Put the audit trail in the generated review view." in text
    assert "pytest tests/pact/test_markdown_view.py -q" in text
    assert "src/okg/substrate/pact/markdown_view.py" in text
    assert "Friction ledger:" in text
    assert "Render loop record details in pact.md." in text
    assert "Check whether humans can see the trace." in text


def test_render_links_design_records(tmp_path) -> None:
    manifest = _demo_manifest()
    save_manifest(tmp_path, manifest)
    design_dir = tmp_path / "pact" / "changes" / "demo" / "designs"
    design_dir.mkdir(parents=True)
    (design_dir / "review-surface.md").write_text(
        "\n".join([
            "# Review Surface Design",
            "Design-ID: design.review-surface",
            "Status: accepted",
            "Refs: demo, req.demo",
            "",
            "## Context",
            "Humans need the proof shape in the generated view.",
            "",
            "## Decision",
            "Render design records alongside requirements.",
            "",
        ]),
        encoding="utf-8",
    )

    write_markdown_view(tmp_path, "demo")
    text = (tmp_path / "pact" / "changes" / "demo" / "pact.md").read_text(
        encoding="utf-8"
    )
    report = check_markdown_view(tmp_path, "demo")

    assert report["ok"] is True
    assert report["frontmatter"]["design_count"] == 1
    assert "## Design Records" in text
    assert "[Review Surface Design](designs/review-surface.md)" in text
    assert "`design.review-surface`" in text
    assert "accepted" in text
    assert "`req.demo`" in text
    assert "Render design records alongside requirements." in text


def test_render_shows_requirement_details() -> None:
    manifest = _demo_manifest()

    text = render_markdown_view(manifest)

    assert "## Requirement Details" in text
    assert "### `req.demo` Readable review" in text
    assert "PACT MUST render Markdown." in text
    assert "Scenario: `scn.demo` renders" in text
    assert "WHEN render runs" in text
    assert "THEN Markdown exists" in text
    assert "Acceptance: `acc.demo`" in text
    assert "reviewable" in text
    assert "Planned Proof" in text
    assert "tests/pact/test_markdown_view.py::test_render_writes_required_sections" in text


def test_pact_md_declares_yaml_canonical() -> None:
    text = render_markdown_view(_demo_manifest())
    frontmatter = parse_frontmatter(text)

    assert frontmatter.canonical is False
    assert frontmatter.source == "pact.yaml"
    assert "[pact.yaml](pact.yaml)" in text


def test_stale_view_fails_check(tmp_path) -> None:
    manifest = _demo_manifest()
    save_manifest(tmp_path, manifest)
    write_markdown_view(tmp_path, manifest.change.id)

    manifest.requirements[0].title = "Changed Review"
    save_manifest(tmp_path, manifest)
    report = check_markdown_view(tmp_path, manifest.change.id)

    assert report["ok"] is False
    assert report["status"] == "stale"
    assert any(issue["code"] == "stale_view" for issue in report["issues"])


def test_fresh_view_passes_check(tmp_path) -> None:
    manifest = _demo_manifest()
    save_manifest(tmp_path, manifest)
    write_markdown_view(tmp_path, manifest.change.id)

    report = check_markdown_view(tmp_path, manifest.change.id)

    assert report["ok"] is True
    assert report["status"] == "fresh"
    assert report["frontmatter"]["change"] == "demo"


def test_agents_instructions_describe_review_surface() -> None:
    """The docs name a path to a reviewer surface, and pact.yaml is canonical.

    The surface changed with v5: `--format=packet --reviewer-kind=...` is
    gone, and the reviewer reads `okg pact digest` and pastes the
    `Pact-Approved` line into their pull-request review. The invariant --
    that a reviewer can find their way in from this document -- is unchanged.
    """
    text = private_policy_path("pact/AGENTS.md").read_text(encoding="utf-8")
    flat = " ".join(text.split())

    # A reviewer surface is named.
    assert "okg pact view" in text and "okg pact digest" in text
    assert "Pact-Approved" in flat
    # pact.yaml is canonical.
    assert "`pact.yaml` is canonical" in text


def _demo_manifest() -> PactManifest:
    return PactManifest(
        change=Change(
            id="demo",
            title="Demo Review",
            rationale="Approve the demo change.",
            root_cause="Humans need a readable view.",
        ),
        requirements=[
            {
                "id": "req.demo",
                "title": "Readable review",
                "statement": "PACT MUST render Markdown.",
                "evidence_policy": {"required": ["planned_test"]},
                "scenarios": [
                    {
                        "id": "scn.demo",
                        "title": "renders",
                        "when": ["render runs"],
                        "then": ["Markdown exists"],
                    }
                ],
                "acceptance": [
                    {
                        "id": "acc.demo",
                        "description": "reviewable",
                        "evidence": ["planned_test"],
                    }
                ],
            }
        ],
        tasks=[
            {
                "id": "task.demo",
                "title": "Render demo",
                "verification": "pytest tests/pact/test_markdown_view.py -q",
                "requirements": ["req.demo"],
            }
        ],
        test_plan=[
            {
                "id": "test.demo",
                "requirement": "req.demo",
                "test_id": "tests/pact/test_markdown_view.py::test_render_writes_required_sections",
            }
        ],
        prose={"non_goals": "Do not make Markdown canonical."},
    )
