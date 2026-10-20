"""Per `reduce-agent-friction` req.friction.actionable-cli-errors.

The five most-used verb families emit a one-line actionable correction
before any argparse usage text, and `generation describe` accepts the
canonical checkpoint ref string instead of demanding an int.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from okg.substrate.cli.main import main
from okg.substrate.cli.parser_groups import _generation_id_token

REPO_ROOT = Path(__file__).resolve().parents[2]


def _err_lines(capsys) -> list[str]:
    return [ln for ln in capsys.readouterr().err.splitlines() if ln.strip()]


def _assert_actionable_before_usage(lines: list[str], *needles: str) -> None:
    assert lines, "expected error output"
    first = lines[0]
    assert first.startswith("error:"), f"first line is not the correction: {first!r}"
    for needle in needles:
        assert needle in first, f"{needle!r} missing from: {first!r}"
    assert any(ln.startswith("usage:") for ln in lines[1:]), (
        "argparse usage should still follow the correction"
    )


@pytest.mark.parametrize(
    ("argv", "needles"),
    [
        (["generation", "descripe", "g1"], ("did you mean `describe`", "okg generation describe")),
        (["deployment", "raedy", "x"], ("did you mean `ready`", "okg deployment ready")),
        (["generaton", "list"], ("did you mean `generation`",)),
    ],
)
def test_family_typos_get_one_line_corrections(argv, needles, capsys) -> None:
    with pytest.raises(SystemExit) as exc:
        main(argv)
    assert exc.value.code == 2
    _assert_actionable_before_usage(_err_lines(capsys), *needles)


def test_pact_family_correction_suppresses_usage_by_design(capsys) -> None:
    # The pact family's richer did-you-mean (polish-pact-agent-ux) prints
    # the correction WITHOUT a usage dump — the usage text was what agents
    # retried against verbatim.
    with pytest.raises(SystemExit) as exc:
        main(["pact", "nex", "demo"])
    assert exc.value.code == 2
    lines = _err_lines(capsys)
    assert "did you mean" in lines[0]
    assert not any(ln.startswith("usage:") for ln in lines)


@pytest.mark.parametrize(
    ("argv", "needle"),
    [
        # leaf subparsers own their error(); the tree pass must cover them
        (["generation", "describe"], "okg generation describe <generation-ref>"),
        (["generation", "list", "--limit", "x"], "okg generation describe"),
        (["deployment", "ready"], "okg deployment ready <name>"),
        (["pact"], "okg pact view <id> --format=next"),
        (["pact", "view", "demo", "--format", "bogus"], "okg pact view <id>"),
    ],
)
def test_leaf_failure_modes_get_the_family_example(argv, needle, capsys) -> None:
    with pytest.raises(SystemExit) as exc:
        main(argv)
    assert exc.value.code == 2
    _assert_actionable_before_usage(_err_lines(capsys), needle)


def test_ingest_missing_required_names_the_example(capsys) -> None:
    with pytest.raises(SystemExit) as exc:
        main(["ingest"])
    assert exc.value.code == 2
    _assert_actionable_before_usage(
        _err_lines(capsys), "okg ingest --deployment <name>"
    )


def test_doctor_unknown_flag_still_gets_a_correction_line(capsys) -> None:
    with pytest.raises(SystemExit) as exc:
        main(["doctor", "--deploymnet", "x"])
    assert exc.value.code == 2
    lines = _err_lines(capsys)
    assert lines[0].startswith("error:")
    assert "unrecognized arguments" in lines[0]


def test_generation_describe_accepts_canonical_ref_string() -> None:
    # The mined class: `generation describe` demanded an int. The parser
    # token must pass the canonical Chronos checkpoint id through.
    ref = "okg:okg-workspace:branch:default:gen:20260702T000000:abc-123"
    assert _generation_id_token(ref) == ref
    assert _generation_id_token("123") == 123


def test_agent_workflow_skill_carries_the_surface_discipline() -> None:
    skill = (
        REPO_ROOT / "src" / "okg" / "substrate" / "pact" / "skills"
        / "agent-workflow" / "SKILL.md"
    ).read_text(encoding="utf-8")
    # opaque-bash guidance (the 1,003-hit class, pre-empted at instruction grain)
    assert "Glance-reviewable Bash" in skill
    assert "heredocs" in skill
    assert ".scratch/" in skill
    # dogfood-through-surface rule + friction-log format
    assert "Dogfood through the surface" in skill
    assert "forcing failure recorded" in skill
    assert "prevention:" in skill
