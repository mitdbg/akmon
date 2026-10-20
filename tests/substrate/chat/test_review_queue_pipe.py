"""One inbox over both pending kinds (req.chat-review-queue-pipe.one-inbox).

These are the POPULATED arm. They exist so the refusal arms in
`test_review_queue_pipe_empty.py` are non-vacuous: the same assembly path
that must refuse when a source is unavailable must, here, actually merge
both CLI JSON sources and render per-row decision actions.
"""

from __future__ import annotations

import ast
import json
from collections.abc import Mapping
from pathlib import Path
from typing import Any

import pytest

from okg.substrate.chat.review_queue_pipe import (
    ADMISSION_DECISIONS,
    DECISION_TYPE_ACTIONS,
    EXTERNAL_ACTION_DECISIONS,
    EXTERNAL_ACTION_INBOX_DECISIONS,
    EXTERNAL_ACTION_LIST_LIMIT,
    PACT_HUMAN_VERDICTS,
    POPULATED,
    SourceOutcome,
    UnmappedDecisionType,
    build_external_action_decide_argv,
    build_external_action_list_argv,
    classify_external_action_result,
    parse_decision_intent,
    packet_row_handle,
    render_pact_section,
    row_action,
)
from okg.substrate.pact.review import _DECISION_RANK
from tests.substrate.chat.conftest import (
    ADMIN_USER,
    DEPLOYMENT,
    FORBIDDEN_BRANCH_LITERAL,
    PENDING_ENVELOPE,
    admission_payload,
    json_result,
    make_pipe,
)


@pytest.fixture
def both_kinds(populated_queue):
    responses = {
        "pact view": json_result(populated_queue),
        "admission list": json_result(admission_payload([PENDING_ENVELOPE])),
    }
    pipe, runner = make_pipe(responses)
    text = pipe.pipe({"messages": []}, __user__=ADMIN_USER)
    return pipe, runner, text


EXTERNAL_PROPOSAL = {
    "proposal_id": "proposal-1",
    "deployment": DEPLOYMENT,
    "review_state": "edited",
    "execution_state": "unavailable",
    "proposer_principal": "automation-owner",
    "action_type": "calendar.create",
    "target_ref": "calendar:team",
    "risk_tier": "medium",
    "created_at": "2026-08-26T12:00:00+00:00",
    "effective_review_expires_at": "2026-08-27T12:00:00+00:00",
    "original_payload_hash": "sha256:original",
    "current_payload_hash": "sha256:edited",
    "summary_code": "payload_changed",
    "evidence_digests": ["sha256:evidence"],
    "decision_id": None,
    "decision_binding_hash": None,
    "etag": '"lifecycle:2;payload-version:2;payload:sha256:edited"',
}

EXTERNAL_TIMELINE = {
    "events": [
        {
            "sequence": 1,
            "lifecycle_version": 1,
            "event_type": "submitted",
            "after_review_state": "pending",
            "payload_hash": "sha256:original",
            "previous_event_hash": "sha256:zero",
            "event_hash": "sha256:event-1",
        },
        {
            "sequence": 2,
            "lifecycle_version": 2,
            "event_type": "edited",
            "before_review_state": "pending",
            "after_review_state": "edited",
            "payload_hash": "sha256:edited",
            "event_hash": "sha256:event-2",
            "outcome_reason_code": "awaiting_review",
        },
    ]
}


def external_pipe():
    pipe, runner = make_pipe(
        {
            "pact view": json_result(
                {
                    "ok": True,
                    "chat_review_queue": {
                        "rows": [],
                        "shown": 0,
                        "total": 0,
                        "warnings": [],
                    },
                }
            ),
            "admission list": json_result(admission_payload([])),
            "external-action list": json_result(
                {"proposals": [EXTERNAL_PROPOSAL]}
            ),
            "external-action show": json_result(EXTERNAL_PROPOSAL),
            "external-action timeline": json_result(EXTERNAL_TIMELINE),
        }
    )
    pipe.valves.external_action_api_url = "https://review.example.invalid"
    default_key = runner.key

    def key(argv):
        if "external-action" in argv:
            return next(
                f"external-action {command}"
                for command in ("list", "show", "timeline", "decide")
                if command in argv
            )
        return default_key(argv)

    runner.key = key  # type: ignore[method-assign]
    return pipe, runner


def test_external_action_source_renders_redacted_projection_and_timeline():
    pipe, runner = external_pipe()
    text = pipe.pipe({"messages": []}, __user__=ADMIN_USER)
    assert "### External-action review proposals" in text
    assert "**E1 `proposal-1`**" in text
    assert "edited" in text and "unavailable" in text
    assert "sha256:original" in text and "sha256:edited" in text
    assert "payload_changed" in text and "sha256:evidence" in text
    assert "inbox decisions: `approve|reject|defer`" in text
    assert "submitted" in text and "edited" in text
    assert "sha256:event-1" in text and "sha256:event-2" in text
    assert [runner.key(argv) for argv in runner.calls].count("external-action list") == 1
    assert [runner.key(argv) for argv in runner.calls].count("external-action show") == 1
    assert [runner.key(argv) for argv in runner.calls].count("external-action timeline") == 1


def test_external_action_unavailable_is_explicit_and_not_decidable():
    pipe, _runner = make_pipe(
        {
            "pact view": json_result(
                {
                    "ok": True,
                    "chat_review_queue": {
                        "rows": [],
                        "shown": 0,
                        "total": 0,
                        "warnings": [],
                    },
                }
            ),
            "admission list": json_result(admission_payload([])),
            "external-action list": json_result(
                {}, returncode=503
            ),
        }
    )
    pipe.valves.external_action_api_url = "https://review.example.invalid"
    pipe._runner.key = lambda argv: "external-action list"  # type: ignore[attr-defined]
    text = pipe.pipe({"messages": [{"role": "user", "content": "decide E1 reject"}]}, __user__=ADMIN_USER)
    assert "external-action source did not answer" in text
    assert "Decision verbs are not offered" in text
    assert "external-action review" in text
    assert "external-action decide" not in text


def test_external_action_list_rejects_saturation_and_secret_fields():
    result = json_result(
        {"proposals": [{**EXTERNAL_PROPOSAL, "payload": "secret"}]}
    )
    outcome = classify_external_action_result(result, queried_at="now")
    assert outcome.state == "unavailable"
    saturated = classify_external_action_result(
        json_result(
            {
                "proposals": [
                    {**EXTERNAL_PROPOSAL, "proposal_id": f"proposal-{index}"}
                    for index in range(EXTERNAL_ACTION_LIST_LIMIT)
                ]
            }
        ),
        queried_at="now",
    )
    assert saturated.state == "unavailable"
    assert "reached its" in saturated.reason


def test_external_action_decision_grammar_and_argv_are_bounded():
    intent = parse_decision_intent("decide E2 approve")
    assert intent is not None
    assert intent.half == "external" and intent.index == 2
    assert parse_decision_intent("decide E2 approve_with_edit") is None
    assert parse_decision_intent("decide E2 free-form") is None
    argv = build_external_action_list_argv(
        "/bin/okg", DEPLOYMENT, api_url="https://review.example.invalid"
    )
    assert "--limit" in argv and str(EXTERNAL_ACTION_LIST_LIMIT) in argv
    decision = build_external_action_decide_argv(
        "/bin/okg",
        DEPLOYMENT,
        "proposal-1",
        decision=EXTERNAL_ACTION_DECISIONS[0],
        idempotency_key="key-1",
        if_match='"etag"',
        body_file="/tmp/review.json",
    )
    assert "secret" not in " ".join(decision)
    assert set(EXTERNAL_ACTION_INBOX_DECISIONS) < set(EXTERNAL_ACTION_DECISIONS)


def test_external_approve_refuses_without_an_execution_deadline():
    pipe, runner = external_pipe()
    pipe.valves.external_action_decision_reason_code = "operator_approved"

    text = pipe.pipe(
        {"messages": [{"role": "user", "content": "decide E1 approve"}]},
        __user__=ADMIN_USER,
    )

    assert "valid RFC3339 `external_action_execute_before`" in text
    assert "no authenticated write was attempted" in text
    assert "external-action decide" not in [runner.key(argv) for argv in runner.calls]


def test_external_approve_sends_bounded_body_then_reads_back():
    pipe, runner = external_pipe()
    pipe.valves.external_action_decision_reason_code = "operator_approved"
    pipe.valves.external_action_execute_before = "2026-08-27T11:00:00Z"
    runner.responses["external-action decide"] = json_result({"ok": True})
    original_runner = pipe._runner
    captured_body: dict[str, Any] = {}
    captured_path: Path | None = None

    def capture(argv):
        nonlocal captured_path
        if runner.key(argv) == "external-action decide":
            body_index = argv.index("--body-file") + 1
            captured_path = Path(argv[body_index])
            captured_body.update(json.loads(captured_path.read_text()))
        return original_runner(argv)

    pipe._runner = capture
    text = pipe.pipe(
        {"messages": [{"role": "user", "content": "decide E1 approve"}]},
        __user__=ADMIN_USER,
    )

    assert captured_body == {
        "verdict": "approve",
        "reason_code": "operator_approved",
        "execute_before": "2026-08-27T11:00:00.000000Z",
    }
    assert captured_path is not None and not captured_path.exists()
    assert [runner.key(argv) for argv in runner.calls][-2:] == [
        "external-action decide",
        "external-action show",
    ]
    assert "is now review state `edited`" in text
    assert "operator_approved" not in " ".join(
        token for argv in runner.calls for token in argv
    )


@pytest.mark.parametrize("decision", ["reject", "defer"])
def test_external_non_approval_does_not_require_execution_deadline(decision):
    pipe, runner = external_pipe()
    pipe.valves.external_action_decision_reason_code = "operator_decision"
    runner.responses["external-action decide"] = json_result({"ok": True})

    text = pipe.pipe(
        {"messages": [{"role": "user", "content": f"decide E1 {decision}"}]},
        __user__=ADMIN_USER,
    )

    assert "Decision outcome" in text
    assert "external_action_execute_before" not in text
    assert "external-action decide" in [runner.key(argv) for argv in runner.calls]


def test_external_api_row_cannot_choose_the_operator_reason_code():
    pipe, runner = external_pipe()
    pipe.valves.external_action_decision_reason_code = "operator_reason"
    runner.responses["external-action decide"] = json_result({"ok": True})
    original_runner = pipe._runner
    captured_body: dict[str, Any] = {}

    def capture(argv):
        if runner.key(argv) == "external-action decide":
            body_index = argv.index("--body-file") + 1
            captured_body.update(json.loads(Path(argv[body_index]).read_text()))
        return original_runner(argv)

    pipe._runner = capture
    row = dict(EXTERNAL_PROPOSAL)
    row["decision_reason_code"] = "api_selected_reason"
    runner.responses["external-action list"] = json_result({"proposals": [row]})
    runner.responses["external-action show"] = json_result(row)

    pipe.pipe(
        {"messages": [{"role": "user", "content": "decide E1 reject"}]},
        __user__=ADMIN_USER,
    )

    assert captured_body == {
        "verdict": "reject",
        "reason_code": "operator_reason",
    }


# -- the mapping is total, and it is the substrate's own vocabulary --------


def test_decision_type_mapping_total_over_rank_keys():
    """A new decision_type can never render as a blank or invented verb."""
    assert set(DECISION_TYPE_ACTIONS) == set(_DECISION_RANK), (
        "the Pipe's decision_type -> action mapping has drifted from "
        "review._DECISION_RANK; an unmapped type would render blank"
    )
    for decision_type in _DECISION_RANK:
        action = row_action(decision_type)
        assert action.text.strip(), f"{decision_type} has no rendered action"
        assert action.kind in {"executable", "instruction"}


def test_unmapped_decision_type_refuses_rather_than_blanks():
    with pytest.raises(UnmappedDecisionType):
        row_action("a-decision-type-that-does-not-exist")


# -- the module's literal tables are bound to the substrate's own ---------
# The Pipe cannot import okg (it runs inside Open WebUI), so it carries
# copies. These are the drift guards that make the copies safe.


def test_pact_verdict_vocabulary_matches_the_fsm():
    from okg.substrate.pact.fsm import HUMAN_VERDICTS

    assert set(PACT_HUMAN_VERDICTS) == set(HUMAN_VERDICTS)


def test_decidable_verdicts_are_a_subset_that_needs_no_extra_argument():
    from okg.substrate.chat.review_queue_pipe import PACT_DECIDABLE_VERDICTS

    assert set(PACT_DECIDABLE_VERDICTS) < set(PACT_HUMAN_VERDICTS)
    assert "supersede" not in PACT_DECIDABLE_VERDICTS, (
        "supersede requires --target, which the chat grammar cannot supply; "
        "offering it would advertise a verb that always fails"
    )


def test_the_forbidden_branch_literal_is_pinned():
    """The guard below must be checking the string the PACT names.

    If this drifts, every "the branch never appears" assertion in this
    suite goes on passing while checking a string nothing produces --
    absence dressed as agreement in the guard itself.
    """
    assert FORBIDDEN_BRANCH_LITERAL == "quarantine:review"


def test_the_pending_set_is_identified_by_deployment_and_mode_only():
    """No branch reaches the read, because there is no flag to carry one.

    Measured against the REAL parser rather than asserted: `okg admission
    list` has no `--branch` flag (cli/parser_groups.py:290-312), so a
    branch predicate could not be expressed even if this module wanted
    one. The positive control is the same parser accepting `--mode
    review`, which proves the parser is wired and the rejection below is
    about `--branch` and not about a dead harness.
    """
    import argparse

    from okg.substrate.chat.review_queue_pipe import build_admission_list_argv
    from okg.substrate.cli.parser_groups import register_admission_parser

    def parser():
        p = argparse.ArgumentParser()
        sub = p.add_subparsers(dest="cmd")
        register_admission_parser(
            sub, add_json_flag=lambda x: x.add_argument("--json", action="store_true")
        )
        return p

    argv = build_admission_list_argv("/bin/okg", DEPLOYMENT)[1:]
    # Positive control: the argv the Pipe actually builds parses, and the
    # two flags that IDENTIFY the pending set are the ones it carries.
    args = parser().parse_args(argv)
    assert args.deployment == DEPLOYMENT
    assert args.held is True
    assert args.mode is None
    assert not hasattr(args, "branch")

    # And a branch predicate is unexpressable against the same parser.
    with pytest.raises(SystemExit):
        parser().parse_args([*argv, "--branch", FORBIDDEN_BRANCH_LITERAL])


def test_the_mcp_write_path_drops_a_caller_supplied_branch():
    """The measurement the branch-free identity rests on.

    MCP `record`/`propose` -- the very writes this inbox reviews -- drop
    any caller-supplied `target_branch_name` (mcp/server.py:835), so a
    pending envelope from that path carries no branch. The positive
    control is a sibling key the same sanitizer preserves, which proves
    the call did something other than return an empty dict.
    """
    from okg.substrate.mcp.server import _sanitize_caller_admission_mode

    sanitized = _sanitize_caller_admission_mode(
        {
            "target_branch_name": FORBIDDEN_BRANCH_LITERAL,
            "producer_kind": "agent",
        }
    )
    assert "target_branch_name" not in sanitized
    assert sanitized.get("producer_kind") == "agent"  # control


def test_admission_decisions_are_accepted_by_the_cli_parser():
    """The two decisions this inbox offers must be real CLI choices."""
    import argparse

    from okg.substrate.cli.parser_groups import register_admission_parser

    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="cmd")
    register_admission_parser(
        sub,
        add_json_flag=lambda p: p.add_argument("--json", action="store_true"),
    )
    # Parsing succeeds only if the choice is in the CLI's own list.
    for decision in ADMISSION_DECISIONS:
        args = parser.parse_args(
            [
                "admission",
                "decide",
                "adm-1",
                "--deployment",
                DEPLOYMENT,
                "--decision",
                decision,
            ]
        )
        assert args.decision == decision


def test_admission_list_argv_is_accepted_by_the_cli_parser():
    """The read argv the Pipe builds must parse against the real parser."""
    import argparse

    from okg.substrate.chat.review_queue_pipe import build_admission_list_argv
    from okg.substrate.cli.parser_groups import register_admission_parser

    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="cmd")
    register_admission_parser(
        sub,
        add_json_flag=lambda p: p.add_argument("--json", action="store_true"),
    )
    argv = build_admission_list_argv("/bin/okg", DEPLOYMENT)[1:]
    args = parser.parse_args(argv)
    assert args.status == "pending"
    assert args.held is True
    assert args.mode is None
    assert args.deployment == DEPLOYMENT


def test_decide_verb_flag_spellings_differ_and_are_spelled_out():
    """The two decide verbs take DIFFERENT flag names."""
    review = DECISION_TYPE_ACTIONS["review"].text
    assert "okg pact decide" in review
    assert "--verdict" in review
    for verdict in PACT_HUMAN_VERDICTS:
        assert verdict in review
    assert "--decision" not in review

    from okg.substrate.chat.review_queue_pipe import admission_row_action

    admission = admission_row_action("adm-1", DEPLOYMENT)
    assert admission.startswith("okg admission decide adm-1")
    assert "--deployment" in admission
    assert "--decision" in admission
    assert "--verdict" not in admission
    for decision in ADMISSION_DECISIONS:
        assert decision in admission


def test_go_ahead_renders_the_gate_action_as_an_instruction():
    action = DECISION_TYPE_ACTIONS["go-ahead"]
    assert action.text == "okg pact gate task-done <change> <task>"
    assert action.kind == "instruction", (
        "okg pact gate is outside the enumerated review verb set, so the "
        "Pipe must render it rather than run it"
    )


def test_archive_renders_a_repo_move_never_an_invented_okg_verb():
    action = DECISION_TYPE_ACTIONS["archive"]
    assert action.kind == "instruction"
    assert "pact/archive/" in action.text
    assert "okg pact archive" not in action.text
    assert not action.text.startswith("okg ")


# -- one response, both kinds ---------------------------------------------


def test_one_response_lists_both_pending_kinds(both_kinds):
    _pipe, _runner, text = both_kinds
    assert "### PACT review items" in text
    assert "### Pending held admission envelopes" in text
    assert PENDING_ENVELOPE["admission_id"] in text
    assert "deployment-portfolio-lifecycle-contract" in text


def test_row_assembly_reads_both_cli_sources(both_kinds):
    _pipe, runner, _text = both_kinds
    keys = {runner.key(argv) for argv in runner.calls}
    assert keys == {"pact view", "admission list"}


def test_each_kind_survives_the_merge_independently(populated_queue):
    """Drop one source's rows and ONLY that kind disappears.

    The naive form of this test -- arrange an empty source, assert its
    rows are absent -- is a tautology that passes against an
    implementation that renders neither kind. The control here is the
    other half: it must still be present in both arms, so an
    implementation that drops everything fails.
    """
    empty_pact = {
        "ok": True,
        "format": "chat-review-queue",
        "chat_review_queue": {
            "rows": [],
            "shown": 0,
            "total": 0,
            "warnings": [],
        },
    }
    pact_only, _ = make_pipe(
        {
            "pact view": json_result(populated_queue),
            "admission list": json_result(admission_payload([])),
        }
    )
    text = pact_only.pipe({"messages": []}, __user__=ADMIN_USER)
    assert PENDING_ENVELOPE["admission_id"] not in text
    assert "deployment-portfolio-lifecycle-contract" in text  # control

    admission_only, _ = make_pipe(
        {
            "pact view": json_result(empty_pact),
            "admission list": json_result(admission_payload([PENDING_ENVELOPE])),
        }
    )
    text = admission_only.pipe({"messages": []}, __user__=ADMIN_USER)
    assert "deployment-portfolio-lifecycle-contract" not in text
    assert PENDING_ENVELOPE["admission_id"] in text  # control


def test_every_pact_row_carries_its_cli_action(both_kinds, populated_queue):
    _pipe, _runner, text = both_kinds
    rows = populated_queue["chat_review_queue"]["rows"]
    assert rows, "the captured fixture must be populated or this arm is vacuous"
    for row in rows:
        action = row_action(row["decision_type"])
        rendered = action.text.replace("<change>", row["change"])
        # The repair action carries the row's own note, so compare the
        # invariant head of the command rather than the whole string.
        head = rendered.split(" --note ")[0]
        assert head in text, f"row {row['change']} lost its action"


def test_admission_rows_carry_the_admission_decide_verb(both_kinds):
    _pipe, _runner, text = both_kinds
    expected = (
        f"okg admission decide {PENDING_ENVELOPE['admission_id']} "
        f"--deployment {DEPLOYMENT} --decision <accept|reject>"
    )
    assert expected in text


# -- coverage statement ----------------------------------------------------


def test_response_states_the_kinds_it_covers(both_kinds):
    _pipe, _runner, text = both_kinds
    assert "PACT review items" in text
    assert "pending held admission envelopes" in text
    # The coverage statement names the IDENTIFYING predicate, which is
    # deployment plus mode -- never a branch.
    assert "--held" in text


def test_the_populated_rendering_names_no_branch(both_kinds):
    """A populated inbox names no branch anywhere.

    Non-vacuity: this same rendering is asserted above to carry both
    kinds, so the absence below is measured against text that actually
    listed rows -- not against a response that rendered nothing.
    """
    _pipe, _runner, text = both_kinds
    assert PENDING_ENVELOPE["admission_id"] in text  # control: rows rendered
    assert FORBIDDEN_BRANCH_LITERAL not in text
    assert "branch" not in text.lower()


def _module_ast() -> tuple[ast.Module, set[int]]:
    """The Pipe module's AST, plus the ids of its docstring nodes.

    Used only by the IDENTIFIER scan below. The STRING scan deliberately
    does not go through the AST any more -- see
    `test_no_source_text_outside_the_module_docstring_carries_a_marker`.
    """
    from okg.substrate.chat import review_queue_pipe as module

    tree = ast.parse(Path(module.__file__).read_text())
    docstrings: set[int] = set()
    for node in ast.walk(tree):
        body = getattr(node, "body", None)
        if not isinstance(node, (ast.Module, ast.ClassDef, ast.FunctionDef)):
            continue
        if (
            body
            and isinstance(body[0], ast.Expr)
            and isinstance(body[0].value, ast.Constant)
            and isinstance(body[0].value.value, str)
        ):
            docstrings.add(id(body[0].value))
    return tree, docstrings


# --------------------------------------------------------------------------
# The reintroduction guard.
#
# The previous guard walked `ast.Constant` and skipped every docstring. It
# was unsound twice over. (1) A `Constant` walk sees only what the parser
# folded into one literal, so `"quarantine:" + "review"`, an f-string with
# the halves in separate slots, `":".join([...])`, `%`, `.format()`, a
# bytes literal plus `.decode()`, and a comprehension all slipped past it
# while producing the forbidden text at runtime. (2) The docstring
# exclusion covered EVERY docstring, and a function or class docstring is
# a live runtime string -- `__doc__` -- that a renderer can print.
#
# The guard is therefore two scans with ONE narrow, named exemption:
#
#   * RAW SOURCE TEXT, every line except the module docstring's own lines.
#     Every construction above must spell the marker characters somewhere
#     in the file, so the text is the thing to scan.
#   * The module's RUNTIME string surface -- module-level constants, every
#     function and class `__doc__`, the dataclass tables, and the Valve
#     descriptions -- which closes the `__doc__` hole at the level it
#     actually bites, and catches a construction that spells the marker
#     nowhere in the source (character arithmetic).
#
# The module docstring is exempted because it EXPLAINS why the predicate
# is excluded, and citing `target_branch_name` in that explanation is the
# evidence, not a reintroduction. Nothing else is exempt: every other
# comment and docstring in the module was reworded to name the contract
# rather than repeat the word.
# --------------------------------------------------------------------------

# Case-insensitive substrings that no source line outside the module
# docstring may carry. `quarantine` is scanned on its own, not only as
# part of the full literal, because every split construction above leaves
# the halves as separate tokens.
BRANCH_MARKERS = ("branch", "quarantine", FORBIDDEN_BRANCH_LITERAL)


def _module_source() -> str:
    from okg.substrate.chat import review_queue_pipe as module

    return Path(module.__file__).read_text()


def _module_docstring_lines(source: str) -> range:
    """The 1-based line span of the module docstring: the ONE exemption."""
    tree = ast.parse(source)
    head = tree.body[0] if tree.body else None
    if not (
        isinstance(head, ast.Expr)
        and isinstance(head.value, ast.Constant)
        and isinstance(head.value.value, str)
    ):
        raise AssertionError(
            "the module has no docstring, so the exemption below would be "
            "silently empty -- check the parse"
        )
    return range(head.lineno, (head.end_lineno or head.lineno) + 1)


def scan_source_for_markers(
    source: str, *, exempt_module_docstring: bool = True
) -> list[tuple[int, str, str]]:
    """(line number, marker, line) for every marker hit in the source text.

    With `exempt_module_docstring=False` nothing is exempt, which is how
    the exemption itself is measured.
    """
    exempt = (
        _module_docstring_lines(source) if exempt_module_docstring else range(0)
    )
    hits: list[tuple[int, str, str]] = []
    for number, line in enumerate(source.splitlines(), start=1):
        if number in exempt:
            continue
        lowered = line.lower()
        for marker in BRANCH_MARKERS:
            if marker.lower() in lowered:
                hits.append((number, marker, line.strip()))
    return hits


def test_the_module_docstring_is_the_only_exempted_region():
    """Name the exemption, and prove it is load-bearing rather than idle.

    Measured, not asserted: the SAME scan with nothing exempted must find
    hits, and every one of them must fall inside the exempted span. That
    proves at once that the exemption is doing work (drop it and the scan
    fires) and that it is the only thing being excused.
    """
    source = _module_source()
    exempt = _module_docstring_lines(source)
    assert exempt.start == 1, "the exemption must be the file's own header"

    unexempted = scan_source_for_markers(source, exempt_module_docstring=False)
    assert unexempted, (
        "the exempted region carries no marker at all, so exempting it "
        "protects nothing and the exemption should be deleted"
    )
    outside = [hit for hit in unexempted if hit[0] not in exempt]
    assert not outside, outside

    # And the exemption is a small header, not most of the file.
    lines = source.splitlines()
    assert len(exempt) < len(lines) // 4, (
        f"the exemption spans {len(exempt)} of {len(lines)} lines; an "
        "exemption that large is not narrow"
    )


def test_no_source_text_outside_the_module_docstring_carries_a_marker():
    """Reintroduction is a source-level impossibility, not a review note.

    Scanned as TEXT. An `ast.Constant` walk sees only parser-folded
    literals, so it passes against a module that assembles the forbidden
    string at runtime from pieces no single literal contains.
    """
    hits = scan_source_for_markers(_module_source())
    assert not hits, "\n".join(f"line {n}: [{m}] {line}" for n, m, line in hits)


# The reintroduction corpus. Each row is
# (label, snippet, caught_by_source_scan, caught_by_runtime_scan,
#  caught_by_the_OLD_ast_constant_scan).
#
# The last column keeps the adversarial finding a MEASURED fact rather
# than a claim in a commit message, and the first two record which of the
# two scans reaches each form -- they are complementary, not redundant.
# `split-word` and `computed` spell no marker contiguously in the source
# and are reachable ONLY at runtime; the docstring forms are reachable by
# both. Every form must be caught by at least one.
REINTRODUCTION_FORMS = [
    ("plain-literal", 'LEAK = "quarantine:review"', True, True, True),
    ("adjacent-literal", 'LEAK = ("quarantine:" "review")', True, True, True),
    ("bare-word", 'LEAK = "the branch is named here"', True, True, True),
    ("binop-concat", 'LEAK = "quarantine:" + "review"', True, True, False),
    ("split-fstring", "LEAK = f\"{'quarantine'}:{'review'}\"", True, True, False),
    ("fstring-name", '_Q = "quarantine"\nLEAK = f"{_Q}:review"', True, True, False),
    ("str-join", 'LEAK = ":".join(["quarantine", "review"])', True, True, False),
    ("percent-format", 'LEAK = "%s:%s" % ("quarantine", "review")', True, True, False),
    ("str-format", 'LEAK = "{}:{}".format("quarantine", "review")', True, True, False),
    ("bytes-decode", 'LEAK = b"quarantine:review".decode()', True, True, False),
    ("list-join", 'LEAK = "".join(["quarantine", ":", "review"])', True, True, False),
    (
        "comprehension",
        'LEAK = "".join(p for p in ["quarantine", ":", "review"])',
        True,
        True,
        False,
    ),
    (
        "augmented-assign",
        '_P = "quarantine"\n_P += ":review"\nLEAK = _P',
        True,
        True,
        False,
    ),
    (
        "function-doc",
        'def _leak() -> None:\n    """quarantine:review"""\n',
        True,
        True,
        False,
    ),
    (
        "class-doc",
        'class _Leak:\n    """quarantine:review"""\n',
        True,
        True,
        False,
    ),
    # Splits the MARKER WORD itself across adjacent literals, so the raw
    # text carries no contiguous "quarantine". Only the runtime scan sees
    # it -- and it is why the runtime scan exists.
    ("split-word", 'LEAK = "quaran" "tine:" + "review"', False, True, False),
    # Spells nothing at all: the string is built from code points.
    (
        "computed",
        'LEAK = "".join(chr(c) for c in (113, 117, 97, 114, 97, 110, 116, '
        "105, 110, 101, 58, 114, 101, 118, 105, 101, 119))",
        False,
        True,
        False,
    ),
]


def _load_mutated_module(tmp_path, label: str, snippet: str):
    """Import the Pipe module with `snippet` appended, as a real module."""
    import importlib.util
    import sys

    name = f"okg_review_queue_pipe_mutant_{label.replace('-', '_')}"
    path = tmp_path / f"{name}.py"
    path.write_text(_module_source() + f"\n\n# mutation: {label}\n{snippet}\n")
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    # `@dataclass` resolves annotations through `sys.modules`, so the
    # module must be registered BEFORE it executes. Removed again so a
    # mutant can never leak into another test.
    sys.modules[name] = module
    try:
        spec.loader.exec_module(module)
        return _runtime_strings(module)
    finally:
        sys.modules.pop(name, None)


def _ast_constant_scan(source: str) -> list[str]:
    """The PREVIOUS guard, reproduced so its holes can be measured."""
    tree = ast.parse(source)
    docstrings: set[int] = set()
    for node in ast.walk(tree):
        body = getattr(node, "body", None)
        if not isinstance(node, (ast.Module, ast.ClassDef, ast.FunctionDef)):
            continue
        if (
            body
            and isinstance(body[0], ast.Expr)
            and isinstance(body[0].value, ast.Constant)
            and isinstance(body[0].value.value, str)
        ):
            docstrings.add(id(body[0].value))
    return [
        node.value
        for node in ast.walk(tree)
        if isinstance(node, ast.Constant)
        and isinstance(node.value, str)
        and id(node) not in docstrings
        and (FORBIDDEN_BRANCH_LITERAL in node.value or "branch" in node.value.lower())
    ]


@pytest.mark.parametrize(
    "label,snippet,by_source,by_runtime,by_old",
    REINTRODUCTION_FORMS,
    ids=[form[0] for form in REINTRODUCTION_FORMS],
)
def test_every_known_reintroduction_form_is_caught(
    tmp_path, label, snippet, by_source, by_runtime, by_old
):
    """The mutation corpus, run against ALL THREE guards.

    The combined guard must flag every form. Each scan's individual reach
    is asserted exactly as recorded -- so "the source scan alone is not
    enough" and "the previous guard was bypassable" are test results
    rather than prose.
    """
    mutated = _module_source() + f"\n\n# injected mutation: {label}\n{snippet}\n"
    ast.parse(mutated)  # the mutation must be real, loadable Python

    source_hit = bool(scan_source_for_markers(mutated))
    runtime_hit = any(
        marker.lower() in text.lower()
        for _where, text in _load_mutated_module(tmp_path, label, snippet)
        for marker in BRANCH_MARKERS
    )
    old_hit = bool(_ast_constant_scan(mutated))

    assert source_hit or runtime_hit, (
        f"the {label} reintroduction was caught by neither scan"
    )
    assert source_hit is by_source, f"{label}: source scan reach changed"
    assert runtime_hit is by_runtime, f"{label}: runtime scan reach changed"
    assert old_hit is by_old, (
        f"the previous ast.Constant guard's behaviour on {label} has changed; "
        f"it was recorded as caught={by_old}"
    )


def test_the_old_guard_was_bypassable_and_the_two_scans_are_complementary():
    """Summarise the corpus, so the improvement is readable in one place."""
    source = _module_source()
    # Controls: the real, unmutated module trips no scan.
    assert not scan_source_for_markers(source)
    assert not _ast_constant_scan(source)

    bypassed_old = [row[0] for row in REINTRODUCTION_FORMS if not row[4]]
    assert len(bypassed_old) >= 10, (
        "the recorded bypass corpus shrank; the finding was 10+ forms that "
        "the ast.Constant guard could not see"
    )
    # Neither scan alone covers the corpus, and together they do.
    assert [row[0] for row in REINTRODUCTION_FORMS if not row[2]], (
        "if the source scan covered everything the runtime scan would be "
        "redundant -- say so rather than keeping a scan nothing needs"
    )
    assert all(row[2] or row[3] for row in REINTRODUCTION_FORMS)


def _runtime_strings(module) -> list[tuple[str, str]]:
    """(where, text) for every string the module EXPOSES at runtime.

    Module-level constants, containers of strings, dataclass tables,
    class and function `__doc__`, and the Valve field descriptions. The
    module's own `__doc__` is the one exemption, matching the source scan.
    """
    found: list[tuple[str, str]] = []
    seen: set[int] = set()

    def visit(where: str, value: object, depth: int = 0) -> None:
        if depth > 6 or id(value) in seen:
            return
        seen.add(id(value))
        if isinstance(value, str):
            found.append((where, value))
            return
        if isinstance(value, bytes):
            found.append((where, value.decode("utf-8", "replace")))
            return
        if isinstance(value, Mapping):
            for key, item in value.items():
                visit(f"{where}[{key!r}]", key, depth + 1)
                visit(f"{where}[{key!r}]", item, depth + 1)
            return
        if isinstance(value, (list, tuple, set, frozenset)):
            for index, item in enumerate(value):
                visit(f"{where}[{index}]", item, depth + 1)
            return
        doc = getattr(value, "__doc__", None)
        if isinstance(doc, str) and value is not module:
            found.append((f"{where}.__doc__", doc))
        if isinstance(value, type) or hasattr(value, "__dataclass_fields__"):
            for name, item in vars(value).items():
                if name.startswith("__"):
                    continue
                visit(f"{where}.{name}", item, depth + 1)
        fields = getattr(value, "model_fields", None)
        if isinstance(fields, Mapping):
            for name, field in fields.items():
                visit(f"{where}.model_fields[{name!r}]", field.description, depth + 1)
        if hasattr(value, "__dataclass_fields__") and not isinstance(value, type):
            for name in value.__dataclass_fields__:
                visit(f"{where}.{name}", getattr(value, name, None), depth + 1)

    for name, value in vars(module).items():
        if name == "__doc__" or name.startswith("__"):
            continue
        visit(name, value)
    return found


def test_no_runtime_string_the_module_exposes_carries_a_marker():
    """The `__doc__` hole, closed where it bites rather than by proxy.

    A function or class docstring is a live string a renderer can print.
    The source scan already covers the ones written as text; this covers
    the whole exposed surface, including strings assembled at import time.
    """
    from okg.substrate.chat import review_queue_pipe as module

    strings = _runtime_strings(module)
    assert strings, "the runtime walk found no strings at all -- check it"
    # Control: the walk really reaches the rendered vocabulary and the
    # docstrings, so the absence claim below is measured against a
    # populated surface.
    texts = [text for _where, text in strings]
    assert any("Workflow escalations are not covered" in text for text in texts)
    assert any("okg pact decide" in text for text in texts)
    assert any(where.endswith(".__doc__") for where, _text in strings)

    offenders = [
        (where, text)
        for where, text in strings
        if any(marker.lower() in text.lower() for marker in BRANCH_MARKERS)
    ]
    assert not offenders, offenders


def test_no_identifier_in_the_module_is_branch_named():
    """No constant, Valve, parameter, or function can carry a branch."""
    tree, _ = _module_ast()
    names: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Name):
            names.append(node.id)
        elif isinstance(node, ast.Attribute):
            names.append(node.attr)
        elif isinstance(node, ast.arg):
            names.append(node.arg)
        elif isinstance(node, (ast.FunctionDef, ast.ClassDef)):
            names.append(node.name)
        elif isinstance(node, ast.keyword) and node.arg:
            names.append(node.arg)
    assert names, "the AST walk found no identifiers -- check the parse"
    assert "deployment" in names, "control: the identifiers were really read"
    assert not [name for name in names if "branch" in name.lower()]


def test_the_valves_carry_no_branch_field():
    """No Valve can reintroduce a branch predicate by configuration."""
    from okg.substrate.chat.review_queue_pipe import Pipe

    fields = set(Pipe.Valves.model_fields)
    assert not any("branch" in name for name in fields), sorted(fields)
    # Control: the Valves that DO identify the pending set are present, so
    # the assertion above is not passing against an empty model.
    assert {"deployment", "okg_cli", "repo_root", "admin_group"} <= fields


def test_response_names_workflow_escalations_as_not_covered(both_kinds):
    _pipe, _runner, text = both_kinds
    assert "Workflow escalations are not covered" in text


def test_escalations_are_never_listed_created_or_decided(both_kinds, runner=None):
    _pipe, runner, text = both_kinds
    for argv in runner.calls:
        assert "escalation" not in " ".join(argv)
        assert "escalate" not in " ".join(argv)
    assert "request_escalation" not in text


# -- truncation ------------------------------------------------------------


def test_truncated_queue_states_shown_of_total(both_kinds, populated_queue):
    _pipe, _runner, text = both_kinds
    packet = populated_queue["chat_review_queue"]
    assert packet["shown"] < packet["total"], (
        "the captured fixture must be truncated or this arm proves nothing"
    )
    assert f"{packet['shown']} of {packet['total']}" in text


def test_saturated_admission_read_refuses_to_state_a_total():
    """`okg admission list` reports `count` AFTER applying `--limit`.

    A saturated read therefore knows nothing about the true total, and
    printing the truncated length as "N pending" would be the
    shown-as-total defect. Non-vacuity: the row count here equals the
    limit exactly, which is the only way the CLI signals saturation.
    """
    from okg.substrate.chat.review_queue_pipe import ADMISSION_LIST_LIMIT

    rows = [
        dict(PENDING_ENVELOPE, admission_id=f"adm-{index}")
        for index in range(ADMISSION_LIST_LIMIT)
    ]
    pipe, _ = make_pipe(
        {
            "pact view": json_result(
                {
                    "ok": True,
                    "chat_review_queue": {
                        "rows": [],
                        "shown": 0,
                        "total": 0,
                        "warnings": [],
                    },
                }
            ),
            "admission list": json_result(admission_payload(rows)),
        }
    )
    text = pipe.pipe({"messages": []}, __user__=ADMIN_USER)
    assert "the total is not known" in text
    assert f"{ADMISSION_LIST_LIMIT} pending." not in text


def test_unsaturated_admission_read_states_the_total():
    """Non-vacuity control for the saturation arm."""
    rows = [
        dict(PENDING_ENVELOPE, admission_id=f"adm-{index}") for index in range(3)
    ]
    pipe, _ = make_pipe(
        {
            "pact view": json_result(
                {
                    "ok": True,
                    "chat_review_queue": {
                        "rows": [],
                        "shown": 0,
                        "total": 0,
                        "warnings": [],
                    },
                }
            ),
            "admission list": json_result(admission_payload(rows)),
        }
    )
    text = pipe.pipe({"messages": []}, __user__=ADMIN_USER)
    assert "3 pending." in text
    assert "the total is not known" not in text


def test_admission_read_asks_for_the_cli_limit_ceiling():
    """The default `--limit 20` would silently truncate at 20."""
    from okg.substrate.chat.review_queue_pipe import (
        ADMISSION_LIST_LIMIT,
        build_admission_list_argv,
    )

    argv = build_admission_list_argv("/bin/okg", DEPLOYMENT)
    assert "--limit" in argv
    assert argv[argv.index("--limit") + 1] == str(ADMISSION_LIST_LIMIT)


def test_untruncated_queue_does_not_claim_shown_of_total():
    packet = {
        "rows": [
            {
                "change": "one-change",
                "title": "One change",
                "decision_type": "review",
            }
        ],
        "shown": 1,
        "total": 1,
        "warnings": [],
    }
    pipe, _ = make_pipe(
        {
            "pact view": json_result(
                {"ok": True, "chat_review_queue": packet}
            ),
            "admission list": json_result(admission_payload([])),
        }
    )
    text = pipe.pipe({"messages": []}, __user__=ADMIN_USER)
    assert "1 of 1" not in text
    assert "1 waiting." in text


# -- the --query path ------------------------------------------------------


def test_query_turn_renders_the_packet_that_came_back(populated_queue):
    """An operator question goes through --query and the packet answers."""
    pipe, runner = make_pipe(
        {
            "pact view": json_result(populated_queue),
            "admission list": json_result(admission_payload([])),
        }
    )
    question = "what's blocking the agents' work?"
    text = pipe.pipe(
        {"messages": [{"role": "user", "content": question}]},
        __user__=ADMIN_USER,
    )
    view_argv = next(
        argv for argv in runner.calls if runner.key(argv) == "pact view"
    )
    assert "--query" in view_argv
    assert view_argv[view_argv.index("--query") + 1] == question
    assert "### PACT review items" in text


def test_query_turn_renders_the_answer_first_summary_not_raw_rank_rows(
    query_queue,
):
    """The --query packet's own answer leads; its sections replace the rank
    list. Captured from a real `--query` run in this worktree."""
    pipe, _ = make_pipe(
        {
            "pact view": json_result(query_queue),
            "admission list": json_result(admission_payload([])),
        }
    )
    text = pipe.pipe(
        {
            "messages": [
                {"role": "user", "content": "what's blocking the agents' work?"}
            ]
        },
        __user__=ADMIN_USER,
    )
    packet = query_queue["chat_review_queue"]
    assert packet["answer"] in text
    assert packet["interpreted_as"] in text
    for section in packet["sections"]:
        assert section["title"] in text
    assert "Read as:" in text


def test_query_section_rows_keep_the_packet_row_numbers(query_queue):
    """One row keeps one handle, so `decide P<n>` resolves the same row."""
    pipe, _ = make_pipe(
        {
            "pact view": json_result(query_queue),
            "admission list": json_result(admission_payload([])),
        }
    )
    text = pipe.pipe(
        {"messages": [{"role": "user", "content": "what blocks agents?"}]},
        __user__=ADMIN_USER,
    )
    numbered = [
        row
        for section in query_queue["chat_review_queue"]["sections"]
        for row in section["rows"]
    ]
    assert numbered, "the captured query packet must carry section rows"
    for row in numbered:
        assert f"**P{row['number']} `{row['change']}`**" in text


def test_no_query_turn_renders_the_rank_list_not_the_answer(populated_queue):
    """Non-vacuity control for the answer-first arm."""
    pipe, _ = make_pipe(
        {
            "pact view": json_result(populated_queue),
            "admission list": json_result(admission_payload([])),
        }
    )
    text = pipe.pipe({"messages": []}, __user__=ADMIN_USER)
    assert "Read as:" not in text
    assert populated_queue["chat_review_queue"]["answer"] not in text


def test_module_is_self_contained():
    """The Open WebUI process does not import okg; neither may this module.

    It is copied verbatim into an instance as a function file, so an
    `okg.*` import would make it unloadable there -- and the failure would
    surface as a missing model in the picker, not as a clear error.
    """
    import ast

    from okg.substrate.chat import review_queue_pipe as module

    # Walk the AST, not line prefixes: a LAZY (indented, in-function)
    # `import okg.…` is exactly the form a future contributor would reach
    # for, and a startswith() filter would never see it.
    tree = ast.parse(Path(module.__file__).read_text())
    imported: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported += [alias.name for alias in node.names]
        elif isinstance(node, ast.ImportFrom):
            imported.append(node.module or "")
    offenders = [name for name in imported if name == "okg" or name.startswith("okg.")]
    assert not offenders, f"the Pipe module must not import from okg.*: {offenders}"
    assert imported, "the AST walk found no imports at all -- check the parse"


def test_pipe_registers_one_model_in_the_picker():
    from okg.substrate.chat.review_queue_pipe import MODEL_ID, Pipe

    entries = Pipe().pipes()
    assert [entry["id"] for entry in entries] == [MODEL_ID]


def test_fixture_is_real_cli_output(populated_queue):
    """Guard: the fixture must keep the shape the CLI actually emits."""
    assert populated_queue["format"] == "chat-review-queue"
    packet = populated_queue["chat_review_queue"]
    for key in ("rows", "shown", "total", "warnings", "answer", "summary_lines"):
        assert key in packet, f"captured fixture lost {key}"
    assert json.dumps(packet)  # serializable, i.e. plain JSON


def _section_packet(rows):
    """A --query packet whose ONE section carries the given rows."""
    return {
        "rows": [],
        "shown": 0,
        "total": 9,
        "warnings": [],
        "answer": "an answer",
        "summary_lines": [],
        "sections": [{"title": "Excluded examples", "rows": rows}],
    }


def test_an_unnumbered_section_row_gets_no_handle_it_could_be_decided_by():
    """An unnumbered `--query` row must not borrow a rank-list number.

    A decision turn resolves `P<n>` against the RANK LIST, never against
    the sections it just rendered. `_chat_stale_queue_row`
    (src/okg/substrate/pact/cli.py) emits rows carrying neither `number`
    nor `decision_type`, and they reach the stale packet and the
    batch-low-risk packet's excluded examples. Numbering them by their
    position inside a section put TWO different rows under `P2` in one
    response, so `decide P2 approve` would have written to a DIFFERENT
    change -- with the id named only after the write landed.
    """
    numbered = {
        "change": "real-change",
        "title": "t",
        "decision_type": "review",
        "number": 7,
    }
    unnumbered = {"change": "stale-change", "title": "t"}
    assert packet_row_handle(numbered) == 7
    assert packet_row_handle(unnumbered) is None

    outcome = SourceOutcome(
        source="pact",
        state=POPULATED,
        command="okg pact view",
        rows=(),
        shown=0,
        total=9,
        packet=_section_packet([unnumbered, unnumbered]),
    )
    text = "\n".join(render_pact_section(outcome, answer_first=True))
    assert "stale-change" in text, "the row must still be shown"
    assert "P1" not in text and "P2" not in text, (
        f"an unnumbered section row invented a handle:\n{text}"
    )
    # A stale row carries no decision_type either, so it additionally
    # renders as unmapped rather than as something runnable.
    assert "no action mapped" in text

    # A row that DOES map to an action but still carries no number must
    # say plainly that it has no handle to be decided by.
    actionable = {"change": "unnumbered-change", "title": "t", "decision_type": "review"}
    outcome = SourceOutcome(
        source="pact",
        state=POPULATED,
        command="okg pact view",
        rows=(),
        shown=0,
        total=9,
        packet=_section_packet([actionable]),
    )
    text = "\n".join(render_pact_section(outcome, answer_first=True))
    assert "unnumbered-change" in text
    assert "P1" not in text, f"an unnumbered section row invented a handle:\n{text}"
    assert "cannot be decided from here" in text


def test_a_numbered_section_row_keeps_its_packet_handle():
    """Control: the fix must not strip handles from rows that HAVE one."""
    outcome = SourceOutcome(
        source="pact",
        state=POPULATED,
        command="okg pact view",
        rows=(),
        shown=0,
        total=9,
        packet=_section_packet(
            [
                {
                    "change": "real-change",
                    "title": "t",
                    "decision_type": "review",
                    "number": 4,
                }
            ]
        ),
    )
    text = "\n".join(render_pact_section(outcome, answer_first=True))
    assert "**P4 `real-change`**" in text
    assert "cannot be decided from here" not in text


def test_a_pact_outcome_with_no_counts_refuses_instead_of_counting_rows():
    """No default-value-as-fallback in the count.

    Substituting len(rows) for a missing total manufactures a count
    nobody reported -- the shown-as-total defect, which the admission
    half already refuses. The two halves must agree.
    """
    outcome = SourceOutcome(
        source="pact",
        state=POPULATED,
        command="okg pact view --format=chat-review-queue --json",
        rows=({"change": "c1", "title": "t", "decision_type": "review"},),
        shown=None,
        total=None,
        packet={"rows": [], "warnings": []},
    )
    text = "\n".join(render_pact_section(outcome))
    assert "1 waiting" not in text, f"a count was manufactured from the rows:\n{text}"
    assert "refusing to report a count" in text


def test_a_pact_outcome_with_counts_still_states_them():
    """Its control: a real count is still rendered."""
    outcome = SourceOutcome(
        source="pact",
        state=POPULATED,
        command="okg pact view",
        rows=({"change": "c1", "title": "t", "decision_type": "review"},),
        shown=1,
        total=5,
        packet={"rows": [], "warnings": []},
    )
    text = "\n".join(render_pact_section(outcome))
    assert "Showing **1 of 5**" in text
    assert "refusing to report a count" not in text
