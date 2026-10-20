"""Empty and unavailable are different answers.

req.chat-review-queue-pipe.verified-empty. Four rendered states:

  (a) both sources answer with zero rows      -> per-half verified-empty
  (b) the admission source fails              -> refusal, no zero count
  (c) the PACT source fails, admission answers -> not the whole inbox
  (d) LEDGER DOWN BUT EXIT 0                  -> refusal, nothing rendered
      from the packet

State (d) is the one that matters. Its fixture is REAL captured CLI
output: `okg pact view --format=chat-review-queue --json` run with the
decision ledger deliberately unreachable exits 0, reports `ok: true`,
emits valid JSON, silently drops rows (28 -> 23 in this worktree, and
206 stale claims -> 0), and still answers "Nothing urgent is blocking
active work." Exit code and JSON validity are therefore PROVEN
insufficient as availability signals, and the tests at the bottom of this
file re-run every assertion against a rendering path that uses only those
two signals -- the pre-fix path -- to prove the assertions are not vacuous.
"""

from __future__ import annotations

import json

import pytest

from okg.substrate.chat.review_queue_pipe import (
    POPULATED,
    UNAVAILABLE,
    VERIFIED_EMPTY,
    CommandResult,
    SourceOutcome,
    classify_admission_result,
    classify_pact_result,
    render_admission_section,
    render_pact_section,
)
from tests.substrate.chat.conftest import (
    ADMIN_USER,
    CLI,
    DEPLOYMENT,
    FORBIDDEN_BRANCH_LITERAL,
    PENDING_ENVELOPE,
    admission_payload,
    json_result,
    make_pipe,
    result,
)

COMMIT_SHA = "c0ffee1234567890c0ffee1234567890c0ffee12"

EMPTY_PACKET = {
    "ok": True,
    "format": "chat-review-queue",
    "chat_review_queue": {
        "rows": [],
        "shown": 0,
        "total": 0,
        "warnings": [],
        "answer": "Nothing urgent is blocking active work.",
        "summary_lines": ["Urgent blockers: 0"],
        "stale_rows": [],
        "stale_total": 0,
    },
}


def _render(responses, **kwargs) -> str:
    pipe, runner = make_pipe(responses, **kwargs)
    text = pipe.pipe({"messages": []}, __user__=ADMIN_USER)
    return text


def _section(text: str, heading: str) -> str:
    start = text.index(heading)
    rest = text[start + len(heading) :]
    end = rest.find("\n### ")
    return rest if end < 0 else rest[:end]


# -- (a) both empty --------------------------------------------------------


def test_a_both_empty_renders_per_half_verified_empty_statements():
    text = _render(
        {
            "pact view": json_result(EMPTY_PACKET),
            "admission list": json_result(admission_payload([])),
        },
        commit_sha=COMMIT_SHA,
    )
    assert "PACT queue: verified empty" in text
    assert "pact/changes" in text
    assert COMMIT_SHA in text
    assert "decision ledger reachable" in text

    # Deployment, mode, timestamp -- and NOTHING else. The timestamp is
    # asserted separately in the next test.
    assert "Admission: verified empty" in text
    assert DEPLOYMENT in text
    assert "--held" in text


def test_a_admission_empty_statement_names_a_query_timestamp():
    text = _render(
        {
            "pact view": json_result(EMPTY_PACKET),
            "admission list": json_result(admission_payload([])),
        },
        commit_sha=COMMIT_SHA,
    )
    admission = _section(text, "### Pending held admission envelopes")
    # An ISO-8601 UTC instant captured around the read, not a generation id.
    assert "T" in admission and "+00:00" in admission
    assert "generation" not in admission.lower(), (
        "no verb in the enumerated set produces a generation on the empty "
        "path; claiming one would be default-value-as-provenance"
    )


def test_a_empty_is_never_a_blank_or_unprovenanced_message():
    text = _render(
        {
            "pact view": json_result(EMPTY_PACKET),
            "admission list": json_result(admission_payload([])),
        },
        commit_sha=COMMIT_SHA,
    )
    assert "nothing to do" not in text.lower()
    assert text.strip()


def test_a_unresolvable_commit_sha_refuses_rather_than_claiming_empty():
    """The sha IS the authority; without it the claim is unprovenanced."""
    text = _render(
        {
            "pact view": json_result(EMPTY_PACKET),
            "admission list": json_result(admission_payload([])),
        },
        commit_sha="",
        sha_error="`git -C /srv/okg-repo rev-parse HEAD` exited 128",
    )
    pact = _section(text, "### PACT review items")
    assert "verified empty" not in pact or "refusing" in pact.lower()
    assert "Unavailable" in pact
    assert "rev-parse" in pact


# -- the branch literal appears in NO rendered state ----------------------
# The requirement's flagship condition. A verified-empty statement naming
# a branch would assert the emptiness of a referent that no MCP-created
# envelope occupies (mcp/server.py:835, :2727-2731), while a pending
# envelope exists -- absence dressed as agreement in the one surface built
# to exclude it.


@pytest.fixture
def four_states(populated_queue, ledger_down_queue) -> dict[str, str]:
    """All four rendered states of this requirement, in one place."""
    return {
        "a-both-empty": _render(
            {
                "pact view": json_result(EMPTY_PACKET),
                "admission list": json_result(admission_payload([])),
            },
            commit_sha=COMMIT_SHA,
        ),
        "b-admission-failed": _render(
            {
                "pact view": json_result(populated_queue),
                "admission list": result("", returncode=2, stderr="dsn unreachable"),
            }
        ),
        "c-pact-failed": _render(
            {
                "pact view": result("", returncode=1, stderr="ledger DSN refused"),
                "admission list": json_result(admission_payload([PENDING_ENVELOPE])),
            }
        ),
        "d-ledger-down": _render(
            {
                "pact view": json_result(ledger_down_queue),
                "admission list": json_result(admission_payload([PENDING_ENVELOPE])),
            }
        ),
    }


# What proves each arm is the arm it claims to be. Without these the
# absence assertion below would pass just as well against four empty
# strings.
_STATE_MARKERS = {
    "a-both-empty": "Admission: verified empty",
    "b-admission-failed": "This is not the whole inbox",
    "c-pact-failed": "refusing to report a count",
    "d-ledger-down": "decision ledger is unavailable",
}


@pytest.mark.parametrize("state", sorted(_STATE_MARKERS))
def test_no_rendered_state_names_a_branch(four_states, state):
    text = four_states[state]
    # Two positive controls before the absence claim: the arm rendered
    # something, and it rendered THIS arm.
    assert text.strip(), f"{state} rendered nothing at all"
    assert _STATE_MARKERS[state] in text, f"{state} is not the state it claims"
    assert FORBIDDEN_BRANCH_LITERAL not in text
    assert "branch" not in text.lower()


# A Valve value carrying BOTH predicates the absence assertion checks:
# the forbidden literal, and the bare word. Valves are operator-typed
# strings, so this is a value the production renderers really can be
# handed.
POISONED_NAME = f"okg-workspace-branch-{FORBIDDEN_BRANCH_LITERAL}"


def test_the_absence_assertion_can_fail_through_production_rendering():
    """Mutation control for the assertion above, executing REAL code.

    The predicate must be falsifiable by the renderers themselves, or
    `FORBIDDEN_BRANCH_LITERAL not in text` is passing because the literal
    is unproduceable by anything rather than because these renderers
    stopped producing it. Hand-appending the literal to a finished string
    proves only that Python concatenates -- it runs no production code
    and would keep passing against a module that had been deleted.

    Three arms, all through production entry points: the whole `Pipe`
    turn, and each half's renderer on its own.
    """
    # (1) The full turn, driven by a poisoned `deployment` Valve.
    whole_turn = _render(
        {
            "pact view": json_result(EMPTY_PACKET),
            "admission list": json_result(admission_payload([])),
        },
        deployment=POISONED_NAME,
        commit_sha=COMMIT_SHA,
    )
    assert "Admission: verified empty" in whole_turn, (
        "the poisoned arm must reach the same verified-empty statement the "
        "absence assertion is made against"
    )
    assert FORBIDDEN_BRANCH_LITERAL in whole_turn
    assert "branch" in whole_turn.lower()

    # (2) The admission renderer alone.
    admission = "\n".join(
        render_admission_section(
            SourceOutcome(
                source="admission",
                state=VERIFIED_EMPTY,
                command="okg admission list",
                rows=(),
                shown=0,
                total=0,
                emptiness_authority="2026-08-11T00:00:00+00:00",
            ),
            deployment=POISONED_NAME,
        )
    )
    assert FORBIDDEN_BRANCH_LITERAL in admission
    assert "branch" in admission.lower()

    # (3) The PACT renderer alone, poisoned through the echoed command.
    pact = "\n".join(
        render_pact_section(
            SourceOutcome(
                source="pact",
                state=VERIFIED_EMPTY,
                command=f"okg pact --root /srv/{POISONED_NAME} view",
                rows=(),
                shown=0,
                total=0,
            ),
            commit_sha=COMMIT_SHA,
        )
    )
    assert FORBIDDEN_BRANCH_LITERAL in pact
    assert "branch" in pact.lower()


def test_the_unpoisoned_renderers_produce_neither_predicate():
    """Non-vacuity control for the arm above, in the SAME suite.

    Same three entry points, ordinary inputs. Without this the poisoned
    arms would be satisfied by renderers that always emit the literal.
    """
    whole_turn = _render(
        {
            "pact view": json_result(EMPTY_PACKET),
            "admission list": json_result(admission_payload([])),
        },
        commit_sha=COMMIT_SHA,
    )
    assert "Admission: verified empty" in whole_turn
    assert FORBIDDEN_BRANCH_LITERAL not in whole_turn
    assert "branch" not in whole_turn.lower()

    admission = "\n".join(
        render_admission_section(
            SourceOutcome(
                source="admission",
                state=VERIFIED_EMPTY,
                command="okg admission list",
                rows=(),
                shown=0,
                total=0,
                emptiness_authority="2026-08-11T00:00:00+00:00",
            ),
            deployment=DEPLOYMENT,
        )
    )
    assert "Admission: verified empty" in admission
    assert FORBIDDEN_BRANCH_LITERAL not in admission
    assert "branch" not in admission.lower()

    pact = "\n".join(
        render_pact_section(
            SourceOutcome(
                source="pact",
                state=VERIFIED_EMPTY,
                command="okg pact --root /srv/okg-repo view",
                rows=(),
                shown=0,
                total=0,
            ),
            commit_sha=COMMIT_SHA,
        )
    )
    assert "verified empty" in pact
    assert FORBIDDEN_BRANCH_LITERAL not in pact
    assert "branch" not in pact.lower()


def test_the_verified_empty_statement_names_deployment_mode_and_time(four_states):
    """Non-vacuity for the branch-free empty statement.

    The admission half must still be a STATEMENT -- deployment, mode,
    timestamp -- after the branch was removed from it. Deleting the whole
    sentence would satisfy the absence assertion above just as well.
    """
    admission = _section(
        four_states["a-both-empty"], "### Pending held admission envelopes"
    )
    # The STATEMENT LINE itself, not the section. The section also carries
    # the `Command:` echo, which contains `--held` for free -- so a
    # section-wide assertion would keep passing against a statement that
    # had dropped the mode entirely. This is the mutation that exposed it.
    statement = next(
        line
        for line in admission.splitlines()
        if "Admission: verified empty" in line
    )
    assert DEPLOYMENT in statement
    assert "--held" in statement
    assert "+00:00" in statement, "the query timestamp is the emptiness authority"
    assert "branch" not in statement.lower()


# -- (b) admission source fails -------------------------------------------


@pytest.mark.parametrize(
    "failure",
    [
        result("", returncode=2, stderr="could not connect to server"),
        result("{not json", returncode=0),
    ],
    ids=["non-zero-exit", "malformed-json"],
)
def test_b_admission_failure_refuses_and_claims_no_count(failure, populated_queue):
    text = _render(
        {
            "pact view": json_result(populated_queue),
            "admission list": failure,
        }
    )
    admission = _section(text, "### Pending held admission envelopes")
    assert "Unavailable" in admission
    assert "refusing to report a count" in admission
    # The exact failed command is named.
    assert f"{CLI} admission list --deployment {DEPLOYMENT}" in admission
    assert "--status pending --held --limit" in admission
    assert "--json" in admission
    # No zero count, and no verified-empty claim for a source that did not
    # answer.
    assert "verified empty" not in admission.lower()
    assert "0 pending" not in admission
    assert "pending." not in admission.replace("No number of pending", "")


def test_b_partial_view_is_labelled_as_partial(populated_queue):
    text = _render(
        {
            "pact view": json_result(populated_queue),
            "admission list": result("", returncode=2, stderr="dsn unreachable"),
        }
    )
    assert "This is not the whole inbox" in text


# -- (c) PACT source fails, admission answers ------------------------------


def test_c_reachable_half_is_not_rendered_as_the_whole_inbox():
    text = _render(
        {
            "pact view": result("", returncode=1, stderr="ledger DSN refused"),
            "admission list": json_result(admission_payload([PENDING_ENVELOPE])),
        }
    )
    assert "This is not the whole inbox" in text
    pact = _section(text, "### PACT review items")
    assert "Unavailable" in pact
    assert "refusing to report a count" in pact
    assert "verified empty" not in pact.lower()
    # The admission half still renders -- but it is explicitly not the inbox.
    assert PENDING_ENVELOPE["admission_id"] in text


def test_c_failed_pact_source_names_its_exact_command():
    text = _render(
        {
            "pact view": result("", returncode=1, stderr="ledger DSN refused"),
            "admission list": json_result(admission_payload([PENDING_ENVELOPE])),
        }
    )
    pact = _section(text, "### PACT review items")
    assert f"{CLI} pact --root" in pact
    assert "view --format=chat-review-queue --json" in pact


# -- (d) ledger down but exit 0 -- the arm that matters --------------------


def test_d_fixture_is_a_clean_looking_failure(ledger_down_queue):
    """Guard: the captured fixture must LOOK healthy, or (d) proves nothing."""
    assert ledger_down_queue["ok"] is True
    packet = ledger_down_queue["chat_review_queue"]
    assert packet["warnings"], "the fixture lost its ledger warning"
    assert any(
        "decision history is unavailable" in str(w).lower()
        for w in packet["warnings"]
    )
    assert packet["rows"], "the fixture still carries a (smaller) row set"
    assert packet["answer"] == "Nothing urgent is blocking active work."
    assert json.dumps(packet)


def test_d_ledger_down_packet_classifies_as_unavailable(ledger_down_queue):
    outcome = classify_pact_result(
        CommandResult(
            argv=("okg", "pact", "view"),
            returncode=0,
            stdout=json.dumps(ledger_down_queue),
            stderr="",
        )
    )
    assert outcome.state == UNAVAILABLE
    assert "decision ledger is unavailable" in outcome.reason
    # Nothing from the warned packet is carried forward.
    assert outcome.rows is None
    assert outcome.shown is None
    assert outcome.total is None
    assert outcome.packet is None


def test_d_renders_no_answer_no_summary_lines_no_count(ledger_down_queue):
    text = _render(
        {
            "pact view": json_result(ledger_down_queue),
            "admission list": json_result(admission_payload([PENDING_ENVELOPE])),
        }
    )
    packet = ledger_down_queue["chat_review_queue"]
    assert packet["answer"] not in text
    assert "Nothing urgent" not in text
    for line in packet["summary_lines"]:
        assert line not in text
    assert f"{packet['total']} waiting" not in text
    assert f"{packet['shown']} of {packet['total']}" not in text
    # No row from the warned packet is rendered. Checked inside the PACT
    # section: one of the fixture's change ids is `mcp-http-auth`, which
    # the withheld-decisions prose legitimately names elsewhere.
    pact = _section(text, "### PACT review items")
    assert "- **P" not in pact, "a row from a warned packet was rendered"
    for row in packet["rows"]:
        assert row["change"] not in pact


def test_d_query_turn_renders_no_answer_and_no_summary_lines(ledger_down_queue):
    """The leak path that matters: a --query turn is answer-first.

    On the default turn the renderer never emits `answer` or
    `summary_lines` for ANY classification, so asserting their absence
    there proves nothing. A `--query` turn is the path where an operator's
    question would surface the packet's all-clear sentence, and it is the
    default for every real question. This is the arm that must hold.
    """
    pipe, _ = make_pipe(
        {
            "pact view": json_result(ledger_down_queue),
            "admission list": json_result(admission_payload([PENDING_ENVELOPE])),
        }
    )
    text = pipe.pipe(
        {"messages": [{"role": "user", "content": "what needs review?"}]},
        __user__=ADMIN_USER,
    )
    packet = ledger_down_queue["chat_review_queue"]
    assert packet["answer"] not in text
    assert "Nothing urgent" not in text
    assert packet["interpreted_as"] not in text
    for line in packet["summary_lines"]:
        assert line not in text
    assert "Read as:" not in text
    pact = _section(text, "### PACT review items")
    assert "- **P" not in pact
    assert "refusing to report a count" in pact


def test_d_refusal_names_the_source_and_the_proven_insufficient_signals(
    ledger_down_queue,
):
    text = _render(
        {
            "pact view": json_result(ledger_down_queue),
            "admission list": json_result(admission_payload([PENDING_ENVELOPE])),
        }
    )
    pact = _section(text, "### PACT review items")
    assert "decision ledger is unavailable" in pact
    assert "exited 0 with valid JSON" in pact
    assert f"{CLI} pact --root" in pact
    assert "This is not the whole inbox" in text


def test_d_any_warning_makes_the_source_unavailable():
    """Fail-closed: an unrecognised warning is still a completeness doubt."""
    outcome = classify_pact_result(
        CommandResult(
            argv=("okg", "pact", "view"),
            returncode=0,
            stdout=json.dumps(
                {
                    "ok": True,
                    "chat_review_queue": {
                        "rows": [],
                        "shown": 0,
                        "total": 0,
                        "warnings": ["something else went sideways"],
                        "answer": "All clear.",
                    },
                }
            ),
            stderr="",
        )
    )
    assert outcome.state == UNAVAILABLE
    assert outcome.total is None


def test_d_missing_warnings_array_is_unavailable_not_clean():
    """An absent signal is a distinct fail-closed state, never a pass."""
    outcome = classify_pact_result(
        CommandResult(
            argv=("okg", "pact", "view"),
            returncode=0,
            stdout=json.dumps(
                {"ok": True, "chat_review_queue": {"rows": [], "shown": 0, "total": 0}}
            ),
            stderr="",
        )
    )
    assert outcome.state == UNAVAILABLE


# -- the admission half refuses a missing count, like the PACT half -------
# The defect: `count = payload.get("count"); if not isinstance(count, int):
# count = len(rows)`. A payload with neither a count nor any rows then
# rendered "Admission: verified empty" as AUTHORITY -- a count nobody
# reported, manufactured from an empty list. The PACT half already refuses
# in the same situation (no shown/total counts -> UNAVAILABLE); this makes
# the two halves agree.


def _admission_result(payload: dict) -> CommandResult:
    return CommandResult(
        argv=("okg", "admission", "list"),
        returncode=0,
        stdout=json.dumps(payload),
        stderr="",
    )


@pytest.mark.parametrize(
    "payload",
    [
        {"admissions": []},
        {"admissions": [], "count": None},
        {"admissions": [], "count": "0"},
        {"admissions": [], "count": 0.0},
    ],
    ids=["absent", "null", "string", "float"],
)
def test_an_admission_payload_with_no_integer_count_is_unavailable(payload):
    outcome = classify_admission_result(
        _admission_result(payload), queried_at="2026-08-11T00:00:00+00:00"
    )
    assert outcome.state == UNAVAILABLE, (
        "a count nobody reported must not be manufactured from len(rows)"
    )
    assert "count" in outcome.reason
    # Nothing from an uncounted read may be rendered.
    assert outcome.rows is None
    assert outcome.shown is None
    assert outcome.total is None


def test_an_uncounted_admission_read_renders_a_refusal_not_verified_empty():
    """The rendered consequence, through the production renderer."""
    text = _render(
        {
            "pact view": json_result(EMPTY_PACKET),
            "admission list": json_result({"admissions": []}),
        },
        commit_sha=COMMIT_SHA,
    )
    admission = _section(text, "### Pending held admission envelopes")
    assert "verified empty" not in admission.lower()
    assert "refusing to report a count" in admission
    assert "This is not the whole inbox" in text
    # Control: the PACT half, whose payload IS well-formed, still renders
    # its verified-empty statement -- so the refusal above is about the
    # admission payload and not about a renderer that refused everything.
    assert "PACT queue: verified empty" in text


@pytest.mark.parametrize(
    "payload,state",
    [
        ({"admissions": [], "count": 0}, VERIFIED_EMPTY),
        ({"admissions": [dict(PENDING_ENVELOPE)], "count": 1}, POPULATED),
    ],
    ids=["counted-empty", "counted-populated"],
)
def test_a_counted_admission_read_still_classifies_normally(payload, state):
    """Non-vacuity control for the refusal above, in the SAME suite.

    A classifier that returned UNAVAILABLE for every admission payload
    would satisfy the refusal arms and prove nothing.
    """
    outcome = classify_admission_result(
        _admission_result(payload), queried_at="2026-08-11T00:00:00+00:00"
    )
    assert outcome.state == state
    assert outcome.total == payload["count"]


def test_healthy_packet_still_classifies_as_populated(populated_queue):
    """Non-vacuity for the classifier: the healthy capture must pass."""
    outcome = classify_pact_result(
        CommandResult(
            argv=("okg", "pact", "view"),
            returncode=0,
            stdout=json.dumps(populated_queue),
            stderr="",
        )
    )
    assert outcome.state == POPULATED
    assert outcome.total == populated_queue["chat_review_queue"]["total"]


# -- non-vacuity: the same assertions against the pre-fix rendering path ---


def prefix_classify_pact_result(result_: CommandResult) -> SourceOutcome:
    """The rendering path BEFORE this change: trust exit code + JSON only.

    This is the defect the requirement names. It is reproduced here so the
    refusal assertions above can be re-run against it and shown to fail --
    an assertion that passes against both paths would be proving nothing.
    """
    payload = json.loads(result_.stdout)
    packet = payload["chat_review_queue"]
    return SourceOutcome(
        source="pact",
        state=VERIFIED_EMPTY if packet["total"] == 0 else POPULATED,
        command=result_.display,
        rows=tuple(packet["rows"]),
        shown=packet["shown"],
        total=packet["total"],
        packet=dict(packet),
    )


def test_prefix_path_reports_the_ledger_down_packet_as_healthy(
    ledger_down_queue,
):
    outcome = prefix_classify_pact_result(
        CommandResult(
            argv=("okg", "pact", "view"),
            returncode=0,
            stdout=json.dumps(ledger_down_queue),
            stderr="",
        )
    )
    assert outcome.state == POPULATED, (
        "the pre-fix path must read the failure as healthy, or the refusal "
        "assertions prove nothing"
    )


@pytest.mark.parametrize("answer_first", [False, True], ids=["default", "query"])
def test_refusal_assertions_fail_against_the_prefix_path(
    ledger_down_queue, answer_first
):
    """Re-run the (d) assertions against the pre-fix renderer: they must fail.

    Run on BOTH turn kinds. The answer-first arm is the one that carries
    the `answer` and `summary_lines` conditions -- on the default turn the
    renderer emits neither for any classification, so those two
    conditions would be vacuously satisfied by the pre-fix path too.
    """
    outcome = prefix_classify_pact_result(
        CommandResult(
            argv=("okg", "pact", "view"),
            returncode=0,
            stdout=json.dumps(ledger_down_queue),
            stderr="",
        )
    )
    text = "\n".join(
        render_pact_section(
            outcome, commit_sha=COMMIT_SHA, answer_first=answer_first
        )
    )
    packet = ledger_down_queue["chat_review_queue"]

    conditions = [
        ("refusal present", "refusing to report a count" in text),
        ("no rows", all(row["change"] not in text for row in packet["rows"])),
    ]
    if answer_first:
        conditions += [
            ("no answer", packet["answer"] not in text),
            (
                "no summary lines",
                all(line not in text for line in packet["summary_lines"]),
            ),
        ]
    else:
        conditions.append(
            ("no count", f"{packet['shown']} of {packet['total']}" not in text)
        )

    failed = [label for label, holds in conditions if not holds]
    assert len(failed) == len(conditions), (
        "every refusal assertion must FAIL against the pre-fix path; these "
        f"held anyway and are therefore vacuous: "
        f"{[label for label, holds in conditions if holds]}"
    )


def test_prefix_path_leaks_the_all_clear_on_a_query_turn(ledger_down_queue):
    """Name the leak explicitly: the pre-fix path DOES print the all-clear."""
    outcome = prefix_classify_pact_result(
        CommandResult(
            argv=("okg", "pact", "view"),
            returncode=0,
            stdout=json.dumps(ledger_down_queue),
            stderr="",
        )
    )
    text = "\n".join(
        render_pact_section(outcome, commit_sha=COMMIT_SHA, answer_first=True)
    )
    packet = ledger_down_queue["chat_review_queue"]
    assert packet["answer"] in text
    assert "Nothing urgent is blocking active work." in text
    assert all(line in text for line in packet["summary_lines"])


def test_prefix_and_fixed_paths_disagree_on_the_healthy_capture_not_at_all(
    populated_queue,
):
    """Control: on a healthy packet the two paths agree, so the difference
    above is caused by the ledger failure and not by the rewrite."""
    args = CommandResult(
        argv=("okg", "pact", "view"),
        returncode=0,
        stdout=json.dumps(populated_queue),
        stderr="",
    )
    assert (
        prefix_classify_pact_result(args).state == classify_pact_result(args).state
    )
