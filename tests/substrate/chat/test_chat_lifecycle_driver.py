"""Hermetic contracts for the chat operator lifecycle proof.

These run in the ordinary fast lane. The live driver
(``tests/live/chat/drive_chat_lifecycle.py``) is never imported: it needs a
container runtime, a published deployment and an app database. What is
imported is its pure core, ``chat_lifecycle_chain``, plus the driver source
TEXT for the static checks at the bottom.
"""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

from tests.live.chat.chat_lifecycle_chain import (
    DRIFT_READBACK_KEY,
    EXPECTED_FAILURES,
    FAIL,
    NOT_RUN,
    PASS,
    READBACK_DEVIATION_CODE,
    ROTATION_READBACK_STEP,
    XFAIL,
    XPASS_STRICT,
    ExpectedFailure,
    add_mcp_auth_token_env,
    command_json,
    lifecycle_verdict,
    not_run,
    readback_clean,
    readback_flags,
    settle,
)
from tests.live.chat.grounded_api_chain import Refusal
from tests.live.chat.real_graph_model_fixture import offered_search_tool

REPO_ROOT = Path(__file__).resolve().parents[3]
DRIVER = REPO_ROOT / "tests" / "live" / "chat" / "drive_chat_lifecycle.py"

MARKS = {
    "flaky_step": ExpectedFailure(
        pact="some-fix", reason="known bug", signature=(("exit", 0), ("ok", True))
    )
}
#: The data a step records when it fails exactly as the known bug does.
KNOWN = {"exit": 0, "ok": True}

#: What the rotation readback records today, on dev: the bug itself.
ROTATION_BUG = {"exit": 0, "ok": True, "code": None, "deviations": []}


# --------------------------------------------------------------------------
# the expected-failure rule: strict, keyed to the fixing change
# --------------------------------------------------------------------------


def test_an_unmarked_step_passes_or_fails_on_its_result() -> None:
    assert (
        settle("a", passed=True, detail="d", expected_failures=MARKS)["outcome"] == PASS
    )
    assert (
        settle("a", passed=False, detail="d", expected_failures=MARKS)["outcome"]
        == FAIL
    )


def test_a_marked_step_that_fails_is_an_expected_failure_naming_its_fix() -> None:
    step = settle(
        "flaky_step", passed=False, detail="d", data=KNOWN, expected_failures=MARKS
    )
    assert step["outcome"] == XFAIL
    assert step["expected_failure"] == {"pact": "some-fix", "reason": "known bug"}


@pytest.mark.parametrize(
    "data",
    [
        {},  # a crash or unparseable output records nothing
        {"exit": 2, "ok": True},  # another exit code
        {"exit": 0, "ok": None},  # output that did not parse
        {"exit": -1},  # a timeout
    ],
)
def test_a_marked_step_failing_some_other_way_is_a_plain_failure(data) -> None:
    step = settle(
        "flaky_step", passed=False, detail="d", data=data, expected_failures=MARKS
    )
    assert step["outcome"] == FAIL
    assert "NOT as the known bug" in step["detail"]
    assert lifecycle_verdict([step], planned=["flaky_step"])["ok"] is False


def test_a_mark_with_an_empty_signature_never_counts_as_expected() -> None:
    marks = {"x": ExpectedFailure(pact="p", reason="r", signature=())}
    assert (
        settle("x", passed=False, detail="d", data={}, expected_failures=marks)[
            "outcome"
        ]
        == FAIL
    )


def test_a_marked_step_that_passes_fails_the_run_and_says_what_to_remove() -> None:
    step = settle("flaky_step", passed=True, detail="d", expected_failures=MARKS)
    assert step["outcome"] == XPASS_STRICT
    assert "UNEXPECTED PASS" in step["detail"] and "some-fix" in step["detail"]
    assert "EXPECTED_FAILURES" in step["detail"]
    verdict = lifecycle_verdict([step], planned=["flaky_step"])
    assert verdict["ok"] is False
    assert verdict["failed_steps"] == ["flaky_step"]


def test_no_step_is_an_expected_failure_once_the_readback_fix_landed() -> None:
    """chat-readback-stale-credential landed with this change, so the rotation
    readback must flag the stale credential outright (a clean readback fails)."""
    assert EXPECTED_FAILURES == {}
    step = settle(ROTATION_READBACK_STEP, passed=False, detail="d", data=ROTATION_BUG)
    assert step["outcome"] == FAIL


_ROTATION_MARK = {
    ROTATION_READBACK_STEP: ExpectedFailure(
        pact="chat-readback-stale-credential",
        reason="the known readback bug",
        signature=(("exit", 0), ("ok", True), ("deviations", [])),
    )
}


def test_the_rotation_xfail_accepts_only_the_known_readback_bug() -> None:
    """Exit 0, ok, no deviations is the bug; a crash, unparseable output
    ({}) or any other exit code fails the step instead."""
    bug = settle(
        ROTATION_READBACK_STEP,
        passed=False,
        detail="d",
        data=ROTATION_BUG,
        expected_failures=_ROTATION_MARK,
    )
    assert bug["outcome"] == XFAIL
    for data in (
        {},
        {"exit": -1, "ok": None, "deviations": None},
        {"exit": 1, "ok": None, "deviations": None},
        {"exit": 0, "ok": None, "deviations": None},
        {"exit": 0, "ok": True, "deviations": [{"key": "x"}]},
    ):
        step = settle(
            ROTATION_READBACK_STEP,
            passed=False,
            detail="d",
            data=data,
            expected_failures=_ROTATION_MARK,
        )
        assert step["outcome"] == FAIL, data


def test_an_expected_failure_leaves_the_verdict_green() -> None:
    steps = [
        settle("a", passed=True, detail="d", expected_failures=MARKS),
        settle(
            "flaky_step", passed=False, detail="d", data=KNOWN, expected_failures=MARKS
        ),
    ]
    verdict = lifecycle_verdict(steps, planned=["a", "flaky_step"])
    assert verdict["ok"] is True
    assert verdict["expected_failures"] == ["flaky_step"]
    assert verdict["outcomes"] == {PASS: 1, XFAIL: 1}


@pytest.mark.parametrize(
    ("steps", "planned", "field"),
    [
        ([], ["a"], "missing_steps"),
        (["a", "b"], ["a"], "unplanned_steps"),
        (["a", "a"], ["a"], "duplicated_steps"),
    ],
)
def test_a_missing_extra_or_repeated_step_fails_the_verdict(
    steps, planned, field
) -> None:
    records = [
        settle(name, passed=True, detail="d", expected_failures={}) for name in steps
    ]
    verdict = lifecycle_verdict(records, planned=planned)
    assert verdict["ok"] is False
    assert verdict[field]


def test_a_step_never_reached_is_recorded_and_fails_the_verdict() -> None:
    steps = [
        settle("a", passed=True, detail="d", expected_failures={}),
        not_run("b", because="a stopped"),
    ]
    verdict = lifecycle_verdict(steps, planned=["a", "b"])
    assert verdict["ok"] is False
    assert steps[1]["outcome"] == NOT_RUN and "a stopped" in steps[1]["detail"]


# --------------------------------------------------------------------------
# readback judgement, on the shape okg chat readback --json prints
# --------------------------------------------------------------------------


def clean_payload() -> dict:
    return {"code": None, "ok": True, "deviations": [], "openwebui_version": "0.11.4"}


def drift_payload() -> dict:
    return {
        "code": READBACK_DEVIATION_CODE,
        "ok": False,
        "deviations": [
            {
                "key": DRIFT_READBACK_KEY,
                "expected": False,
                "observed": True,
                "reason": "readback_mismatch",
                "surface": "instance_ui",
            }
        ],
    }


def test_a_clean_readback_is_clean() -> None:
    assert readback_clean(0, clean_payload())[0] is True


@pytest.mark.parametrize(
    ("code", "change"),
    [
        (1, {}),
        (0, {"ok": False}),
        (0, {"deviations": None}),
        (0, {"deviations": [{"key": "x"}]}),
    ],
)
def test_anything_short_of_clean_is_not_clean(code, change) -> None:
    payload = {**clean_payload(), **change}
    assert readback_clean(code, payload)[0] is False


def test_a_flagged_drift_is_flagged_under_its_key() -> None:
    judged, why = readback_flags(1, drift_payload(), key=DRIFT_READBACK_KEY)
    assert judged is True and DRIFT_READBACK_KEY in why


def test_the_rotation_bug_reads_as_not_flagged() -> None:
    """What dev does today after a rotation: exit 0, ok, no deviation."""
    judged, why = readback_flags(0, clean_payload())
    assert judged is False and "reported clean" in why


@pytest.mark.parametrize(
    ("code", "change", "key", "named"),
    [
        (1, {"code": "chat_plan_drift"}, None, "not chat_readback_deviation"),
        (1, {"deviations": []}, None, "listed no deviation"),
        (1, {}, "config:other", "not config:other"),
        (0, {}, None, "reported clean"),
    ],
)
def test_a_readback_that_fails_for_another_reason_is_not_a_flag(
    code, change, key, named
) -> None:
    payload = {**drift_payload(), **change}
    judged, why = readback_flags(code, payload, key=key)
    assert judged is False and named in why


def test_command_json_reads_the_envelope_or_nothing() -> None:
    assert command_json('{"ok": true}\n') == {"ok": True}
    assert command_json("") == {}
    assert command_json("not json") == {}
    assert command_json("[1, 2]") == {}


# --------------------------------------------------------------------------
# declaring the graph bearer in the manifest setup wrote
# --------------------------------------------------------------------------

MANIFEST = """\
deployment:
  name: demo
mcp:
  port: 9999
chat:
  enabled: true
  mcp:
    port: 5431
    tools:
    - search
  provider:
    base_url: http://host.docker.internal:1/v1
release:
  channel: dev
"""


def test_the_bearer_is_declared_under_chat_mcp_and_nowhere_else() -> None:
    edited = add_mcp_auth_token_env(MANIFEST, "OKG_LIFECYCLE_MCP_TOKEN")
    assert edited.count("auth_token_env") == 1
    assert (
        "  mcp:\n    auth_token_env: OKG_LIFECYCLE_MCP_TOKEN\n    port: 5431\n"
        in edited
    )
    assert (
        edited.replace("    auth_token_env: OKG_LIFECYCLE_MCP_TOKEN\n", "") == MANIFEST
    )


def test_a_manifest_already_declaring_the_bearer_refuses() -> None:
    once = add_mcp_auth_token_env(MANIFEST, "OKG_LIFECYCLE_MCP_TOKEN")
    with pytest.raises(ValueError, match="already declared"):
        add_mcp_auth_token_env(once, "OKG_OTHER_TOKEN")


@pytest.mark.parametrize(
    "manifest",
    [
        "mcp:\n  port: 1\n",
        "chat:\n  enabled: true\nmcp:\n  port: 1\n",
        "chat:\n    mcp:\n      port: 1\n",
    ],
)
def test_a_manifest_without_the_setup_layout_refuses(manifest) -> None:
    with pytest.raises(ValueError, match="no two-space chat"):
        add_mcp_auth_token_env(manifest, "OKG_LIFECYCLE_MCP_TOKEN")


@pytest.mark.parametrize("variable", ["lower_case", "OK", "A-B", "TOKEN VALUE"])
def test_only_a_variable_name_is_ever_written(variable) -> None:
    with pytest.raises(ValueError, match="not an environment variable name"):
        add_mcp_auth_token_env(MANIFEST, variable)


# --------------------------------------------------------------------------
# the fixture model calls the search operator even when a preset offers seven
# --------------------------------------------------------------------------


def tool(name: str) -> dict:
    return {"type": "function", "function": {"name": name, "parameters": {}}}


def test_the_fixture_picks_the_search_operator_out_of_a_preset() -> None:
    offered = [
        tool(f"okg-demo_{name}") for name in ("inspect", "expand", "search", "query")
    ]
    assert offered_search_tool(offered) == "okg-demo_search"


def test_the_fixture_refuses_when_no_search_tool_is_offered() -> None:
    """A near miss is not search, and the first tool is never a fallback."""
    offered = [
        tool("okg-demo_research"),
        tool("okg-demo_searches"),
        tool("okg-demo_inspect"),
    ]
    with pytest.raises(Refusal, match="no search tool offered") as caught:
        offered_search_tool(offered)
    assert "okg-demo_inspect" in str(caught.value)


def test_the_fixture_falls_back_to_the_bare_operator_with_nothing_offered() -> None:
    assert offered_search_tool([]) == "search"


# --------------------------------------------------------------------------
# the driver, read as text
# --------------------------------------------------------------------------


def driver_tree() -> ast.Module:
    return ast.parse(DRIVER.read_text("utf-8"))


def _planned_steps() -> list[str]:
    for node in ast.walk(driver_tree()):
        if isinstance(node, ast.Assign) and any(
            isinstance(t, ast.Name) and t.id == "PLANNED_STEPS" for t in node.targets
        ):
            names = []
            for element in node.value.elts:
                if isinstance(element, ast.Constant):
                    names.append(element.value)
                elif (
                    isinstance(element, ast.Name)
                    and element.id == "ROTATION_READBACK_STEP"
                ):
                    names.append(ROTATION_READBACK_STEP)
            return names
    raise AssertionError("PLANNED_STEPS not found")


def test_the_planned_steps_cover_the_whole_lifecycle_in_order() -> None:
    steps = _planned_steps()
    for required in (
        "instance_up",
        "setup",
        "plan",
        "sync",
        "readback_clean",
        "drift_readback_flags",
        "drift_sync",
        "drift_readback_clean",
        "restart_down",
        "restart_up",
        "restart_readback_clean",
        "rotation_apply",
        ROTATION_READBACK_STEP,
        "rotation_sync",
        "rotation_readback_clean",
        "rotation_turn",
    ):
        assert required in steps, required
    assert len(steps) == len(set(steps))
    assert steps.index("drift_readback_flags") < steps.index("drift_sync")
    assert steps.index(ROTATION_READBACK_STEP) < steps.index("rotation_sync")
    assert set(EXPECTED_FAILURES) <= set(steps)


def test_the_driver_writes_artifacts_only_through_the_scrubber() -> None:
    """One direct write: the manifest edit. Everything else is scrubbed."""
    direct = [
        node
        for node in ast.walk(driver_tree())
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == "write_text"
    ]
    assert len(direct) == 1
    source = DRIVER.read_text("utf-8")
    assert "assert_no_secret_shapes(out)" in source


def test_the_driver_never_prints_a_minted_value_or_the_environment() -> None:
    """``print`` sees a step record's scrubbed detail, a count, or verdict
    fields; never ``self.env``, a token, or a password."""
    for node in ast.walk(driver_tree()):
        if (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Name)
            and node.func.id == "print"
        ):
            text = ast.unparse(node)
            for forbidden in ("env", "token", "password", "secret", "minted"):
                assert forbidden not in text.lower(), text


def test_the_network_mode_is_always_chosen_never_auto() -> None:
    source = DRIVER.read_text("utf-8")
    assert 'choices=("gateway", "pasta", "host")' in source
    assert '"auto"' not in source
