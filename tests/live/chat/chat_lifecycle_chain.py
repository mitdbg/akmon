"""The pure, import-safe core of the chat operator lifecycle proof.

Standard library only, no import side effects, no ``sys.path`` mutation. The
live driver (``drive_chat_lifecycle.py``) and the hermetic contract test
(``tests/substrate/chat/test_chat_lifecycle_driver.py``) both import it, so a
defect in the driver cannot become a pytest collection error.

The proof walks the operator lifecycle of one chat site against the product's
own commands, in order: ``chat-instance up``, ``chat setup``, ``chat plan``,
``chat sync`` and ``chat readback``; then an admin-API drift that readback must
flag and sync must repair; then a ``down``/``up`` restart that must come back
clean with its chats kept; then a graph-credential rotation that readback must
flag before a sync repairs it.

Each step settles to one outcome. A step can be declared an EXPECTED FAILURE,
keyed to the change that will fix it -- the strict-xfail rule this repository
uses for pytest (``xfail(strict=True, reason=...)``), applied to a driver
step: the failure is recorded as ``xfail`` and does not fail the run, and a
PASS of that step fails the run as ``xpass_strict`` until the marker is
removed, so a fix cannot land without the proof being updated to demand it.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from typing import Any

#: Outcomes a settled step can have.
PASS = "pass"
FAIL = "fail"
XFAIL = "xfail"
XPASS_STRICT = "xpass_strict"
NOT_RUN = "not_run"

#: Outcomes that leave the run green.
GREEN = (PASS, XFAIL)


@dataclass(frozen=True)
class ExpectedFailure:
    """Why a step is expected to fail, which change removes the marker, and
    the exact shape of the known failure.

    ``signature`` pins the KIND of failure, as ``xfail(strict=True,
    raises=KnownBug)`` does for a pytest test: a failed step counts as the
    expected failure only when every key of ``signature`` equals the value
    the step recorded in its ``data``. Any other failure -- a crash, output
    that did not parse, another exit code -- is a plain ``fail``.
    """

    pact: str
    reason: str
    signature: tuple[tuple[str, Any], ...]

    def matches(self, data: dict[str, Any]) -> bool:
        return bool(self.signature) and all(
            key in data and data[key] == value for key, value in self.signature
        )


#: The rotation readback step: after the graph bearer rotates, readback must
#: flag the credential the chat site still stores. On dev at 5868ae0778 it
#: reports clean (exit 0, no deviations) because its verify probe sends the
#: OPERATOR's token rather than the stored one (#2287 comment of 2026-09-30,
#: ``_verify_probe`` in src/okg/substrate/chat/plan_apply.py). The fix is lane
#: A's PACT ``chat-readback-stale-credential``; when it lands this step passes,
#: the run fails as ``xpass_strict``, and this entry must be deleted.
ROTATION_READBACK_STEP = "rotation_readback_flags_stale_credential"
# Empty: the one entry (the rotation readback, PACT
# chat-readback-stale-credential) was removed when that fix landed in the same
# pull request, so a clean readback after a rotation is now a plain failure.
EXPECTED_FAILURES: dict[str, ExpectedFailure] = {}

#: The drift the proof applies through the vendor admin API, and the key
#: readback names for it. Community sharing is one of the features sync turns
#: off, so turning it on is a real operator-visible drift.
DRIFT_ADMIN_CONFIG_KEY = "ENABLE_COMMUNITY_SHARING"
DRIFT_READBACK_KEY = "config:ui.enable_community_sharing"
READBACK_DEVIATION_CODE = "chat_readback_deviation"


def settle(
    name: str,
    *,
    passed: bool,
    detail: str,
    data: dict[str, Any] | None = None,
    expected_failures: dict[str, ExpectedFailure] | None = None,
) -> dict[str, Any]:
    """One step's record, with its outcome decided by the expected-failure rule."""
    marks = EXPECTED_FAILURES if expected_failures is None else expected_failures
    mark = marks.get(name)
    if mark is None:
        outcome = PASS if passed else FAIL
    elif passed:
        outcome = XPASS_STRICT
    else:
        outcome = XFAIL if mark.matches(data or {}) else FAIL
    record: dict[str, Any] = {
        "step": name,
        "outcome": outcome,
        "passed": passed,
        "detail": detail,
        "data": data or {},
    }
    if mark is not None:
        record["expected_failure"] = {"pact": mark.pact, "reason": mark.reason}
        if outcome == XPASS_STRICT:
            record["detail"] = (
                f"{detail} -- UNEXPECTED PASS: the step is marked as an expected "
                f"failure until {mark.pact} lands; remove its entry from "
                "EXPECTED_FAILURES in tests/live/chat/chat_lifecycle_chain.py"
            )
        elif outcome == FAIL:
            record["detail"] = (
                f"{detail} -- failed, but NOT as the known bug {mark.pact} names "
                f"(expected {dict(mark.signature)!r}); this is a different failure"
            )
    return record


def not_run(name: str, *, because: str) -> dict[str, Any]:
    """A step the run never reached, recorded rather than left out."""
    return {
        "step": name,
        "outcome": NOT_RUN,
        "passed": False,
        "detail": f"not run: {because}",
        "data": {},
    }


def lifecycle_verdict(
    steps: list[dict[str, Any]], *, planned: list[str]
) -> dict[str, Any]:
    """The run verdict: green only when every PLANNED step settled green.

    A planned step with no record, or a record naming a step that was not
    planned, fails the verdict: a missing step is not a passing one.
    """
    seen = [step["step"] for step in steps]
    missing = [name for name in planned if name not in seen]
    unplanned = [name for name in seen if name not in planned]
    duplicated = sorted({name for name in seen if seen.count(name) > 1})
    counts: dict[str, int] = {}
    for step in steps:
        counts[step["outcome"]] = counts.get(step["outcome"], 0) + 1
    ok = (
        not missing
        and not unplanned
        and not duplicated
        and all(step["outcome"] in GREEN for step in steps)
    )
    return {
        "ok": ok,
        "outcomes": counts,
        "missing_steps": missing,
        "unplanned_steps": unplanned,
        "duplicated_steps": duplicated,
        "failed_steps": [s["step"] for s in steps if s["outcome"] not in GREEN],
        "expected_failures": [s["step"] for s in steps if s["outcome"] == XFAIL],
    }


def command_json(stdout: str) -> dict[str, Any]:
    """The JSON envelope an ``okg ... --json`` command printed, or ``{}``.

    An unparseable stdout is returned as ``{}`` and the caller's exit-code
    check decides; the raw text is kept in the step artifacts either way.
    """
    text = (stdout or "").strip()
    if not text:
        return {}
    try:
        payload = json.loads(text)
    except json.JSONDecodeError:
        return {}
    return payload if isinstance(payload, dict) else {}


def readback_clean(exit_code: int, payload: dict[str, Any]) -> tuple[bool, str]:
    """Readback is clean when it exits 0, says ok, and lists no deviation."""
    deviations = payload.get("deviations")
    if exit_code != 0:
        return False, f"readback exited {exit_code} ({payload.get('code')})"
    if payload.get("ok") is not True:
        return False, f"readback reported ok={payload.get('ok')!r}"
    if not isinstance(deviations, list):
        return False, "readback reported no deviation list"
    if deviations:
        keys = [d.get("key") for d in deviations if isinstance(d, dict)]
        return False, f"readback listed {len(deviations)} deviation(s): {keys}"
    return True, "readback clean: exit 0, ok, 0 deviations"


def readback_flags(
    exit_code: int, payload: dict[str, Any], *, key: str | None = None
) -> tuple[bool, str]:
    """Readback flags a deviation: nonzero exit, not ok, and at least one
    deviation (naming ``key`` when given) under ``chat_readback_deviation``.

    For the rotation step no key is required: the fix may name the stale
    credential under a key this proof cannot know in advance, and the claim
    is only that readback stops reporting clean.
    """
    deviations = payload.get("deviations")
    if exit_code == 0 or payload.get("ok") is not False:
        return False, (
            f"readback reported clean (exit {exit_code}, ok={payload.get('ok')!r}, "
            f"deviations={deviations!r:.200})"
        )
    if payload.get("code") != READBACK_DEVIATION_CODE:
        return False, (
            f"readback failed with {payload.get('code')!r}, not "
            f"{READBACK_DEVIATION_CODE}"
        )
    if not isinstance(deviations, list) or not deviations:
        return False, "readback failed but listed no deviation"
    keys = [d.get("key") for d in deviations if isinstance(d, dict)]
    if key is not None and key not in keys:
        return False, f"readback flagged {keys}, not {key}"
    return True, f"readback flagged {keys} under {READBACK_DEVIATION_CODE}"


_CHAT_BLOCK = re.compile(r"^chat:\s*$")
_MCP_LINE = re.compile(r"^  mcp:\s*$")
_TOP_LEVEL = re.compile(r"^\S")


def add_mcp_auth_token_env(manifest: str, variable: str) -> str:
    """Declare ``chat.mcp.auth_token_env`` in a manifest ``okg chat setup``
    wrote, by inserting one line under ``chat:`` / ``  mcp:``.

    ``okg chat setup --configure-only`` has no flag for the graph bearer, so
    the proof adds the one key as text rather than re-serialising the whole
    manifest (which would reorder and restyle every other block). It refuses
    rather than guessing when the layout is not the two-space one setup
    writes, or when the key is already there.
    """
    if not re.fullmatch(r"[A-Z][A-Z0-9_]{2,63}", variable):
        raise ValueError(f"{variable!r} is not an environment variable name")
    lines = manifest.splitlines(keepends=True)
    in_chat = False
    for index, line in enumerate(lines):
        if _CHAT_BLOCK.match(line):
            in_chat = True
            continue
        if in_chat and _TOP_LEVEL.match(line):
            break
        if in_chat and _MCP_LINE.match(line):
            block_end = index + 1
            while block_end < len(lines) and lines[block_end].startswith("    "):
                if lines[block_end].startswith("    auth_token_env:"):
                    raise ValueError("chat.mcp.auth_token_env is already declared")
                block_end += 1
            lines.insert(index + 1, f"    auth_token_env: {variable}\n")
            return "".join(lines)
    raise ValueError("no two-space chat: / mcp: block to add auth_token_env to")
