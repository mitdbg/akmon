"""Shared harness for the review-inbox Pipe tests.

The Pipe reaches the substrate only through `okg` subprocess argv lists,
so every test here drives it with a recording fake runner. Two of the
three PACT-queue payloads are REAL CLI output captured in this worktree
with `okg pact view --format=chat-review-queue --json` -- one healthy, one
with the decision ledger deliberately unreachable. The ledger-down capture
is the whole point of the refusal arms: it exits 0, reports `ok: true`,
parses as valid JSON, carries a silently smaller row set, and still says
"Nothing urgent is blocking active work."
"""

from __future__ import annotations

import json
from collections.abc import Sequence
from pathlib import Path
from typing import Any

import pytest

from okg.substrate.admission import QUARANTINE_BRANCH
from okg.substrate.chat.review_queue_pipe import CommandResult, Pipe

FIXTURES = Path(__file__).parent / "fixtures"

# The literal the rendered inbox must NEVER contain. It is READ from the
# substrate's own constant rather than typed here, so a rename in the
# substrate cannot quietly leave this guard checking a string that nothing
# produces any more; `test_the_forbidden_branch_literal_is_pinned` asserts
# it is still the literal the PACT names.
#
# `--mode review` does map to this branch -- for SOURCE INGEST
# (admission.py:74-78). It does NOT for the MCP record/propose writes this
# inbox exists to review: those drop any caller-supplied
# target_branch_name (mcp/server.py:835) and leave it NULL
# (mcp/server.py:2727-2731). Naming it in the inbox would assert the
# emptiness of a referent no such envelope occupies.
FORBIDDEN_BRANCH_LITERAL = QUARANTINE_BRANCH

CLI = "/opt/okg/bin/okg"
REPO_ROOT = "/srv/okg-repo"
DEPLOYMENT = "okg-workspace"
ADMIN_GROUP = "okg-workspace-admins"

# MEASURED vendor shape at Open WebUI v0.11.0. The `__user__` dict a Pipe
# receives carries `role` and `id` but NO `groups` key -- for an instance
# admin AND for a confirmed member of a real group. Evidence:
# pact/changes/openwebui-chat-frontend/evidence/pipe-user-context-probe.md
# (image ghcr.io/open-webui/open-webui@sha256:72c0ba64…, measured
# 2026-08-04; the member arm verified `member_count: 1` before probing,
# so the absence is not an artefact of an empty group).
#
# These fixtures deliberately omit `groups`. A gate that requires it
# refuses every caller including the admin, which is the defect the probe
# caught in the first version of this Pipe.
ADMIN_USER = {
    "id": "u-1",
    "name": "operator",
    "email": "operator@example.invalid",
    "role": "admin",
}
GROUP_MEMBER_USER = {
    "id": "u-3",
    "name": "member",
    "email": "member@example.invalid",
    "role": "user",
}
NON_ADMIN_USER = {
    "id": "u-2",
    "name": "reader",
    "email": "reader@example.invalid",
    "role": "user",
}
# A hypothetical future version that does supply `groups`; the fast path
# must keep working if one ever does.
FUTURE_GROUPS_USER = {
    "id": "u-4",
    "name": "future",
    "role": "user",
    "groups": [{"id": "g-1", "name": ADMIN_GROUP}],
}

# The EXACT key set Open WebUI v0.11.0 hands a Pipe, transcribed from the
# probe transcript (evidence/pipe-user-context-probe.md). 22 keys,
# IDENTICAL in the admin arm and the confirmed-group-member arm. The
# absence of `groups` is structural: every construction site builds the
# dict as `user.model_dump()` from a `UserModel`
# (utils/middleware.py:2404, :3508, :3769; utils/plugin.py:121-122), and
# `UserModel` (models/users.py:86) declares no such field. A `groups`
# field exists on `UserInfoResponse` (models/users.py:217) -- a different
# model that is never the source of `__user__`.
MEASURED_PIPE_USER_KEYS = (
    "bio",
    "created_at",
    "date_of_birth",
    "email",
    "gender",
    "id",
    "info",
    "last_active_at",
    "name",
    "oauth",
    "presence_state",
    "profile_banner_image_url",
    "profile_image_url",
    "role",
    "scim",
    "settings",
    "status_emoji",
    "status_expires_at",
    "status_message",
    "timezone",
    "updated_at",
    "username",
)


def measured_pipe_user(*, role: str, user_id: str) -> dict[str, Any]:
    """A `__user__` dict with exactly the measured v0.11.0 key set."""
    user: dict[str, Any] = dict.fromkeys(MEASURED_PIPE_USER_KEYS)
    user["id"] = user_id
    user["role"] = role
    user["name"] = "measured"
    user["email"] = f"{user_id}@example.invalid"
    return user


def membership_resolver(state: str, reason: str = "stubbed"):
    """A stubbed server-side membership lookup with a recorded call log."""
    from okg.substrate.chat.review_queue_pipe import UNDETERMINED

    calls: list[tuple[str, str]] = []

    def resolve(user_id: str, group: str, **_: Any) -> tuple[str, str]:
        calls.append((user_id, group))
        return state, reason

    resolve.calls = calls  # type: ignore[attr-defined]
    resolve.undetermined = UNDETERMINED  # type: ignore[attr-defined]
    return resolve


def load_fixture(name: str) -> dict[str, Any]:
    return json.loads((FIXTURES / f"{name}.json").read_text())


class FakeRunner:
    """Records every argv and replays a scripted result per okg verb."""

    def __init__(self, responses: dict[str, CommandResult]) -> None:
        self.responses = responses
        self.calls: list[tuple[str, ...]] = []

    def key(self, argv: Sequence[str]) -> str:
        tokens = [token for token in argv[1:] if not token.startswith("-")]
        tokens = [token for token in tokens if token not in {REPO_ROOT, DEPLOYMENT}]
        return " ".join(tokens[:2])

    def __call__(self, argv: Sequence[str]) -> CommandResult:
        self.calls.append(tuple(argv))
        key = self.key(argv)
        if key not in self.responses:
            raise AssertionError(f"unscripted okg command: {' '.join(argv)}")
        scripted = self.responses[key]
        return CommandResult(
            argv=tuple(argv),
            returncode=scripted.returncode,
            stdout=scripted.stdout,
            stderr=scripted.stderr,
        )


def result(
    stdout: str = "", *, returncode: int = 0, stderr: str = ""
) -> CommandResult:
    return CommandResult(
        argv=(), returncode=returncode, stdout=stdout, stderr=stderr
    )


def json_result(payload: Any, *, returncode: int = 0) -> CommandResult:
    return result(json.dumps(payload), returncode=returncode)


def admission_payload(rows: list[dict[str, Any]]) -> dict[str, Any]:
    return {"admissions": rows, "count": len(rows)}


PENDING_ENVELOPE = {
    "admission_id": "3f2b1c88-0000-4000-8000-00000000abcd",
    "deployment_name": DEPLOYMENT,
    "status": "pending",
    "admission_mode": "review",
    "producer_kind": "agent",
    "producer_id": "chat-record",
    "intent": "review_proposal",
    "source": "chat",
    "created_at": "2026-08-03T12:00:00+00:00",
}


def make_pipe(
    responses: dict[str, CommandResult],
    *,
    okg_cli: str = CLI,
    repo_root: str = REPO_ROOT,
    deployment: str = DEPLOYMENT,
    admin_group: str = ADMIN_GROUP,
    host_ok: bool = True,
    commit_sha: str = "c0ffee1234567890c0ffee1234567890c0ffee12",
    sha_error: str = "",
    membership: Any = None,
) -> tuple[Pipe, FakeRunner]:
    """A Pipe wired to a recording fake runner.

    `host_ok` controls the measured filesystem probes only: the Valve
    values stay exactly as passed, so an unset-Valve test still exercises
    the unset path (an unset Valve is unmet regardless of the probes).
    """
    pipe = Pipe()
    pipe.valves.okg_cli = okg_cli
    pipe.valves.repo_root = repo_root
    pipe.valves.deployment = deployment
    pipe.valves.admin_group = admin_group
    runner = FakeRunner(responses)
    pipe._runner = runner
    pipe._host_probe = _host_probe(host_ok)
    pipe._sha_resolver = lambda _root: (commit_sha, sha_error)
    if membership is None:
        # Default: the lookup cannot answer. Any test whose caller is
        # authorized must therefore be authorized by a REAL signal (an
        # admin role, an explicit groups entry, or an explicitly stubbed
        # MEMBER result) rather than by a permissive default.
        from okg.substrate.chat.review_queue_pipe import UNDETERMINED

        membership = membership_resolver(UNDETERMINED, "no lookup configured")
    pipe._membership_resolver = membership
    return pipe, runner


def _host_probe(ok: bool):
    """Pin the measured host probes so hermetic tests need no okg on disk."""
    from okg.substrate.chat import review_queue_pipe as module

    original = module.check_host_preconditions

    def patched(**kwargs: Any) -> list[module.Precondition]:
        return original(
            **kwargs,
            executable_probe=lambda _path: ok,
            directory_probe=lambda _path: ok,
        )

    return patched


@pytest.fixture
def populated_queue() -> dict[str, Any]:
    return load_fixture("chat_review_queue_populated")


@pytest.fixture
def ledger_down_queue() -> dict[str, Any]:
    return load_fixture("chat_review_queue_ledger_down")


@pytest.fixture
def query_queue() -> dict[str, Any]:
    """A real `--query "what's blocking the agents' work?"` capture."""
    return load_fixture("chat_review_queue_query")
