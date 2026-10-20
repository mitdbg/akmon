"""OKG Review Inbox — an Open WebUI Pipe over three typed review sources.

title: OKG Review Inbox
author: OKG
version: 0.1.0
required_open_webui_version: 0.11.0
license: MIT

A *Pipe* registers as a model in the Open WebUI picker and answers the
request itself: no LLM is invoked, and the returned string is the message
the operator sees. This one answers exactly one question -- "what is
waiting on me" -- over the pending kinds an OKG deployment has:

1. PACT review items, read from
   `okg pact --root <root> view --format=chat-review-queue --json`
2. Pending held admission envelopes -- review and quarantine mode, the
   runs that wait for `okg admission decide` (issue #1752) -- read from
   `okg admission list --deployment <d> --status pending --held --json`.
   The CLI resolves `--held` from `admission.HELD_ADMISSION_MODES`, so this
   module never spells a mode that shares its name with a branch.
3. External-action review proposals, read through the authenticated
   `okg external-action` HTTP client when its API URL is configured.

Workflow escalations are a third pending kind and are deliberately NOT
covered (design.md section 7 Plane 3); the rendered coverage statement
says so rather than leaving a reader to infer it.

Four contracts this module exists to keep
-----------------------------------------

**The pending set is identified by DEPLOYMENT and MODE, and by nothing
else.** There is no graph branch named, configured, asserted, filtered
on, or rendered anywhere in this module, and none may be reintroduced.
The writes this inbox exists to review arrive through MCP `record` /
`propose`, which DROP any caller-supplied `target_branch_name`
(`mcp/server.py:835`) and leave it unset -- i.e. NULL -- on a default
server (`mcp/server.py:2727-2731`). The mode-to-branch map
(`graph_branch_for_admission_mode`, `admission.py:74-78`) applies to
SOURCE INGEST only: every caller of it is ingest-side. And `okg
admission list` exposes no `--branch` flag at all
(`cli/parser_groups.py:290-312`). A branch predicate here would
therefore exclude exactly the envelopes this surface exists to show, and
a branch-named "verified empty" statement would assert the emptiness of
a referent no such envelope ever occupies -- absence dressed as
agreement in the one surface built to exclude it.

**Self-contained.** The Open WebUI process does not import `okg`. This
module therefore imports nothing from `okg.*` -- only the standard
library and pydantic (which Open WebUI already depends on). Everything
substrate-side is reached by running the `okg` CLI through an argv list.
Because the vocabulary it mirrors lives in the substrate, the repo-side
tests assert this module's literal tables against the substrate's own
(see `tests/substrate/chat/test_review_queue_pipe.py`).

**A source that did not answer is never rendered as zero.** The PACT
queue *degrades* rather than fails when the decision ledger is
unreachable: `build_queue` catches the error, appends a warning, and the
CLI still exits 0 with `ok: true`, valid JSON, a silently smaller row
set, and a reassuring `answer` sentence ("Nothing urgent is blocking
active work."). Exit code and JSON validity are therefore *proven
insufficient* as availability signals for this source. A non-empty
`warnings` array makes the PACT source UNAVAILABLE here, and an
unavailable source contributes no answer text, no summary lines, and no
count -- only a refusal naming the source and the exact command that
produced it.

**Message text can only ever occupy one argv slot.** The single
sanitization boundary is argv-slot confinement: the operator's message
may be passed as the *value* of `--query` on the read-only view verb, and
nowhere else. Shell metacharacters are not rejected -- there is no shell
(every call is `subprocess.run` with an argv list), and rejecting them
would break every question containing an apostrophe while proving
nothing. Decision verbs take no message-sourced text at all: their
arguments come from the Valves, from rows this module itself just
listed, and from the fixed verdict vocabularies below.
"""

from __future__ import annotations

import inspect
import json
import os
import subprocess
import tempfile
import urllib.error
import urllib.request
import uuid
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from typing import Any, Callable

from pydantic import BaseModel, Field

__all__ = [
    "ADMISSION_DECISIONS",
    "ADMISSION_HELD_FLAG",
    "COVERAGE_STATEMENT",
    "DECISION_TYPE_ACTIONS",
    "ENUMERATED_OKG_VERBS",
    "EXTERNAL_ACTION_DECISIONS",
    "EXTERNAL_ACTION_INBOX_DECISIONS",
    "EXTERNAL_ACTION_LIST_LIMIT",
    "EXTERNAL_ACTION_SOURCE",
    "MEMBER",
    "MODEL_ID",
    "MODEL_NAME",
    "NOT_MEMBER",
    "PACT_DECIDABLE_VERDICTS",
    "PACT_HUMAN_VERDICTS",
    "POPULATED",
    "UNAVAILABLE",
    "UNDETERMINED",
    "VERIFIED_EMPTY",
    "CommandResult",
    "DecisionIntent",
    "Pipe",
    "Precondition",
    "RowAction",
    "SourceOutcome",
    "UnmappedDecisionType",
    "admission_row_action",
    "build_admission_decide_argv",
    "build_admission_list_argv",
    "build_admission_show_argv",
    "build_external_action_decide_argv",
    "build_external_action_list_argv",
    "build_external_action_show_argv",
    "build_external_action_timeline_argv",
    "build_pact_decide_argv",
    "build_pact_view_argv",
    "check_host_preconditions",
    "classify_admission_result",
    "classify_pact_result",
    "decision_blockers",
    "packet_row_handle",
    "pact_row_handle",
    "pact_rows_by_handle",
    "parse_decision_intent",
    "render_admission_section",
    "render_decision_availability",
    "render_external_action_section",
    "render_pact_section",
    "resolve_commit_sha",
    "resolve_group_membership",
    "row_action",
    "run_okg",
]

# The id this Pipe's `pipes()` returns. It is NOT the id an instance
# registers the model under: Open WebUI composes a manifold's model id as
# `f'{pipe.id}.{p["id"]}'` (functions.py:106 at v0.11.0), where `pipe.id`
# is the FUNCTION id the module is installed as. Anything auditing the
# live model listing must compose the two -- see
# `review_inbox_grant_audit.live_manifold_model_id`.
MODEL_ID = "okg-review-inbox"
MODEL_NAME = "OKG Review Inbox"

# The two flags that IDENTIFY the pending set, and the whole of it: the
# deployment and `--held`, which the CLI resolves to every admission mode
# whose runs finalize `pending` for a decision. This module deliberately
# holds no constant for the excluded predicate named in the module
# docstring's first contract -- not even to say it is excluded, because the
# tests scan this file's RAW SOURCE TEXT and the module docstring is the
# single exempted region.
ADMISSION_HELD_FLAG = "--held"

# `okg pact decide --verdict` choices a human operator may pick
# (fsm.HUMAN_VERDICTS). `escalate` is agent-only and `reopen` is
# operator-only outside the review loop, so neither is offered here.
# This is the RENDERED vocabulary: it is what the row's instruction
# spells out for the operator to run.
PACT_HUMAN_VERDICTS = ("approve", "revise", "reject", "supersede", "defer")

# The subset this Pipe will EXECUTE. `supersede` needs `--target <id>`
# (fsm.legal_transition guards on it) and the chat grammar has no slot
# for a second identifier, so offering it from here would advertise a
# verb that always fails. It stays in the rendered instruction above,
# where the operator supplies the target themselves.
PACT_DECIDABLE_VERDICTS = ("approve", "revise", "reject", "defer")

# `okg admission decide --decision` choices this surface offers. The CLI
# accepts further spellings that HOLD an envelope rather than resolving
# it; the inbox offers only the two that resolve a pending held
# envelope.
ADMISSION_DECISIONS = ("accept", "reject")

# `okg admission list --limit` is clamped to 500 by the CLI. Asking for
# the clamp maximum and treating a saturated read as an UNKNOWN total is
# the honest handling: the CLI reports `count` as the length of the
# POST-LIMIT row set, so a default read of 25 pending envelopes would
# report "20 pending" with no truncation marker -- the shown-as-total
# defect one notch milder.
ADMISSION_LIST_LIMIT = 500

# The standalone external-action API also clamps list reads at 500 rows.  A
# saturated response is therefore incomplete and cannot be presented as a
# complete review set.
EXTERNAL_ACTION_LIST_LIMIT = 500

# These are all review decisions accepted by the external-action API.  The
# normal inbox offers only the subset below; edit approval remains in the
# detail client because it requires authorized replacement payload bytes.
EXTERNAL_ACTION_DECISIONS = (
    "approve",
    "approve_with_edit",
    "reject",
    "defer",
)
# The normal inbox never reads or accepts replacement payload bytes, so its
# executable grammar is the strict subset that needs no payload argument.
EXTERNAL_ACTION_INBOX_DECISIONS = ("approve", "reject", "defer")
EXTERNAL_ACTION_SOURCE = "external_action"

# The two decide verbs take DIFFERENT flag names. Spelled out so the
# rendered instruction is copy-pasteable and the tests can assert them.
PACT_DECIDE_FLAG = "--verdict"
ADMISSION_DECIDE_FLAG = "--decision"
ADMISSION_DEPLOYMENT_FLAG = "--deployment"

_LEDGER_WARNING_MARKERS = (
    # The raw warning appended by review.build_queue …
    "decision ledger unavailable",
    # … and the operator-facing rewrite applied by the chat packet
    # (_chat_queue_warning in src/okg/substrate/pact/cli.py).
    "decision history is unavailable",
)


class UnmappedDecisionType(Exception):
    """A queue row carried a decision_type this Pipe has no action for.

    Raised rather than rendered blank: an unmapped decision_type must
    surface as a loud refusal, never as an empty action cell or an
    invented okg verb.
    """

    def __init__(self, decision_type: str) -> None:
        super().__init__(
            f"no CLI action is mapped for decision_type {decision_type!r}; "
            "the review inbox refuses to invent one"
        )
        self.decision_type = decision_type


@dataclass(frozen=True)
class RowAction:
    """What acts on one queue row.

    `executable` actions are verbs this Pipe may run (they are inside the
    enumerated review set). `instruction` actions are rendered as text
    for the operator to run themselves -- either because the verb is
    outside the enumerated set (`okg pact gate task-done`) or because
    there is no okg verb at all (archiving is a repo move).
    """

    decision_type: str
    kind: str  # "executable" | "instruction"
    text: str
    explanation: str


# TOTAL over the key set of _DECISION_RANK in
# src/okg/substrate/pact/review.py. The queue's decision_type vocabulary
# is NOT the okg verb vocabulary -- `okg pact` has no archive verb at all
# -- so the mapping is explicit and its totality is asserted by
# test_decision_type_mapping_total_over_rank_keys.
DECISION_TYPE_ACTIONS: dict[str, RowAction] = {
    "review": RowAction(
        decision_type="review",
        kind="executable",
        text=(
            "okg pact decide <change> "
            f"{PACT_DECIDE_FLAG} <{'|'.join(PACT_HUMAN_VERDICTS)}>"
        ),
        explanation="Completed work waiting for an accept/revise decision.",
    ),
    "repair": RowAction(
        decision_type="repair",
        kind="executable",
        text=f"okg pact decide <change> {PACT_DECIDE_FLAG} revise --note <repair note>",
        explanation="The change does not load cleanly; send it back for repair.",
    ),
    "go-ahead": RowAction(
        decision_type="go-ahead",
        kind="instruction",
        text="okg pact gate task-done <change> <task>",
        explanation=(
            "A start/no-start decision. The gate verb is outside this "
            "inbox's enumerated verb set, so it is shown to run yourself."
        ),
    ),
    "archive": RowAction(
        decision_type="archive",
        kind="instruction",
        text="git mv pact/changes/<change> pact/archive/<change>",
        explanation=(
            "Archiving is a repo move, not an okg verb. The inbox will not "
            "invent one."
        ),
    ),
}


def row_action(decision_type: str) -> RowAction:
    """The action for one decision_type. Refuses on an unmapped type."""
    try:
        return DECISION_TYPE_ACTIONS[decision_type]
    except KeyError:
        raise UnmappedDecisionType(decision_type) from None


def _render_row_action(action: RowAction, *, change: str, note: str = "") -> str:
    text = action.text.replace("<change>", change)
    if action.decision_type == "repair" and note:
        text = text.replace("<repair note>", note)
    return text


def admission_row_action(admission_id: str, deployment: str) -> str:
    return (
        f"okg admission decide {admission_id} "
        f"{ADMISSION_DEPLOYMENT_FLAG} {deployment} "
        f"{ADMISSION_DECIDE_FLAG} <{'|'.join(ADMISSION_DECISIONS)}>"
    )


# --------------------------------------------------------------------------
# Command builders. Every okg invocation this Pipe can make is one of
# these five; nothing else is reachable, and none of them is a shell
# string. `query` is the ONLY parameter anywhere that may carry message
# text, and it occupies exactly one argv slot.
# --------------------------------------------------------------------------


def build_pact_view_argv(
    cli: str, repo_root: str, *, query: str | None = None
) -> list[str]:
    """The read-only PACT queue read. `--root` is on the `pact` group."""
    argv = [
        cli,
        "pact",
        "--root",
        repo_root,
        "view",
        "--format=chat-review-queue",
        "--json",
    ]
    if query is not None:
        # THE carve-out. `query` is the operator's own message text and
        # occupies this single argv slot as a value. No shell is involved,
        # so no escaping and no metacharacter rejection: an apostrophe is
        # just a character in an argv element.
        argv += ["--query", query]
    return argv


def build_admission_list_argv(cli: str, deployment: str) -> list[str]:
    """The pending-envelope read: deployment and mode, and nothing else.

    The predicate the module docstring's first contract excludes cannot
    be added here: `okg admission list` exposes no flag that would carry
    it, and the envelopes this inbox exists to show carry no such value
    at all. (This docstring is a LIVE runtime string -- `__doc__` -- so it
    is scanned like any other, and it deliberately names nothing.)
    """
    return [
        cli,
        "admission",
        "list",
        ADMISSION_DEPLOYMENT_FLAG,
        deployment,
        "--status",
        "pending",
        ADMISSION_HELD_FLAG,
        "--limit",
        str(ADMISSION_LIST_LIMIT),
        "--json",
    ]


def build_admission_show_argv(cli: str, deployment: str, admission_id: str) -> list[str]:
    return [
        cli,
        "admission",
        "show",
        admission_id,
        ADMISSION_DEPLOYMENT_FLAG,
        deployment,
        "--json",
    ]


def _external_action_prefix(
    cli: str,
    *,
    api_url: str = "",
    authorization_env: str = "OKG_EXTERNAL_ACTION_AUTHORIZATION",
    dpop_env: str = "OKG_EXTERNAL_ACTION_DPOP",
    client_cert: str = "",
    client_key: str = "",
    ca_bundle: str = "",
) -> list[str]:
    """Build the non-secret global options for the HTTP-only client.

    The Pipe never receives credential bytes.  It passes environment-variable
    *names* to the CLI, leaving the token/proof lookup inside the process that
    owns the configured HTTP client.  Certificate and CA values are paths,
    never certificate contents.
    """
    argv = [cli, "external-action"]
    if api_url.strip():
        argv += ["--api-url", api_url]
    if authorization_env.strip():
        argv += ["--authorization-env", authorization_env]
    if dpop_env.strip():
        argv += ["--dpop-env", dpop_env]
    if client_cert.strip():
        argv += ["--client-cert", client_cert]
    if client_key.strip():
        argv += ["--client-key", client_key]
    if ca_bundle.strip():
        argv += ["--ca-bundle", ca_bundle]
    return argv


def build_external_action_list_argv(
    cli: str,
    deployment: str,
    *,
    state: str | None = None,
    api_url: str = "",
    authorization_env: str = "OKG_EXTERNAL_ACTION_AUTHORIZATION",
    dpop_env: str = "OKG_EXTERNAL_ACTION_DPOP",
    client_cert: str = "",
    client_key: str = "",
    ca_bundle: str = "",
) -> list[str]:
    """Build the redacted external-action proposal list read."""
    argv = _external_action_prefix(
        cli,
        api_url=api_url,
        authorization_env=authorization_env,
        dpop_env=dpop_env,
        client_cert=client_cert,
        client_key=client_key,
        ca_bundle=ca_bundle,
    )
    argv += ["list", ADMISSION_DEPLOYMENT_FLAG, deployment]
    if state is not None:
        if state not in {"pending", "edited", "approved", "rejected", "expired", "cancelled"}:
            raise ValueError(f"unknown external-action review state {state!r}")
        argv += ["--state", state]
    argv += ["--limit", str(EXTERNAL_ACTION_LIST_LIMIT), "--json"]
    return argv


def build_external_action_show_argv(
    cli: str,
    deployment: str,
    proposal_id: str,
    *,
    api_url: str = "",
    authorization_env: str = "OKG_EXTERNAL_ACTION_AUTHORIZATION",
    dpop_env: str = "OKG_EXTERNAL_ACTION_DPOP",
    client_cert: str = "",
    client_key: str = "",
    ca_bundle: str = "",
) -> list[str]:
    """Build a safe metadata read for one external-action proposal."""
    return [
        *_external_action_prefix(
            cli,
            api_url=api_url,
            authorization_env=authorization_env,
            dpop_env=dpop_env,
            client_cert=client_cert,
            client_key=client_key,
            ca_bundle=ca_bundle,
        ),
        "show",
        ADMISSION_DEPLOYMENT_FLAG,
        deployment,
        proposal_id,
        "--json",
    ]


def build_external_action_timeline_argv(
    cli: str,
    deployment: str,
    proposal_id: str,
    *,
    api_url: str = "",
    authorization_env: str = "OKG_EXTERNAL_ACTION_AUTHORIZATION",
    dpop_env: str = "OKG_EXTERNAL_ACTION_DPOP",
    client_cert: str = "",
    client_key: str = "",
    ca_bundle: str = "",
) -> list[str]:
    """Build the complete ordered timeline read for one proposal."""
    return [
        *_external_action_prefix(
            cli,
            api_url=api_url,
            authorization_env=authorization_env,
            dpop_env=dpop_env,
            client_cert=client_cert,
            client_key=client_key,
            ca_bundle=ca_bundle,
        ),
        "timeline",
        ADMISSION_DEPLOYMENT_FLAG,
        deployment,
        proposal_id,
        "--json",
    ]


def build_external_action_decide_argv(
    cli: str,
    deployment: str,
    proposal_id: str,
    *,
    decision: str,
    idempotency_key: str,
    if_match: str,
    body_file: str,
    api_url: str = "",
    authorization_env: str = "OKG_EXTERNAL_ACTION_AUTHORIZATION",
    dpop_env: str = "OKG_EXTERNAL_ACTION_DPOP",
    client_cert: str = "",
    client_key: str = "",
    ca_bundle: str = "",
) -> list[str]:
    """Build one authenticated external-action decision command.

    Body contents stay in a temporary file.  In particular, no payload or
    reason text is inserted into the argv list or the Pipe command log.
    """
    if decision not in EXTERNAL_ACTION_DECISIONS:
        raise ValueError(
            f"decision {decision!r} is not one of {EXTERNAL_ACTION_DECISIONS}; "
            "the review inbox does not pass through free text"
        )
    if not idempotency_key or not if_match or not body_file:
        raise ValueError("external-action decisions require key, ETag, and body file")
    return [
        *_external_action_prefix(
            cli,
            api_url=api_url,
            authorization_env=authorization_env,
            dpop_env=dpop_env,
            client_cert=client_cert,
            client_key=client_key,
            ca_bundle=ca_bundle,
        ),
        "decide",
        ADMISSION_DEPLOYMENT_FLAG,
        deployment,
        proposal_id,
        "--idempotency-key",
        idempotency_key,
        "--if-match",
        if_match,
        "--body-file",
        body_file,
        "--json",
    ]


def build_pact_decide_argv(
    cli: str,
    repo_root: str,
    *,
    change: str,
    verdict: str,
    note: str | None = None,
) -> list[str]:
    """Build `okg pact decide`. `verdict` must be a fixed vocabulary member.

    `change` comes from a row this Pipe listed and `note` from that row's
    own text; neither is ever message-sourced.
    """
    if verdict not in PACT_DECIDABLE_VERDICTS:
        raise ValueError(
            f"verdict {verdict!r} is not one of {PACT_DECIDABLE_VERDICTS}; "
            "the review inbox does not pass through free-text verdicts"
        )
    argv = [
        cli,
        "pact",
        "--root",
        repo_root,
        "decide",
        change,
        PACT_DECIDE_FLAG,
        verdict,
        "--json",
    ]
    if note:
        argv += ["--note", note]
    return argv


def build_admission_decide_argv(
    cli: str, deployment: str, *, admission_id: str, decision: str
) -> list[str]:
    if decision not in ADMISSION_DECISIONS:
        raise ValueError(
            f"decision {decision!r} is not one of {ADMISSION_DECISIONS}; "
            "the review inbox does not pass through free-text decisions"
        )
    return [
        cli,
        "admission",
        "decide",
        admission_id,
        ADMISSION_DEPLOYMENT_FLAG,
        deployment,
        ADMISSION_DECIDE_FLAG,
        decision,
        "--json",
    ]


# The verb prefixes (argv[1:3]) this Pipe may ever invoke. Any other okg
# verb reaching the runner is a bug, and the runner refuses it.
ENUMERATED_OKG_VERBS: frozenset[tuple[str, ...]] = frozenset(
    {
        ("pact", "view"),
        ("pact", "decide"),
        ("admission", "list"),
        ("admission", "show"),
        ("admission", "decide"),
        ("external-action", "list"),
        ("external-action", "show"),
        ("external-action", "timeline"),
        ("external-action", "decide"),
    }
)


def _okg_verb(argv: Sequence[str]) -> tuple[str, ...]:
    """The (family, subcommand) pair of an okg argv, skipping group flags."""
    tokens = [
        token
        for token in list(argv)[1:]
        if not token.startswith("-")
    ]
    # `--root <path>` puts a bare path between the family and the verb.
    if len(tokens) >= 3 and tokens[0] == "pact":
        root_index = None
        for index, token in enumerate(argv):
            if token == "--root":
                root_index = index + 1
                break
        if root_index is not None and root_index < len(argv):
            root_value = argv[root_index]
            tokens = [token for token in tokens if token != root_value]
    return tuple(tokens[:2])


@dataclass(frozen=True)
class CommandResult:
    argv: tuple[str, ...]
    returncode: int
    stdout: str
    stderr: str

    @property
    def display(self) -> str:
        return " ".join(self.argv)


def run_okg(argv: Sequence[str], *, timeout: float = 120.0) -> CommandResult:
    """Run one enumerated okg command. argv list, never a shell."""
    verb = _okg_verb(argv)
    if verb not in ENUMERATED_OKG_VERBS:
        raise ValueError(
            f"okg verb {' '.join(verb)!r} is outside the review inbox's "
            "enumerated set; refusing to widen the bounded CLI exception"
        )
    completed = subprocess.run(  # noqa: S603 - argv list, shell=False
        list(argv),
        capture_output=True,
        text=True,
        timeout=timeout,
        shell=False,
    )
    return CommandResult(
        argv=tuple(argv),
        returncode=completed.returncode,
        stdout=completed.stdout,
        stderr=completed.stderr,
    )


# --------------------------------------------------------------------------
# Source outcomes: populated / verified-empty / unavailable. Three states,
# never two. `shown`, `total`, and `rows` are None on the unavailable path
# so a caller cannot accidentally render a count for a source that did
# not answer.
# --------------------------------------------------------------------------

POPULATED = "populated"
VERIFIED_EMPTY = "verified_empty"
UNAVAILABLE = "unavailable"


@dataclass(frozen=True)
class SourceOutcome:
    source: str  # "pact" | "admission" | "external_action"
    state: str
    command: str
    reason: str = ""
    rows: tuple[dict[str, Any], ...] | None = None
    shown: int | None = None
    total: int | None = None
    emptiness_authority: str = ""
    packet: dict[str, Any] | None = None
    # External-action timelines are keyed by proposal ID but kept as a tuple
    # so a classifier cannot accidentally mutate the response after it has
    # crossed the source boundary.
    timelines: tuple[tuple[str, tuple[dict[str, Any], ...]], ...] = ()
    # True when the read saturated its own limit, so the row set is known
    # to be incomplete and `total` is NOT known. `total` is None in that
    # case: reporting the truncated length as the total is the
    # shown-as-total defect.
    truncated: bool = False

    @property
    def available(self) -> bool:
        return self.state != UNAVAILABLE


def _parse_json(result: CommandResult) -> tuple[dict[str, Any] | None, str]:
    try:
        payload = json.loads(result.stdout)
    except (ValueError, TypeError):
        return None, "the command emitted malformed or truncated JSON"
    if not isinstance(payload, Mapping):
        return None, "the command emitted JSON that is not an object"
    return dict(payload), ""


def _short(text: str, limit: int = 240) -> str:
    collapsed = " ".join((text or "").split())
    return collapsed[:limit]


def classify_pact_result(result: CommandResult) -> SourceOutcome:
    """Classify the PACT queue read into exactly one of three states.

    The warnings check is the load-bearing one. `build_queue` degrades
    when the decision ledger is unreachable: exit 0, `ok: true`, valid
    JSON, a smaller row set, and an unchanged all-clear `answer`. Exit
    code and JSON validity are clean in that failure, so they cannot be
    the triggers. A non-empty `warnings` array is.
    """
    command = result.display
    if result.returncode != 0:
        return SourceOutcome(
            source="pact",
            state=UNAVAILABLE,
            command=command,
            reason=(
                f"the command exited {result.returncode}"
                + (f": {_short(result.stderr)}" if result.stderr.strip() else "")
            ),
        )
    payload, error = _parse_json(result)
    if payload is None:
        return SourceOutcome(
            source="pact", state=UNAVAILABLE, command=command, reason=error
        )
    packet = payload.get("chat_review_queue")
    if not isinstance(packet, Mapping):
        return SourceOutcome(
            source="pact",
            state=UNAVAILABLE,
            command=command,
            reason="the payload carried no chat_review_queue packet",
        )
    warnings = packet.get("warnings")
    if not isinstance(warnings, list):
        return SourceOutcome(
            source="pact",
            state=UNAVAILABLE,
            command=command,
            reason=(
                "the packet carried no warnings array, so degradation could "
                "not be ruled out"
            ),
        )
    if warnings:
        lowered = " ".join(str(w).lower() for w in warnings)
        if any(marker in lowered for marker in _LEDGER_WARNING_MARKERS):
            reason = (
                "the decision ledger is unavailable, so the queue silently "
                "omitted recorded decisions and in-flight intents. The "
                "command exited 0 with valid JSON and an all-clear answer "
                "sentence; neither is evidence that the queue is complete"
            )
        else:
            reason = (
                "the queue reported warnings, so its row set is not known to "
                f"be complete: {_short('; '.join(str(w) for w in warnings))}"
            )
        # Deliberately no rows/shown/total/packet: nothing from a warned
        # packet may be rendered.
        return SourceOutcome(
            source="pact", state=UNAVAILABLE, command=command, reason=reason
        )
    rows = packet.get("rows")
    if not isinstance(rows, list):
        return SourceOutcome(
            source="pact",
            state=UNAVAILABLE,
            command=command,
            reason="the packet carried no rows array",
        )
    total = packet.get("total")
    shown = packet.get("shown")
    if not isinstance(total, int) or not isinstance(shown, int):
        return SourceOutcome(
            source="pact",
            state=UNAVAILABLE,
            command=command,
            reason="the packet carried no shown/total counts",
        )
    state = VERIFIED_EMPTY if total == 0 else POPULATED
    return SourceOutcome(
        source="pact",
        state=state,
        command=command,
        rows=tuple(dict(row) for row in rows),
        shown=shown,
        total=total,
        packet=dict(packet),
    )


def classify_admission_result(
    result: CommandResult, *, queried_at: str
) -> SourceOutcome:
    command = result.display
    if result.returncode != 0:
        return SourceOutcome(
            source="admission",
            state=UNAVAILABLE,
            command=command,
            reason=(
                f"the command exited {result.returncode}"
                + (f": {_short(result.stderr)}" if result.stderr.strip() else "")
            ),
        )
    payload, error = _parse_json(result)
    if payload is None:
        return SourceOutcome(
            source="admission", state=UNAVAILABLE, command=command, reason=error
        )
    rows = payload.get("admissions")
    if not isinstance(rows, list):
        return SourceOutcome(
            source="admission",
            state=UNAVAILABLE,
            command=command,
            reason="the payload carried no admissions array",
        )
    count = payload.get("count")
    if not isinstance(count, int) or isinstance(count, bool):
        # Symmetrical with the PACT half, which refuses a packet carrying
        # no shown/total counts. Falling back to `len(rows)` here turned a
        # payload with NO count and NO rows into "Admission: verified
        # empty" stated as authority -- a count nobody reported,
        # manufactured from an empty list. An absent count is an absent
        # measurement, so the read did not answer.
        return SourceOutcome(
            source="admission",
            state=UNAVAILABLE,
            command=command,
            reason=(
                "the payload carried no integer `count`, so a genuinely "
                "empty pending set could not be distinguished from a read "
                "that reported nothing"
            ),
        )
    # `okg admission list` applies `--limit` in SQL and then reports
    # `count` as the length of the POST-LIMIT row set. A saturated read
    # therefore carries no information about the true total, so the total
    # is refused rather than guessed.
    truncated = len(rows) >= ADMISSION_LIST_LIMIT
    state = VERIFIED_EMPTY if count == 0 else POPULATED
    return SourceOutcome(
        source="admission",
        state=state,
        command=command,
        rows=tuple(dict(row) for row in rows),
        shown=len(rows),
        total=None if truncated else count,
        truncated=truncated,
        emptiness_authority=queried_at,
    )


_EXTERNAL_REVIEW_STATES = {
    "pending",
    "edited",
    "approved",
    "rejected",
    "expired",
    "cancelled",
}
_EXTERNAL_FORBIDDEN_KEYS = {
    "payload",
    "canonical_payload",
    "payload_bytes",
    "payload_value",
    "credential",
    "credentials",
    "credential_secret",
    "access_token",
    "refresh_token",
    "dpop_proof",
    "certificate_bytes",
    "provider_secret",
    "provider_secrets",
    "provider_receipt",
}


def _has_forbidden_external_value(value: Any) -> bool:
    """Return whether a normal external read contains a secret-bearing key."""
    if isinstance(value, Mapping):
        for key, child in value.items():
            normalized = str(key).strip().lower().replace("-", "_")
            if normalized in _EXTERNAL_FORBIDDEN_KEYS:
                return True
            if _has_forbidden_external_value(child):
                return True
    elif isinstance(value, (list, tuple)):
        return any(_has_forbidden_external_value(child) for child in value)
    return False


def _external_timeline_payload(
    result: CommandResult, *, proposal_id: str
) -> tuple[tuple[dict[str, Any], ...] | None, str | None, str | None, str]:
    """Parse one complete timeline without accepting payload-bearing fields."""
    if result.returncode != 0:
        return None, None, None, (
            f"the command exited {result.returncode}"
            + (f": {_short(result.stderr)}" if result.stderr.strip() else "")
        )
    payload, error = _parse_json(result)
    if payload is None:
        return None, None, None, error
    if _has_forbidden_external_value(payload):
        return None, None, None, "the timeline response contained a forbidden payload or credential field"
    events = payload.get("events")
    if not isinstance(events, list):
        return None, None, None, "the payload carried no events array"
    if not events:
        return None, None, None, "the proposal timeline was empty"
    if payload.get("proposal_id") not in {None, proposal_id}:
        return None, None, None, "the timeline returned a different proposal"
    original_hash = payload.get("original_payload_hash")
    current_hash = payload.get("current_payload_hash")
    previous_sequence = 0
    normalized: list[dict[str, Any]] = []
    for event in events:
        if not isinstance(event, Mapping):
            return None, None, None, "the timeline carried a non-object event"
        sequence = event.get("sequence")
        if not isinstance(sequence, int) or isinstance(sequence, bool) or sequence <= previous_sequence:
            return None, None, None, "the timeline events were not strictly ordered"
        previous_sequence = sequence
        if event.get("proposal_id") not in {None, proposal_id}:
            return None, None, None, "the timeline carried an event for a different proposal"
        normalized_event = dict(event)
        normalized.append(normalized_event)
    # These metadata fields are optional because the current HTTP projection
    # returns them on the proposal read, while the timeline route returns the
    # event stream.  When present, retain them only for the parity check in
    # `read_external_action_half`, not for rendering as free text.
    if original_hash is not None and not isinstance(original_hash, str):
        return None, None, None, "the timeline original payload hash was not a string"
    if current_hash is not None and not isinstance(current_hash, str):
        return None, None, None, "the timeline current payload hash was not a string"
    return tuple(normalized), original_hash, current_hash, ""


def classify_external_action_result(
    result: CommandResult, *, queried_at: str
) -> SourceOutcome:
    """Classify the redacted external-action list into three source states.

    The endpoint returns a projection, not a count.  A response at the
    client/server limit or carrying a cursor is therefore incomplete.  Only
    pending and edited review states belong in this review inbox; terminal
    history stays available through the same API but is not an action row.
    """
    command = result.display
    if result.returncode != 0:
        return SourceOutcome(
            source=EXTERNAL_ACTION_SOURCE,
            state=UNAVAILABLE,
            command=command,
            reason=(
                f"the command exited {result.returncode}"
                + (f": {_short(result.stderr)}" if result.stderr.strip() else "")
            ),
        )
    payload, error = _parse_json(result)
    if payload is None:
        return SourceOutcome(
            source=EXTERNAL_ACTION_SOURCE,
            state=UNAVAILABLE,
            command=command,
            reason=error,
        )
    if _has_forbidden_external_value(payload):
        return SourceOutcome(
            source=EXTERNAL_ACTION_SOURCE,
            state=UNAVAILABLE,
            command=command,
            reason="the list response contained a forbidden payload or credential field",
        )
    proposals = payload.get("proposals")
    if not isinstance(proposals, list):
        return SourceOutcome(
            source=EXTERNAL_ACTION_SOURCE,
            state=UNAVAILABLE,
            command=command,
            reason="the payload carried no proposals array",
        )
    cursor = payload.get("next_cursor")
    if cursor is not None and not isinstance(cursor, str):
        return SourceOutcome(
            source=EXTERNAL_ACTION_SOURCE,
            state=UNAVAILABLE,
            command=command,
            reason="the list carried an invalid next cursor",
        )
    truncated = bool(cursor) or len(proposals) >= EXTERNAL_ACTION_LIST_LIMIT
    rows: list[dict[str, Any]] = []
    seen: set[str] = set()
    for proposal in proposals:
        if not isinstance(proposal, Mapping):
            return SourceOutcome(
                source=EXTERNAL_ACTION_SOURCE,
                state=UNAVAILABLE,
                command=command,
                reason="the proposals array carried a non-object row",
            )
        row = dict(proposal)
        if _has_forbidden_external_value(row):
            return SourceOutcome(
                source=EXTERNAL_ACTION_SOURCE,
                state=UNAVAILABLE,
                command=command,
                reason="a proposal row contained a forbidden payload or credential field",
            )
        proposal_id = row.get("proposal_id")
        review_state = row.get("review_state")
        if not isinstance(proposal_id, str) or not proposal_id:
            return SourceOutcome(
                source=EXTERNAL_ACTION_SOURCE,
                state=UNAVAILABLE,
                command=command,
                reason="a proposal row carried no usable proposal_id",
            )
        if proposal_id in seen:
            return SourceOutcome(
                source=EXTERNAL_ACTION_SOURCE,
                state=UNAVAILABLE,
                command=command,
                reason=f"the list repeated proposal {proposal_id}",
            )
        seen.add(proposal_id)
        if not isinstance(review_state, str) or review_state not in _EXTERNAL_REVIEW_STATES:
            return SourceOutcome(
                source=EXTERNAL_ACTION_SOURCE,
                state=UNAVAILABLE,
                command=command,
                reason=f"proposal {proposal_id} carried an unknown review state",
            )
        deployment = row.get("deployment")
        if not isinstance(deployment, str) or not deployment:
            return SourceOutcome(
                source=EXTERNAL_ACTION_SOURCE,
                state=UNAVAILABLE,
                command=command,
                reason=f"proposal {proposal_id} carried no usable deployment",
            )
        for field in ("execution_state", "original_payload_hash", "current_payload_hash", "etag"):
            value = row.get(field)
            if not isinstance(value, str) or not value:
                return SourceOutcome(
                    source=EXTERNAL_ACTION_SOURCE,
                    state=UNAVAILABLE,
                    command=command,
                    reason=f"proposal {proposal_id} carried no usable `{field}`",
                )
        if review_state in {"pending", "edited"}:
            rows.append(row)
    if truncated:
        return SourceOutcome(
            source=EXTERNAL_ACTION_SOURCE,
            state=UNAVAILABLE,
            command=command,
            reason=(
                f"the list reached its {EXTERNAL_ACTION_LIST_LIMIT}-row bound"
                + (" and returned a next cursor" if cursor else "")
                + "; the complete pending set is unknown"
            ),
        )
    return SourceOutcome(
        source=EXTERNAL_ACTION_SOURCE,
        state=VERIFIED_EMPTY if not rows else POPULATED,
        command=command,
        rows=tuple(rows),
        shown=len(rows),
        total=len(rows),
        emptiness_authority=queried_at,
        packet={"proposals": tuple(rows)},
    )


_EXTERNAL_PROJECTION_FIELDS = (
    "proposal_id",
    "deployment",
    "review_state",
    "execution_state",
    "original_payload_hash",
    "current_payload_hash",
    "etag",
)


def _external_projection_from_result(
    result: CommandResult, *, proposal_id: str, deployment: str
) -> tuple[dict[str, Any] | None, str]:
    """Parse one redacted show projection and enforce list/show parity."""
    if result.returncode != 0:
        return None, (
            f"the command exited {result.returncode}"
            + (f": {_short(result.stderr)}" if result.stderr.strip() else "")
        )
    payload, error = _parse_json(result)
    if payload is None:
        return None, error
    if _has_forbidden_external_value(payload):
        return None, "the show response contained a forbidden payload or credential field"
    if payload.get("proposal_id") != proposal_id:
        return None, "the show response returned a different proposal"
    if payload.get("deployment") != deployment:
        return None, "the show response returned a different deployment"
    for field in _EXTERNAL_PROJECTION_FIELDS:
        value = payload.get(field)
        if not isinstance(value, str) or not value:
            return None, f"the show response carried no usable `{field}`"
    if payload["review_state"] not in _EXTERNAL_REVIEW_STATES:
        return None, "the show response carried an unknown review state"
    return payload, ""


# --------------------------------------------------------------------------
# Host preconditions. Measured, never assumed. An unset Valve is UNMET.
# --------------------------------------------------------------------------


@dataclass(frozen=True)
class Precondition:
    name: str
    met: bool
    detail: str


def check_host_preconditions(
    *,
    okg_cli: str,
    repo_root: str,
    deployment: str,
    executable_probe: Callable[[str], bool] | None = None,
    directory_probe: Callable[[str], bool] | None = None,
) -> list[Precondition]:
    """The startup self-check for the three host facts the Pipe needs.

    The Pipe runs INSIDE the Open WebUI process, whose cwd is not the OKG
    repo; a containerized instance must have the repo and the okg
    executable mounted in. Each check is measured against the filesystem,
    not inferred from a Valve being set -- but an unset Valve is UNMET, so
    a forgotten Valve can never read as satisfied.
    """
    is_executable = executable_probe or (
        lambda path: os.path.isfile(path) and os.access(path, os.X_OK)
    )
    is_directory = directory_probe or (lambda path: os.path.isdir(path))

    checks: list[Precondition] = []

    if not okg_cli.strip():
        checks.append(
            Precondition(
                "okg_cli",
                False,
                "Valve `okg_cli` is unset. An unset Valve is unmet, never "
                "assumed; set it to the absolute path of the okg executable "
                "inside the Open WebUI process.",
            )
        )
    elif not is_executable(okg_cli):
        checks.append(
            Precondition(
                "okg_cli",
                False,
                f"`{okg_cli}` is not an executable file in this process.",
            )
        )
    else:
        checks.append(
            Precondition("okg_cli", True, f"`{okg_cli}` is executable here.")
        )

    if not repo_root.strip():
        checks.append(
            Precondition(
                "repo_root",
                False,
                "Valve `repo_root` is unset. The PACT half walks "
                "`pact/changes` under this root and the Open WebUI process's "
                "cwd is not the OKG repo, so the root must be stated.",
            )
        )
    else:
        changes = os.path.join(repo_root, "pact", "changes")
        if not is_directory(changes):
            checks.append(
                Precondition(
                    "repo_root",
                    False,
                    f"`{repo_root}` does not resolve to a checkout containing "
                    "pact/changes.",
                )
            )
        else:
            checks.append(
                Precondition(
                    "repo_root", True, f"`{changes}` exists in this process."
                )
            )

    if not deployment.strip():
        checks.append(
            Precondition(
                "deployment",
                False,
                "Valve `deployment` is unset; the admission half cannot be "
                "read without a deployment name.",
            )
        )
    else:
        checks.append(
            Precondition("deployment", True, f"deployment `{deployment}`.")
        )

    return checks


# --------------------------------------------------------------------------
# Admin-group membership, resolved SERVER-SIDE.
#
# MEASURED VENDOR PIN — Open WebUI v0.11.0
# (image ghcr.io/open-webui/open-webui@sha256:72c0ba64…, `GET /api/version`
# → 0.11.0). Evidence:
# pact/changes/openwebui-chat-frontend/evidence/pipe-user-context-probe.md
#
# The `__user__` dict handed to a Pipe carries 22 keys and `groups` is NOT
# among them -- for an instance admin AND for a confirmed member of a real
# group (the probe created the group and verified `member_count: 1` before
# measuring, so the member arm is not vacuous). `role` and `id` ARE
# supplied, `role` as a plain str that distinguishes "admin" from "user".
#
# That absence is STRUCTURAL, not observational, which is what makes it
# total rather than intermittent. Every construction site builds the dict
# as `user.model_dump()` from a `UserModel`
# (`utils/middleware.py:2404`, `:3508`, `:3769`; `utils/plugin.py:121-122`),
# and `UserModel` (`models/users.py:86`) declares no `groups` field. A
# `groups` field DOES exist at `models/users.py:217` -- on
# `UserInfoResponse`, a DIFFERENT model that is never the source of
# `__user__`. The right name on the wrong object.
#
# A gate keyed on `groups` being present is therefore unconditionally
# false: it refuses every caller including the admin. That is the
# absence-dressed-as-agreement defect mirrored -- an absent signal read as
# a DECIDABLE one, with the fail-closed arm the only reachable arm.
#
# So membership is resolved from the server, never trusted from the caller
# payload, and "could not resolve" is a THIRD state that refuses while
# naming the missing capability -- never a pass, and never a blanket
# refusal that ignores the signals the vendor does supply.
# --------------------------------------------------------------------------

MEMBER = "member"
NOT_MEMBER = "not_member"
UNDETERMINED = "undetermined"


def _membership_via_open_webui(user_id: str, group: str) -> tuple[str, str]:
    """Read membership from Open WebUI's own database, in-process.

    The strongest available trust model: it reads the group table rather
    than a dict the framework assembled. Only importable inside the Open
    WebUI process, so the import is lazy and its absence is UNDETERMINED
    (this module must stay importable in the repo's test suite).

    MEASURED at v0.11.0: `Groups.get_groups_by_member_id` is `async def`
    (`models/groups.py:303`). Called from this SYNC Pipe it returns a
    coroutine rather than rows, and iterating that coroutine raises
    `TypeError: 'coroutine' object is not iterable`. That raise used to
    happen OUTSIDE the guard below, so it escaped the Pipe as an
    unhandled error instead of a typed refusal -- measured live against
    the pinned instance. An awaitable is therefore detected and reported
    as UNDETERMINED (naming the missing capability) so the HTTP transport
    runs; and the whole read, iteration included, is guarded, because a
    lookup that cannot answer must refuse rather than crash.
    """
    try:
        from open_webui.models.groups import Groups  # type: ignore[import-not-found]
    except Exception as err:  # noqa: BLE001 - any import failure is undetermined
        return UNDETERMINED, f"the in-process Open WebUI group API is unavailable ({err})"
    try:
        rows = Groups.get_groups_by_member_id(user_id)
        if inspect.isawaitable(rows):
            # Close it so the interpreter does not also warn about a
            # coroutine that was never awaited; the refusal is the answer.
            getattr(rows, "close", lambda: None)()
            return UNDETERMINED, (
                "the in-process Open WebUI group API is asynchronous at this "
                "version and cannot be driven from a synchronous Pipe"
            )
        if rows is None:
            return UNDETERMINED, "the in-process group lookup returned nothing"
        for row in rows:
            name = getattr(row, "name", None)
            identifier = getattr(row, "id", None)
            if group in {name, identifier}:
                return MEMBER, "in-process group lookup"
    except Exception as err:  # noqa: BLE001 - a failed lookup never allows
        return UNDETERMINED, f"the in-process group lookup failed ({err})"
    return NOT_MEMBER, "in-process group lookup"


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    """Turn any 3xx into an error instead of following it.

    See `_membership_via_http`: this module cannot reach the substrate's
    egress validator, so a followed redirect would be an unvalidated hop
    carrying the instance bearer token.
    """

    def redirect_request(self, req, fp, code, msg, headers, newurl):  # noqa: D102
        raise urllib.error.HTTPError(
            req.full_url, code, f"redirect refused: {msg}", headers, fp
        )


def _fetch_json(
    url: str, *, api_key: str, method: str = "GET", timeout: float = 15.0
) -> tuple[Any, str]:
    """One authenticated read of the instance's own API. (payload, error).

    A redirect is refused, not followed. This module cannot call the
    substrate's `open_validated_url` (it runs inside the Open WebUI
    process, which does not import `okg`), so the redirect is where a
    validated first hop would otherwise become an unvalidated second one
    — and it also carries the bearer token. Refusing is both the safe
    answer and an honest one: the caller sees UNDETERMINED, never an
    allow.
    """
    request = urllib.request.Request(  # noqa: S310 - operator-configured base
        url,
        method=method,
        headers={
            "Authorization": f"Bearer {api_key}",
            "Accept": "application/json",
        },
    )
    opener = urllib.request.build_opener(_NoRedirect())
    try:
        with opener.open(request, timeout=timeout) as response:
            return json.loads(response.read().decode("utf-8")), ""
    except (urllib.error.URLError, OSError, ValueError, TypeError) as err:
        return None, f"`{method} {url}` failed ({err})"


def _member_ids_from_rows(payload: Any) -> tuple[str, ...] | None:
    """Pull member ids out of a group-members payload, or None."""
    if not isinstance(payload, list):
        return None
    ids: list[str] = []
    for row in payload:
        if isinstance(row, Mapping):
            identifier = row.get("id")
            if isinstance(identifier, str) and identifier:
                ids.append(identifier)
                continue
        if isinstance(row, str) and row:
            ids.append(row)
            continue
        return None
    return tuple(ids)


def _membership_via_http(
    user_id: str, group: str, *, api_base: str, api_key: str, timeout: float = 15.0
) -> tuple[str, str]:
    """Read membership from the instance's own admin Groups API.

    The fallback for a deployment where the Pipe cannot import Open
    WebUI's models -- which, MEASURED at v0.11.0, is every deployment:
    the in-process API is async and this Pipe is sync.

    `GET /api/v1/groups/` is the entry point, and at v0.11.0 its rows are
    `GroupResponse`, which carries `member_count` but NOT `user_ids`
    (`routers/groups.py`; only `GroupExportResponse` on
    `/id/{id}/export` carries them). Reading `user_ids` off the listing
    therefore always came back absent, so this transport could never
    answer MEMBER either -- both transports inert, and the gate could
    admit nobody but an instance admin. That is the inert-Pipe defect one
    layer deeper, so membership is resolved from the SAME Groups API's
    member-bearing endpoints when the listing does not carry the ids.

    MEASURED HAZARD, same instance: Open WebUI calls a SYNCHRONOUS `pipe()`
    DIRECTLY inside its event loop (`functions.py:155-158` -- there is no
    threadpool hop), so a blocking HTTP call back to the Pipe's OWN
    instance can never be served while it waits. Measured live at the
    pinned version: this lookup against the instance's own base URL hung
    for the full 15s timeout, while the same URL answered a shell `curl`
    in 2ms and an unroutable base failed in 0.02s. A sync Pipe therefore
    cannot resolve membership over its own HTTP API at all; the refusal
    below is honest but it can only ever REFUSE, never admit a non-admin
    group member. Admitting one requires an ASYNC `pipe()` that awaits the
    in-process Groups API -- a requirement-level change, recorded rather
    than improvised here.

    Any failure at any hop is UNDETERMINED; nothing here can allow.
    """
    if not api_base.strip() or not api_key.strip():
        return UNDETERMINED, (
            "Valves `openwebui_api_base`/`openwebui_api_key` are unset, so no "
            "server-side membership lookup is configured"
        )
    base = api_base.rstrip("/")
    url = base + "/api/v1/groups/"
    payload, error = _fetch_json(url, api_key=api_key, timeout=timeout)
    if error:
        return UNDETERMINED, error
    if not isinstance(payload, list):
        return UNDETERMINED, f"`GET {url}` did not return a group list"
    for row in payload:
        if not isinstance(row, Mapping):
            continue
        if group not in {row.get("name"), row.get("id")}:
            continue
        members = row.get("user_ids")
        if isinstance(members, Sequence) and not isinstance(members, (str, bytes)):
            # A version that supplies the ids on the listing is believed.
            state = MEMBER if user_id in members else NOT_MEMBER
            return state, f"admin API lookup at {url}"
        group_id = row.get("id")
        if not isinstance(group_id, str) or not group_id:
            return UNDETERMINED, (
                f"group `{group}` carries neither `user_ids` nor an `id` to "
                "resolve its members with"
            )
        # The listing does not carry the ids at this version, so ask the
        # endpoints that do. Both belong to the same Groups API.
        reasons: list[str] = []
        for member_url, method in (
            (f"{base}/api/v1/groups/id/{group_id}/export", "GET"),
            (f"{base}/api/v1/groups/id/{group_id}/users", "POST"),
        ):
            body, member_error = _fetch_json(
                member_url, api_key=api_key, method=method, timeout=timeout
            )
            if member_error:
                reasons.append(member_error)
                continue
            ids: tuple[str, ...] | None = None
            if isinstance(body, Mapping):
                ids = _member_ids_from_rows(body.get("user_ids"))
            else:
                ids = _member_ids_from_rows(body)
            if ids is None:
                reasons.append(
                    f"`{method} {member_url}` carried no readable member ids"
                )
                continue
            state = MEMBER if user_id in ids else NOT_MEMBER
            return state, f"admin API member lookup at {member_url}"
        return UNDETERMINED, (
            f"group `{group}` was found, but its membership could not be "
            f"measured: {'; '.join(reasons)}"
        )
    return UNDETERMINED, f"group `{group}` was not found at {url}"


def resolve_group_membership(
    user_id: str,
    group: str,
    *,
    api_base: str = "",
    api_key: str = "",
) -> tuple[str, str]:
    """MEMBER / NOT_MEMBER / UNDETERMINED, with the reason.

    Tries the in-process API first, then the admin HTTP API. These are two
    transports for ONE measurement, not a value fallback: if neither can
    answer, the result is UNDETERMINED and the caller refuses. No failure
    path here can return MEMBER.
    """
    state, reason = _membership_via_open_webui(user_id, group)
    if state != UNDETERMINED:
        return state, reason
    http_state, http_reason = _membership_via_http(
        user_id, group, api_base=api_base, api_key=api_key
    )
    if http_state != UNDETERMINED:
        return http_state, http_reason
    return UNDETERMINED, f"{reason}; {http_reason}"


# --------------------------------------------------------------------------
# Decision intents. The grammar yields typed values only: a half, an
# integer index into rows this Pipe listed, and a member of a fixed
# vocabulary. No substring of the message ever reaches argv.
# --------------------------------------------------------------------------


@dataclass(frozen=True)
class DecisionIntent:
    half: str  # "pact" | "admission" | "external"
    index: int  # 1-based, into the rendered rows of that half
    verdict: str  # a member of the matching source's fixed vocabulary


def parse_decision_intent(message: str) -> DecisionIntent | None:
    """Parse `decide P3 approve`, `decide A1 accept`, or `decide E1 reject`.

    Returns typed values or nothing. A message that does not match the
    grammar exactly is not a decision -- it is a question, and questions
    go to the read-only `--query` slot.
    """
    tokens = (message or "").strip().split()
    if len(tokens) != 3 or tokens[0].lower() != "decide":
        return None
    handle = tokens[1].upper()
    if len(handle) < 2 or handle[0] not in {"P", "A", "E"}:
        return None
    try:
        index = int(handle[1:])
    except ValueError:
        return None
    if index < 1:
        return None
    verdict = tokens[2].lower()
    if handle[0] == "P":
        if verdict not in PACT_DECIDABLE_VERDICTS:
            return None
        return DecisionIntent(half="pact", index=index, verdict=verdict)
    if handle[0] == "E":
        if verdict not in EXTERNAL_ACTION_INBOX_DECISIONS:
            return None
        return DecisionIntent(half="external", index=index, verdict=verdict)
    if verdict not in ADMISSION_DECISIONS:
        return None
    return DecisionIntent(half="admission", index=index, verdict=verdict)


# --------------------------------------------------------------------------
# Rendering
# --------------------------------------------------------------------------

COVERAGE_STATEMENT = (
    "**Coverage.** This inbox covers three kinds of pending decision: "
    "**PACT review items** and **pending held admission envelopes** "
    "(every admission mode that waits for `okg admission decide`; "
    f"identified by deployment and `{ADMISSION_HELD_FLAG}` only), plus **external-action review "
    "proposals** when their authenticated API is configured. **Workflow escalations are not "
    "covered** -- they belong to deployment workflows and are out of scope "
    "for this surface."
)

_NOT_THE_WHOLE_INBOX = (
    "**This is not the whole inbox.** At least one source did not answer, so "
    "what follows is a partial view and must not be read as an all-clear."
)


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def resolve_commit_sha(
    repo_root: str,
    *,
    runner: Callable[[Sequence[str]], CommandResult] | None = None,
) -> tuple[str, str]:
    """The HEAD sha of the checkout the PACT half walked.

    Returns (sha, error). The sha is the *authority* for a verified-empty
    PACT statement, so failing to resolve it is a refusal, not a blank:
    "verified empty against pact/changes" with no commit named would be
    an unprovenanced claim.
    """
    argv = ["git", "-C", repo_root, "rev-parse", "HEAD"]
    try:
        if runner is not None:
            result = runner(argv)
        else:
            completed = subprocess.run(  # noqa: S603 - argv list, shell=False
                argv, capture_output=True, text=True, timeout=30.0, shell=False
            )
            result = CommandResult(
                argv=tuple(argv),
                returncode=completed.returncode,
                stdout=completed.stdout,
                stderr=completed.stderr,
            )
    except (OSError, subprocess.SubprocessError) as err:
        return "", f"`{' '.join(argv)}` could not run: {err}"
    if result.returncode != 0:
        return "", (
            f"`{' '.join(argv)}` exited {result.returncode}: "
            f"{_short(result.stderr)}"
        )
    sha = result.stdout.strip()
    if not sha:
        return "", f"`{' '.join(argv)}` produced no commit sha"
    return sha, ""


def pact_row_handle(row: Mapping[str, Any], fallback: int) -> int:
    """The `P<n>` handle for a RANK-LIST row.

    The rank list is the referent a decision resolves against
    (`pact_rows_by_handle`), so its positions are authoritative and a row
    the packet did not number still gets its position here.

    This is NOT safe for `--query` section rows -- see
    `packet_row_handle`, which returns None for an unnumbered row rather
    than inventing a position that collides with the rank list.
    """
    number = row.get("number")
    return number if isinstance(number, int) and number > 0 else fallback


def packet_row_handle(row: Mapping[str, Any]) -> int | None:
    """The handle for a `--query` SECTION row, or None if it has none.

    A section row is only decidable when the packet NUMBERED it, because
    a decision turn resolves `P<n>` against the RANK LIST rather than
    against the sections it just rendered.

    Not every section row carries a number: `_chat_stale_queue_row`
    (`src/okg/substrate/pact/cli.py`) emits rows with neither `number`
    nor `decision_type`, and those rows appear in the stale packet and in
    the batch-low-risk packet's excluded examples. Numbering them by
    their position within a section produced TWO different rows labelled
    `P2` in one response, and the operator's next turn -- `decide P2
    approve` -- resolves against the rank list, i.e. a DIFFERENT change,
    with the change id only named in the outcome text after the write
    landed. So an unnumbered row is rendered without a handle and is not
    offered as decidable.
    """
    number = row.get("number")
    return number if isinstance(number, int) and number > 0 else None


def _packet_sections(outcome: SourceOutcome) -> list[Mapping[str, Any]]:
    packet = outcome.packet or {}
    return [
        section
        for section in (packet.get("sections") or [])
        if isinstance(section, Mapping)
    ]


def pact_rows_by_handle(outcome: SourceOutcome) -> dict[int, dict[str, Any]]:
    """The PACT rows this Pipe listed, keyed by rendered handle.

    A decision resolves its change id through here, so the identifier
    always comes from a row the Pipe itself just listed -- never from the
    message.

    This maps the packet's rank list, which is exactly what a decision
    turn renders: a decision message does not parse as a question, so it
    is never sent to `--query` and the response is never the answer-first
    form. The handle the operator typed and the row decided therefore
    appear in the same response
    (test_a_decide_turn_renders_the_row_it_decides).
    """
    resolved: dict[int, dict[str, Any]] = {}
    for position, row in enumerate(outcome.rows or (), start=1):
        resolved[pact_row_handle(row, position)] = dict(row)
    return resolved


def _render_pact_row(row: Mapping[str, Any], *, handle: int | None) -> str:
    change = str(row.get("change") or "")
    title = str(row.get("title") or "")
    decision_type = str(row.get("decision_type") or "")
    # An unnumbered row carries no handle at all rather than a position
    # that would collide with the rank list a decision resolves against.
    label = f"**P{handle} `{change}`**" if handle is not None else f"**`{change}`**"
    try:
        action = row_action(decision_type)
    except UnmappedDecisionType as err:
        return f"- {label} — {title} — **no action mapped**: {err}"
    note = str(row.get("caveat") or row.get("why") or "")
    rendered = _render_row_action(action, change=change, note=note)
    marker = "run from here" if action.kind == "executable" else "run yourself"
    line = f"- {label} — {title}  \n  _{decision_type}_ · `{rendered}` ({marker}) · {action.explanation}"
    if handle is None:
        line += (
            "  \n  _This row is not numbered by the queue, so it has no "
            "`P<n>` handle and cannot be decided from here; run the command "
            "yourself._"
        )
    return line


def render_pact_section(
    outcome: SourceOutcome,
    *,
    commit_sha: str = "",
    sha_error: str = "",
    answer_first: bool = False,
) -> list[str]:
    lines = ["### PACT review items"]
    if outcome.state == UNAVAILABLE:
        lines += [
            "",
            "**Unavailable -- refusing to report a count.** The PACT review "
            f"queue did not answer: {outcome.reason}.",
            "",
            f"Failed command: `{outcome.command}`",
            "",
            "No number of pending PACT items is stated here, because none is "
            + "known.",
        ]
        return lines
    if outcome.state == VERIFIED_EMPTY:
        if not commit_sha:
            lines += [
                "",
                "**Unavailable -- refusing to claim an empty queue.** The "
                "queue returned zero rows, but the emptiness could not be "
                f"provenanced: {sha_error}.",
                "",
                f"Command: `{outcome.command}`",
            ]
            return lines
        lines += [
            "",
            f"**PACT queue: verified empty** against `pact/changes` at commit "
            f"`{commit_sha}` with the decision ledger reachable (the queue "
            "packet carried no warnings).",
            "",
            f"Command: `{outcome.command}`",
        ]
        return lines

    rows = outcome.rows or ()
    packet = outcome.packet or {}
    # No default-value-as-fallback here. Substituting len(rows) for a
    # missing count would manufacture a total nobody reported -- the
    # shown-as-total defect this surface refuses everywhere else, and the
    # admission half already refuses the matching case. classify_pact_result
    # guarantees both are ints on this path, so reaching the refusal means
    # a caller built the outcome by hand.
    shown, total = outcome.shown, outcome.total
    if shown is None or total is None:
        lines += [
            "",
            "**Unavailable -- refusing to report a count.** The PACT queue "
            + "outcome carried no shown/total counts, so no number of waiting "
            + "items is stated here.",
            "",
            f"Command: `{outcome.command}`",
        ]
        return lines
    header = f"{total} waiting."
    if shown < total:
        header = f"Showing **{shown} of {total}** waiting items."

    if answer_first:
        # The operator asked a question, so the CLI's own answer-first
        # inbox summary leads and its sections replace the rank list.
        interpreted = str(packet.get("interpreted_as") or "").strip()
        answer = str(packet.get("answer") or "").strip()
        lines += [""]
        if interpreted:
            lines.append(f"_Read as: {interpreted}._")
            lines.append("")
        if answer:
            lines += [answer, ""]
        for line in packet.get("summary_lines") or []:
            lines.append(f"- {line}")
        sections = _packet_sections(outcome)
        if sections:
            rendered_rows = 0
            for section in sections:
                lines += ["", f"**{section.get('title') or 'Items'}**", ""]
                section_rows = [
                    row
                    for row in (section.get("rows") or [])
                    if isinstance(row, Mapping)
                ]
                if not section_rows:
                    lines.append("- none")
                    continue
                for row in section_rows:
                    rendered_rows += 1
                    # packet_row_handle, NOT a positional fallback: an
                    # unnumbered section row must not borrow a number the
                    # rank list already means something else by.
                    lines.append(
                        _render_pact_row(row, handle=packet_row_handle(row))
                    )
            # Say plainly how many rows this ANSWER shows against how many
            # the whole queue holds -- "20 of 28" above 5 rows would read
            # as a count of what is on screen.
            lines += [
                "",
                f"({rendered_rows} shown for this question; the full queue "
                f"holds {total} waiting items.)",
            ]
            return lines
        lines += ["", header, ""]
    else:
        lines += ["", header, ""]

    for position, row in enumerate(rows, start=1):
        lines.append(_render_pact_row(row, handle=pact_row_handle(row, position)))
    return lines


def render_admission_section(outcome: SourceOutcome, *, deployment: str) -> list[str]:
    lines = ["### Pending held admission envelopes"]
    if outcome.state == UNAVAILABLE:
        lines += [
            "",
            "**Unavailable -- refusing to report a count.** The admission "
            f"source did not answer: {outcome.reason}.",
            "",
            f"Failed command: `{outcome.command}`",
            "",
            "No number of pending envelopes is stated here, because none is "
            + "known.",
        ]
        return lines
    if outcome.state == VERIFIED_EMPTY:
        # Deployment, mode, and the query timestamp -- and NOTHING else.
        # The predicate excluded by the module docstring's first contract
        # would, if named here, assert the emptiness of a referent that no
        # MCP-created envelope ever occupies.
        lines += [
            "",
            f"**Admission: verified empty** for deployment `{deployment}`, "
            f"`{ADMISSION_HELD_FLAG}`, at "
            f"{outcome.emptiness_authority}.",
            "",
            f"Command: `{outcome.command}`",
        ]
        return lines

    rows = outcome.rows or ()
    shown = outcome.shown if outcome.shown is not None else len(rows)
    if outcome.truncated or outcome.total is None:
        header = (
            f"Showing **{shown}**, and the read hit its `--limit "
            f"{ADMISSION_LIST_LIMIT}` ceiling — **the total is not known**. "
            "This is not stated as a count."
        )
    elif shown < outcome.total:
        header = f"Showing **{shown} of {outcome.total}** pending envelopes."
    else:
        header = f"{outcome.total} pending."
    lines += ["", header, ""]
    for position, row in enumerate(rows, start=1):
        admission_id = str(row.get("admission_id") or "")
        producer = str(row.get("producer_id") or "")
        intent = str(row.get("intent") or "")
        created = str(row.get("created_at") or "")
        lines.append(
            f"- **A{position} `{admission_id}`** — intent `{intent}`, producer "
            f"`{producer}`, created {created}  \n"
            f"  `{admission_row_action(admission_id, deployment)}` (run from here)"
        )
    return lines


def _safe_external_scalar(value: Any, *, default: str = "unknown") -> str:
    """Render only bounded scalar metadata from an external projection."""
    if value is None or isinstance(value, (Mapping, list, tuple, set)):
        return default
    text = str(value).replace("`", "'").replace("\r", " ").replace("\n", " ").strip()
    return text[:240] if text else default


def _safe_external_digests(value: Any) -> str:
    if not isinstance(value, (list, tuple)):
        return "none"
    digests = [
        _safe_external_scalar(item, default="")
        for item in value[:32]
        if isinstance(item, str) and item.strip()
    ]
    return ", ".join(digests) if digests else "none"


def _external_timeline_for(
    outcome: SourceOutcome, proposal_id: str
) -> tuple[dict[str, Any], ...]:
    for identifier, events in outcome.timelines:
        if identifier == proposal_id:
            return events
    return ()


def _external_latest_outcome(events: Sequence[Mapping[str, Any]]) -> str:
    for event in reversed(events):
        for field in ("outcome", "outcome_reason_code"):
            value = event.get(field)
            if isinstance(value, str) and value:
                return _safe_external_scalar(value)
    return "not reported"


def _render_external_timeline(events: Sequence[Mapping[str, Any]]) -> list[str]:
    lines = ["  Timeline:"]
    for event in events:
        sequence = _safe_external_scalar(event.get("sequence"), default="?")
        lifecycle = event.get("lifecycle_version")
        event_type = _safe_external_scalar(
            event.get("event_type") or event.get("type"), default="unknown event"
        )
        transition = ""
        before = event.get("before_review_state") or event.get("before_state")
        after = event.get("after_review_state") or event.get("after_state")
        if before is not None or after is not None:
            transition = (
                f"; state {_safe_external_scalar(before)} -> "
                f"{_safe_external_scalar(after)}"
            )
        identifiers = []
        for field, label in (
            ("decision_id", "decision"),
            ("execution_id", "execution"),
        ):
            value = event.get(field)
            if isinstance(value, str) and value:
                identifiers.append(f"{label} `{_safe_external_scalar(value)}`")
        hashes = []
        for field, label in (
            ("payload_hash", "payload hash"),
            ("event_hash", "event hash"),
            ("previous_event_hash", "previous hash"),
        ):
            value = event.get(field)
            if isinstance(value, str) and value:
                hashes.append(f"{label} `{_safe_external_scalar(value)}`")
        detail = f"seq {sequence}"
        if lifecycle is not None:
            detail += f", lifecycle {_safe_external_scalar(lifecycle)}"
        detail += f": `{event_type}`{transition}"
        if identifiers:
            detail += "; " + ", ".join(identifiers)
        if hashes:
            detail += "; " + ", ".join(hashes)
        outcome = event.get("outcome") or event.get("outcome_reason_code")
        if isinstance(outcome, str) and outcome:
            detail += f"; outcome `{_safe_external_scalar(outcome)}`"
        lines.append(f"  - {detail}")
    return lines


def render_external_action_section(
    outcome: SourceOutcome, *, deployment: str
) -> list[str]:
    """Render the third source using only the API's safe projection fields."""
    lines = ["### External-action review proposals"]
    if outcome.state == UNAVAILABLE:
        lines += [
            "",
            "**Unavailable -- refusing to report a count or offer decisions.** "
            f"The external-action source did not answer: {outcome.reason}.",
            "",
            f"Failed command: `{outcome.command}`",
            "",
            "No external-action proposal count is stated here, because the "
            "complete review set is unknown.",
        ]
        return lines
    if outcome.state == VERIFIED_EMPTY:
        lines += [
            "",
            f"**External-action queue: verified empty** for deployment "
            f"`{_safe_external_scalar(deployment)}` at "
            f"{_safe_external_scalar(outcome.emptiness_authority)}.",
            "",
            f"Command: `{outcome.command}`",
        ]
        return lines

    rows = outcome.rows or ()
    if outcome.shown is None or outcome.total is None:
        lines += [
            "",
            "**Unavailable -- refusing to report a count.** The external-action "
            "projection carried no complete count.",
            "",
            f"Command: `{outcome.command}`",
        ]
        return lines
    header = (
        f"{outcome.total} waiting external actions."
        if outcome.shown == outcome.total
        else f"Showing **{outcome.shown} of {outcome.total}** waiting external actions."
    )
    lines += ["", header, ""]
    for position, row in enumerate(rows, start=1):
        proposal_id = _safe_external_scalar(row.get("proposal_id"))
        action_type = _safe_external_scalar(row.get("action_type"))
        proposer = _safe_external_scalar(
            row.get("proposer_principal") or row.get("proposer")
        )
        automation = _safe_external_scalar(
            row.get("automation_id") or row.get("automation")
        )
        run = _safe_external_scalar(row.get("run_id") or row.get("run"))
        target = _safe_external_scalar(row.get("target_ref") or row.get("target"))
        risk = _safe_external_scalar(row.get("risk_tier") or row.get("risk"))
        submitted = _safe_external_scalar(
            row.get("created_at") or row.get("submitted_at")
        )
        expiry = _safe_external_scalar(
            row.get("effective_review_expires_at")
            or row.get("requested_review_expires_at")
        )
        lines.append(
            f"- **E{position} `{proposal_id}`** — `{action_type}`; review "
            f"state `{_safe_external_scalar(row.get('review_state'))}`, "
            f"execution `{_safe_external_scalar(row.get('execution_state'))}`"
        )
        lines.append(
            f"  Proposer `{proposer}`; automation/run `{automation}` / `{run}`; "
            f"target `{target}`; risk `{risk}`; submitted {submitted}; expires {expiry}."
        )
        lines.append(
            f"  Hashes: original `{_safe_external_scalar(row.get('original_payload_hash'))}`; "
            f"current `{_safe_external_scalar(row.get('current_payload_hash'))}`."
        )
        lines.append(
            f"  Difference: summary code `{_safe_external_scalar(row.get('summary_code'))}`; "
            f"evidence digests `{_safe_external_digests(row.get('evidence_digests'))}`."
        )
        lines.append(
            f"  Decision `{_safe_external_scalar(row.get('decision_id'), default='none')}`; "
            f"binding `{_safe_external_scalar(row.get('decision_binding_hash'), default='none')}`; "
            f"ETag `{_safe_external_scalar(row.get('etag'))}`."
        )
        events = _external_timeline_for(outcome, str(row.get("proposal_id") or ""))
        lines.append(
            f"  Outcome: `{_external_latest_outcome(events)}`; "
            f"inbox decisions: `{'|'.join(EXTERNAL_ACTION_INBOX_DECISIONS)}`."
        )
        lines.extend(_render_external_timeline(events))
    return lines


def decision_blockers(
    *,
    preconditions: Sequence[Precondition],
    pact: SourceOutcome | None,
    admission: SourceOutcome | None,
    external: SourceOutcome | None = None,
) -> list[str]:
    """Every reason decision verbs are withheld this turn, or an empty list.

    ONE predicate with TWO callers: `render_decision_availability` prints
    it and `Pipe.execute_decision` gates on it. They were previously
    separate expressions that disagreed -- the renderer withheld the verbs
    when EITHER source failed while the executor gated on only the half
    being decided -- so a `decide A<n>` turn with the PACT half down
    rendered "Decisions — withheld" AND ran `okg admission decide`. A write
    landed while the surface said it had refused.

    Both halves block both halves, and that is deliberate rather than
    incidental: an operator decides a row from a RANKED LIST, and a list
    assembled from one working source is not the list the ranking claims.
    Deciding `A1` against a response whose PACT half refused means acting
    on a partial view that the same response labelled partial.
    """
    blockers = [
        f"`{check.name}`: {check.detail}"
        for check in preconditions
        if not check.met
    ]
    # Report the MEASURED reason each source gave, not a presumed cause.
    # "the DSN did not answer" would be an unmeasured claim when the real
    # failure was malformed JSON or a degradation warning.
    sources = [(pact, "PACT queue"), (admission, "admission")]
    if external is not None:
        sources.append((external, "external-action review"))
    for outcome, label in sources:
        if outcome is not None and not outcome.available:
            blockers.append(
                f"the {label} source did not answer: {outcome.reason}"
            )
    return blockers


def render_decision_availability(
    *,
    preconditions: Sequence[Precondition],
    pact: SourceOutcome | None,
    admission: SourceOutcome | None,
    external: SourceOutcome | None = None,
) -> list[str]:
    """State plainly whether decision verbs are offered, and why not."""
    blockers = decision_blockers(
        preconditions=preconditions,
        pact=pact,
        admission=admission,
        external=external,
    )
    if not blockers:
        external_instruction = ""
        if external is not None:
            external_instruction = (
                " or `decide E<n> "
                f"<{ '|'.join(EXTERNAL_ACTION_INBOX_DECISIONS) }>`"
            )
        return [
            "### Decisions",
            "",
            "Decision verbs are available. Reply `decide P<n> "
            + f"<{'|'.join(PACT_DECIDABLE_VERDICTS)}>` or `decide A<n> "
            + f"<{'|'.join(ADMISSION_DECISIONS)}>`; the outcome is read back "
            + f"from the CLI, never assumed{external_instruction}.",
            "",
            "`supersede` is not offered here: it requires `--target <id>`, "
            + "which this grammar has no slot for. Run it yourself.",
        ]
    lines = [
        "### Decisions — withheld",
        "",
        "**Decision verbs are not offered.** Until `mcp-http-auth` lands the "
        + "only honest transport for an OKG write is local CLI passthrough on "
        + "the deployment's own host, and that precondition is not met here. "
        + "This is a refusal, not a read-only mode: nothing is queued for "
        + "later.",
        "",
        "Unmet preconditions:",
    ]
    lines += [f"- {blocker}" for blocker in blockers]
    return lines


# --------------------------------------------------------------------------
# The Pipe
# --------------------------------------------------------------------------


class Pipe:
    """The review inbox, registered as a model in the Open WebUI picker."""

    class Valves(BaseModel):
        deployment: str = Field(
            default="",
            description=(
                "OKG deployment name. Unset is UNMET -- the Pipe refuses "
                "rather than guessing."
            ),
        )
        okg_cli: str = Field(
            default="",
            description=(
                "Absolute path to the okg executable INSIDE the Open WebUI "
                "process. Containerized instances must mount it in."
            ),
        )
        repo_root: str = Field(
            default="",
            description=(
                "Absolute path to an OKG checkout containing pact/changes, "
                "passed as `okg pact --root`. The Open WebUI process's cwd is "
                "not the OKG repo, so this is load-bearing."
            ),
        )
        admin_group: str = Field(
            default="",
            description=(
                "The deployment's admin group. An instance admin role is "
                "accepted directly; a non-admin member is resolved by a "
                "SERVER-SIDE lookup (v0.11.0 does not put `groups` in the "
                "__user__ dict). Unset is UNMET."
            ),
        )
        openwebui_api_base: str = Field(
            default="",
            description=(
                "Base URL of this Open WebUI instance, e.g. "
                "http://localhost:8080. Used only for the admin-group "
                "membership lookup, and only when the in-process group API "
                "is unavailable. PROJECTED by `okg chat sync` from the "
                "deployment's chat.deep_links.chat_site_base, so a "
                "hand-typed value cannot silently disagree with the "
                "manifest; setting it by hand still works on an instance "
                "that is not synced."
            ),
        )
        openwebui_api_key: str = Field(
            default="",
            description=(
                "Admin API key for the membership lookup above. Unset means "
                "the HTTP lookup is unavailable, which is UNDETERMINED (a "
                "refusal), never an allow."
            ),
        )
        external_action_api_url: str = Field(
            default="",
            description=(
                "Base URL for the standalone external-action review API. "
                "When unset, this third source is not queried; the CLI may "
                "also resolve OKG_EXTERNAL_ACTION_API_URL in its process."
            ),
        )
        external_action_authorization_env: str = Field(
            default="OKG_EXTERNAL_ACTION_AUTHORIZATION",
            description="Environment variable name for the API Authorization value.",
        )
        external_action_dpop_env: str = Field(
            default="OKG_EXTERNAL_ACTION_DPOP",
            description="Environment variable name for the API DPoP proof.",
        )
        external_action_client_cert: str = Field(
            default="", description="Optional client-certificate path for mTLS."
        )
        external_action_client_key: str = Field(
            default="", description="Optional client-key path for mTLS."
        )
        external_action_ca_bundle: str = Field(
            default="", description="Optional CA bundle path for mTLS."
        )
        external_action_decision_reason_code: str = Field(
            default="",
            description=(
                "Bounded reason code used for review decisions. An unset code "
                "refuses to send a decision."
            ),
        )
        external_action_execute_before: str = Field(
            default="",
            description=(
                "RFC3339 execution deadline used only for approve decisions. "
                "The API still enforces its action-contract window; an unset "
                "or invalid deadline refuses approval."
            ),
        )
        # There is deliberately NO Valve for the predicate the module
        # docstring's first contract excludes. The pending set is
        # identified by deployment and mode only; that contract carries
        # the measurements behind it.
        command_timeout_seconds: float = Field(default=120.0)

    def __init__(self) -> None:
        self.type = "manifold"
        self.id = MODEL_ID
        self.name = MODEL_NAME
        self.valves = self.Valves()
        # Every argv this instance ran, in order. The precondition-unmet
        # proof is an EMPTY log, not the absence of an error message.
        self.command_log: list[tuple[str, ...]] = []
        self._runner: Callable[[Sequence[str]], CommandResult] = self._run
        # Seam for hermetic tests: the production probe hits the real
        # filesystem, which is the point in a real instance.
        self._host_probe: Callable[..., list[Precondition]] = (
            check_host_preconditions
        )
        self._sha_resolver: Callable[[str], tuple[str, str]] = resolve_commit_sha
        self._membership_resolver: Callable[..., tuple[str, str]] = (
            resolve_group_membership
        )

    # -- plumbing ---------------------------------------------------------

    def pipes(self) -> list[dict[str, str]]:
        return [{"id": MODEL_ID, "name": MODEL_NAME}]

    def _run(self, argv: Sequence[str]) -> CommandResult:
        return run_okg(argv, timeout=self.valves.command_timeout_seconds)

    def _run_logged(self, argv: Sequence[str]) -> CommandResult:
        """Every okg invocation goes through here, so `command_log` is the
        complete record. The precondition-unmet proof is an EMPTY log."""
        self.command_log.append(tuple(argv))
        return self._runner(argv)

    # -- gates ------------------------------------------------------------

    def admin_refusal(self, user: Any) -> str | None:
        """Assert the caller's authority. Fail-closed, but on real signals.

        This is the ENFORCING layer. Open WebUI's per-model access control
        is a second, independent layer that hides the model from non-admin
        pickers -- measured working at v0.11.0, but a model row with a
        group grant must exist for it to apply at all (see the probe's
        incidental finding), so it is defence in depth, never the gate.

        Keyed on what v0.11.0 ACTUALLY supplies (probe evidence at
        pact/changes/openwebui-chat-frontend/evidence/pipe-user-context-probe.md):

        1. `role == "admin"` -- supplied, a plain str, and authoritative.
        2. an explicit `groups` field -- NOT supplied at v0.11.0, kept as a
           fast path in case a later version adds it. Its ABSENCE no longer
           refuses; that was the inert-Pipe defect.
        3. a server-side membership lookup keyed on the supplied `id`.

        A lookup that cannot answer is UNDETERMINED and refuses while
        naming the missing capability. Nothing here allows on failure.
        """
        group = self.valves.admin_group.strip()
        if not group:
            return (
                "**Refused.** Valve `admin_group` is unset. An unset Valve is "
                "unmet, never assumed satisfied, so no caller can be "
                "authorized."
            )
        if not isinstance(user, Mapping):
            return (
                "**Refused.** No `__user__` context reached this Pipe, so the "
                "caller's authority could not be measured."
            )

        # 1. Instance admin role -- the signal the vendor does supply.
        if user.get("role") == "admin":
            return None

        # 2. Explicit groups, if a future version ever supplies them.
        groups = user.get("groups")
        if isinstance(groups, Sequence) and not isinstance(groups, (str, bytes)):
            for entry in groups:
                if isinstance(entry, str) and entry == group:
                    return None
                if isinstance(entry, Mapping) and group in {
                    entry.get("name"),
                    entry.get("id"),
                }:
                    return None

        # 3. Server-side membership, keyed on the supplied id.
        user_id = user.get("id")
        if not isinstance(user_id, str) or not user_id:
            return (
                "**Refused.** The `__user__` context carries no usable `id`, "
                f"so membership of admin group `{group}` could not be looked "
                "up server-side. An unmeasurable membership is a refusal, "
                "not a pass."
            )
        state, reason = self._membership_resolver(
            user_id,
            group,
            api_base=self.valves.openwebui_api_base,
            api_key=self.valves.openwebui_api_key,
        )
        if state == MEMBER:
            return None
        if state == NOT_MEMBER:
            return (
                f"**Refused.** This inbox is limited to the `{group}` admin "
                f"group. The caller is not a member ({reason})."
            )
        return (
            f"**Refused.** Membership of admin group `{group}` could not be "
            f"determined: {reason}. An undetermined membership is a refusal, "
            "not a pass."
        )

    # -- the request ------------------------------------------------------

    def pipe(self, body: Mapping[str, Any], __user__: Any = None, **_: Any) -> str:
        refusal = self.admin_refusal(__user__)
        if refusal is not None:
            return f"# Review inbox\n\n{refusal}\n"

        preconditions = self._host_probe(
            okg_cli=self.valves.okg_cli,
            repo_root=self.valves.repo_root,
            deployment=self.valves.deployment,
        )
        unmet = [check for check in preconditions if not check.met]
        if unmet:
            # The startup self-check. A host missing okg, the repo, or the
            # deployment name renders a typed refusal -- never an inbox that
            # merely looks empty.
            lines = [
                "# Review inbox — host preconditions unmet",
                "",
                "**Refused.** This Pipe executes inside the Open WebUI "
                + "process, and that process does not have what the inbox "
                + "needs. No source was read and no count is claimed.",
                "",
            ]
            for check in unmet:
                lines.append(f"- `{check.name}`: {check.detail}")
            lines += ["", COVERAGE_STATEMENT, ""]
            return "\n".join(lines)

        message = self._latest_user_message(body)
        intent = parse_decision_intent(message)
        query = None if intent is not None else (message.strip() or None)

        pact = self.read_pact_half(query=query)
        admission = self.read_admission_half()
        external = self.read_external_action_half() if self._external_action_enabled() else None

        commit_sha, sha_error = "", ""
        if pact.state == VERIFIED_EMPTY:
            # Logged like every other subprocess: `command_log` is the
            # complete record of what this turn executed, not just the okg
            # part of it.
            self.command_log.append(
                ("git", "-C", self.valves.repo_root, "rev-parse", "HEAD")
            )
            commit_sha, sha_error = self._sha_resolver(self.valves.repo_root)

        sections: list[str] = [
            f"# Review inbox — `{self.valves.deployment}`",
            "",
            COVERAGE_STATEMENT,
            "",
        ]
        if (
            not pact.available
            or not admission.available
            or (external is not None and not external.available)
        ):
            sections += [_NOT_THE_WHOLE_INBOX, ""]
        sections += render_pact_section(
            pact,
            commit_sha=commit_sha,
            sha_error=sha_error,
            answer_first=query is not None,
        )
        sections += [""]
        sections += render_admission_section(
            admission, deployment=self.valves.deployment
        )
        if external is not None:
            sections += [""]
            sections += render_external_action_section(
                external, deployment=self.valves.deployment
            )
        sections += [""]
        sections += render_decision_availability(
            preconditions=preconditions,
            pact=pact,
            admission=admission,
            external=external,
        )

        if intent is not None:
            sections += [""]
            sections += self.execute_decision(
                intent,
                preconditions=preconditions,
                pact=pact,
                admission=admission,
                external=external,
                answer_first=query is not None,
            )
        return "\n".join(sections) + "\n"

    @staticmethod
    def _latest_user_message(body: Mapping[str, Any]) -> str:
        messages = (body or {}).get("messages") or []
        for message in reversed(list(messages)):
            if isinstance(message, Mapping) and message.get("role") == "user":
                content = message.get("content")
                if isinstance(content, str):
                    return content
                if isinstance(content, list):
                    parts = [
                        str(part.get("text") or "")
                        for part in content
                        if isinstance(part, Mapping) and part.get("type") == "text"
                    ]
                    return " ".join(part for part in parts if part)
        return ""

    # -- reads ------------------------------------------------------------

    def read_pact_half(self, *, query: str | None = None) -> SourceOutcome:
        argv = build_pact_view_argv(
            self.valves.okg_cli, self.valves.repo_root, query=query
        )
        try:
            result = self._run_logged(argv)
        except (OSError, ValueError, subprocess.SubprocessError) as err:
            return SourceOutcome(
                source="pact",
                state=UNAVAILABLE,
                command=" ".join(argv),
                reason=f"the command could not run: {err}",
            )
        return classify_pact_result(result)

    def read_admission_half(self) -> SourceOutcome:
        argv = build_admission_list_argv(
            self.valves.okg_cli, self.valves.deployment
        )
        queried_at = _utc_now()
        try:
            result = self._run_logged(argv)
        except (OSError, ValueError, subprocess.SubprocessError) as err:
            return SourceOutcome(
                source="admission",
                state=UNAVAILABLE,
                command=" ".join(argv),
                reason=f"the command could not run: {err}",
            )
        return classify_admission_result(result, queried_at=queried_at)

    def _external_action_enabled(self) -> bool:
        """Whether the separately authenticated third source is configured."""
        return bool(
            self.valves.external_action_api_url.strip()
            or os.environ.get("OKG_EXTERNAL_ACTION_API_URL", "").strip()
        )

    def _external_action_options(self) -> dict[str, str]:
        return {
            "api_url": self.valves.external_action_api_url,
            "authorization_env": self.valves.external_action_authorization_env,
            "dpop_env": self.valves.external_action_dpop_env,
            "client_cert": self.valves.external_action_client_cert,
            "client_key": self.valves.external_action_client_key,
            "ca_bundle": self.valves.external_action_ca_bundle,
        }

    def read_external_action_half(self) -> SourceOutcome:
        """Read, re-read, and timeline-check the redacted external queue."""
        options = self._external_action_options()
        argv = build_external_action_list_argv(
            self.valves.okg_cli, self.valves.deployment, **options
        )
        queried_at = _utc_now()
        try:
            result = self._run_logged(argv)
        except (OSError, ValueError, subprocess.SubprocessError) as err:
            return SourceOutcome(
                source=EXTERNAL_ACTION_SOURCE,
                state=UNAVAILABLE,
                command=" ".join(argv),
                reason=f"the command could not run: {err}",
            )
        outcome = classify_external_action_result(result, queried_at=queried_at)
        if not outcome.available or not outcome.rows:
            return outcome

        normalized_rows: list[dict[str, Any]] = []
        timelines: list[tuple[str, tuple[dict[str, Any], ...]]] = []
        for listed in outcome.rows:
            proposal_id = listed["proposal_id"]
            listed_deployment = listed.get("deployment")
            if listed_deployment != self.valves.deployment:
                return SourceOutcome(
                    source=EXTERNAL_ACTION_SOURCE,
                    state=UNAVAILABLE,
                    command=outcome.command,
                    reason=(
                        f"proposal {proposal_id} was returned for deployment "
                        f"{listed_deployment!r}, not the configured deployment"
                    ),
                )

            show_argv = build_external_action_show_argv(
                self.valves.okg_cli,
                self.valves.deployment,
                proposal_id,
                **options,
            )
            try:
                show_result = self._run_logged(show_argv)
            except (OSError, ValueError, subprocess.SubprocessError) as err:
                return SourceOutcome(
                    source=EXTERNAL_ACTION_SOURCE,
                    state=UNAVAILABLE,
                    command=" ".join(show_argv),
                    reason=f"the command could not run: {err}",
                )
            shown, show_error = _external_projection_from_result(
                show_result,
                proposal_id=proposal_id,
                deployment=self.valves.deployment,
            )
            if shown is None:
                return SourceOutcome(
                    source=EXTERNAL_ACTION_SOURCE,
                    state=UNAVAILABLE,
                    command=show_result.display,
                    reason=show_error,
                )
            for field in _EXTERNAL_PROJECTION_FIELDS:
                if shown[field] != listed.get(field):
                    return SourceOutcome(
                        source=EXTERNAL_ACTION_SOURCE,
                        state=UNAVAILABLE,
                        command=show_result.display,
                        reason=(
                            f"list/show parity failed for proposal {proposal_id} "
                            f"at `{field}`"
                        ),
                    )

            timeline_argv = build_external_action_timeline_argv(
                self.valves.okg_cli,
                self.valves.deployment,
                proposal_id,
                **options,
            )
            try:
                timeline_result = self._run_logged(timeline_argv)
            except (OSError, ValueError, subprocess.SubprocessError) as err:
                return SourceOutcome(
                    source=EXTERNAL_ACTION_SOURCE,
                    state=UNAVAILABLE,
                    command=" ".join(timeline_argv),
                    reason=f"the command could not run: {err}",
                )
            events, original_hash, current_hash, timeline_error = _external_timeline_payload(
                timeline_result, proposal_id=proposal_id
            )
            if events is None:
                return SourceOutcome(
                    source=EXTERNAL_ACTION_SOURCE,
                    state=UNAVAILABLE,
                    command=timeline_result.display,
                    reason=timeline_error,
                )
            if original_hash is not None and original_hash != shown["original_payload_hash"]:
                return SourceOutcome(
                    source=EXTERNAL_ACTION_SOURCE,
                    state=UNAVAILABLE,
                    command=timeline_result.display,
                    reason=f"timeline hash parity failed for proposal {proposal_id}",
                )
            if current_hash is not None and current_hash != shown["current_payload_hash"]:
                return SourceOutcome(
                    source=EXTERNAL_ACTION_SOURCE,
                    state=UNAVAILABLE,
                    command=timeline_result.display,
                    reason=f"timeline hash parity failed for proposal {proposal_id}",
                )
            normalized_rows.append({**listed, **shown})
            timelines.append((proposal_id, events))

        return replace(
            outcome,
            rows=tuple(normalized_rows),
            packet={"proposals": tuple(normalized_rows)},
            timelines=tuple(timelines),
        )

    # -- writes -----------------------------------------------------------

    def execute_decision(
        self,
        intent: DecisionIntent,
        *,
        preconditions: Sequence[Precondition],
        pact: SourceOutcome,
        admission: SourceOutcome,
        external: SourceOutcome | None = None,
        answer_first: bool = False,
    ) -> list[str]:
        """Run one decision verb, then read the outcome back from the CLI."""
        # THE SAME predicate the surface rendered, not a second expression
        # that agrees with it by coincidence. Gating on only the addressed
        # half is what let a write land under a rendered refusal.
        blockers = decision_blockers(
            preconditions=preconditions,
            pact=pact,
            admission=admission,
            external=external,
        )
        if blockers:
            return [
                "### Decision not executed",
                "",
                "**Refused.** The transport precondition for writes is not "
                + "met, so no decide subprocess ran and nothing was queued "
                + "for later.",
                "",
                "Unmet preconditions:",
                *[f"- {blocker}" for blocker in blockers],
            ]
        if intent.half == "external":
            half = external
            if half is None:
                return [
                    "### Decision not executed",
                    "",
                    "**Refused.** External-action review is not configured for "
                    "this inbox.",
                ]
        else:
            half = pact if intent.half == "pact" else admission
        if answer_first:
            # Unreachable by construction: a decision message does not
            # parse as a question, so no `--query` is sent on this turn.
            # Stated as a refusal rather than trusted, because if it ever
            # became reachable the operator would be deciding a row the
            # response did not show them.
            return [
                "### Decision not executed",
                "",
                "**Refused.** A decision turn must render the ranked row "
                + "list it decides from; this turn did not.",
            ]
        try:
            if intent.half == "pact":
                row = pact_rows_by_handle(half).get(intent.index)
                if row is None:
                    return self._no_such_row(intent)
                return self._decide_pact(row, verdict=intent.verdict)
            rows = half.rows or ()
            if intent.index > len(rows):
                return self._no_such_row(intent)
            if intent.half == "external":
                return self._decide_external(
                    rows[intent.index - 1], decision=intent.verdict
                )
            return self._decide_admission(
                rows[intent.index - 1], decision=intent.verdict
            )
        except (OSError, ValueError, subprocess.SubprocessError) as err:
            # A decide that could not complete must say so in-surface,
            # not escape as a stack trace into the chat client.
            return [
                "### Decision outcome unknown",
                "",
                f"**The decision command did not complete: {err}.** Whether "
                "it took effect is unknown; read the state back with the CLI "
                "before retrying.",
            ]

    @staticmethod
    def _no_such_row(intent: DecisionIntent) -> list[str]:
        return [
            "### Decision not executed",
            "",
            f"**Refused.** There is no row {intent.half[0].upper()}"
            f"{intent.index} in the list above.",
        ]

    def _decide_external(
        self, row: Mapping[str, Any], *, decision: str
    ) -> list[str]:
        """Submit a redacted external decision and read back its state."""
        proposal_id = row.get("proposal_id")
        etag = row.get("etag")
        if not isinstance(proposal_id, str) or not proposal_id:
            return [
                "### Decision not executed",
                "",
                "**Refused.** The external row carried no usable proposal id.",
            ]
        if not isinstance(etag, str) or not etag:
            return [
                "### Decision not executed",
                "",
                f"**Refused.** Proposal `{_safe_external_scalar(proposal_id)}` "
                "carried no current ETag.",
            ]
        if decision == "approve_with_edit":
            return [
                "### Decision not executed",
                "",
                "**Refused.** `approve_with_edit` requires an authorized "
                "replacement payload, and the normal review inbox never reads "
                "or constructs one. Use the external-action detail client for "
                "that decision.",
            ]
        execute_before = None
        if decision == "approve":
            configured_deadline = self.valves.external_action_execute_before
            try:
                parsed_deadline = datetime.fromisoformat(
                    configured_deadline.replace("Z", "+00:00")
                )
                if parsed_deadline.tzinfo is None:
                    raise ValueError("deadline has no timezone")
                execute_before = parsed_deadline.astimezone(timezone.utc).isoformat(
                    timespec="microseconds"
                ).replace("+00:00", "Z")
            except (AttributeError, TypeError, ValueError):
                return [
                    "### Decision not executed",
                    "",
                    "**Refused.** Approval requires a valid RFC3339 "
                    "`external_action_execute_before` Valve; no authenticated "
                    "write was attempted.",
                ]
        reason_code = self.valves.external_action_decision_reason_code
        if not isinstance(reason_code, str) or not reason_code.strip():
            return [
                "### Decision not executed",
                "",
                "**Refused.** No bounded external-action decision reason code "
                "is configured; no authenticated write was attempted.",
            ]

        idempotency_key = f"okg-review-inbox-{uuid.uuid4().hex}"
        body_file = ""
        try:
            with tempfile.NamedTemporaryFile(
                mode="w", encoding="utf-8", suffix=".json", delete=False
            ) as handle:
                body_file = handle.name
                body = {"verdict": decision, "reason_code": reason_code.strip()}
                if execute_before is not None:
                    body["execute_before"] = execute_before
                json.dump(
                    body,
                    handle,
                    separators=(",", ":"),
                )
            argv = build_external_action_decide_argv(
                self.valves.okg_cli,
                self.valves.deployment,
                proposal_id,
                decision=decision,
                idempotency_key=idempotency_key,
                if_match=etag,
                body_file=body_file,
                **self._external_action_options(),
            )
            result = self._run_logged(argv)
        finally:
            if body_file:
                try:
                    os.unlink(body_file)
                except FileNotFoundError:
                    pass

        lines = ["### Decision outcome", "", f"Command: `{' '.join(argv)}`", ""]
        if result.returncode != 0:
            lines.append(
                f"**The external decision did not record.** The command exited "
                f"{result.returncode}: {_short(result.stderr)}"
            )
            return lines

        show_argv = build_external_action_show_argv(
            self.valves.okg_cli,
            self.valves.deployment,
            proposal_id,
            **self._external_action_options(),
        )
        try:
            readback = self._run_logged(show_argv)
        except (OSError, ValueError, subprocess.SubprocessError) as err:
            lines += [
                f"Readback: `{' '.join(show_argv)}`",
                "",
                "**Outcome unknown.** The decision command exited 0 but its "
                f"state could not be read back ({err}).",
            ]
            return lines
        shown, show_error = _external_projection_from_result(
            readback, proposal_id=proposal_id, deployment=self.valves.deployment
        )
        lines += [f"Readback: `{' '.join(show_argv)}`", ""]
        if shown is None:
            lines.append(
                "**Outcome unknown.** The decision command exited 0 but its "
                f"redacted state could not be read back ({show_error})."
            )
            return lines
        lines.append(
            f"Proposal `{_safe_external_scalar(proposal_id)}` is now review "
            f"state `{_safe_external_scalar(shown.get('review_state'))}`, "
            f"execution `{_safe_external_scalar(shown.get('execution_state'))}`; "
            f"original hash `{_safe_external_scalar(shown.get('original_payload_hash'))}`, "
            f"current hash `{_safe_external_scalar(shown.get('current_payload_hash'))}`."
        )
        return lines

    def _decide_pact(self, row: Mapping[str, Any], *, verdict: str) -> list[str]:
        change = str(row.get("change") or "")
        decision_type = str(row.get("decision_type") or "")
        try:
            action = row_action(decision_type)
        except UnmappedDecisionType as err:
            return ["### Decision not executed", "", f"**Refused.** {err}"]
        if action.kind != "executable":
            return [
                "### Decision not executed",
                "",
                f"**Refused.** `{decision_type}` rows are resolved by "
                f"`{_render_row_action(action, change=change)}`, which is "
                "outside this inbox's enumerated verb set. Run it yourself.",
            ]
        note = str(row.get("caveat") or "") if decision_type == "repair" else None
        argv = build_pact_decide_argv(
            self.valves.okg_cli,
            self.valves.repo_root,
            change=change,
            verdict=verdict,
            note=note or None,
        )
        result = self._run_logged(argv)
        lines = ["### Decision outcome", "", f"Command: `{' '.join(argv)}`", ""]
        if result.returncode != 0:
            lines.append(
                f"**The verdict did not record.** The command exited "
                f"{result.returncode}: {_short(result.stderr)}"
            )
            return lines
        payload, error = _parse_json(result)
        recorded = (payload or {}).get("recorded")
        if payload is None or not isinstance(recorded, Mapping):
            lines.append(
                "**Outcome unknown.** The command exited 0 but returned no "
                f"ledger entry to read back ({error or 'no `recorded` object'}), "
                "so this inbox will not claim the verdict landed."
            )
            return lines
        lines.append(
            f"Ledger entry `{recorded.get('decision_id')}`: `{change}` "
            f"{recorded.get('verdict')} "
            f"({recorded.get('from_state')} -> {recorded.get('to_state')}) "
            f"by {recorded.get('principal')}."
        )
        return lines

    def _decide_admission(
        self, row: Mapping[str, Any], *, decision: str
    ) -> list[str]:
        admission_id = str(row.get("admission_id") or "")
        argv = build_admission_decide_argv(
            self.valves.okg_cli,
            self.valves.deployment,
            admission_id=admission_id,
            decision=decision,
        )
        result = self._run_logged(argv)
        lines = ["### Decision outcome", "", f"Command: `{' '.join(argv)}`", ""]
        if result.returncode != 0:
            lines.append(
                f"**The decision did not record.** The command exited "
                f"{result.returncode}: {_short(result.stderr)}"
            )
            return lines
        show_argv = build_admission_show_argv(
            self.valves.okg_cli, self.valves.deployment, admission_id
        )
        readback = self._run_logged(show_argv)
        lines.append(f"Readback: `{' '.join(show_argv)}`")
        lines.append("")
        if readback.returncode != 0:
            lines.append(
                "**Outcome unknown.** The decision command exited 0 but the "
                f"envelope could not be read back (exit {readback.returncode}: "
                f"{_short(readback.stderr)}), so no post-decision status is "
                "claimed."
            )
            return lines
        payload, error = _parse_json(readback)
        envelope = (payload or {}).get("admission")
        if payload is None or not isinstance(envelope, Mapping):
            lines.append(
                "**Outcome unknown.** The readback returned no envelope "
                f"({error or 'no `admission` object'}), so no post-decision "
                "status is claimed."
            )
            return lines
        status = envelope.get("status")
        lines.append(
            f"Envelope `{admission_id}` status is now `{status}` "
            f"(decided_at {envelope.get('decided_at')})."
        )
        if status == "pending":
            lines.append("")
            lines.append(
                "**Still pending.** The envelope did not leave the pending "
                "state; the decision has not taken effect."
            )
        return lines
