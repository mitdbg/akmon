"""Parsing, guard-checking, and in-process execution of emitted commands.

A refusal only counts as self-repairing if its first recommended command is
something an agent can actually run: it must parse, fit the repo's command
guards, and execute. The guards are not decoration — the audit measured ~19%
of inline attach attempts blocked by them and 171 wrapper scripts written
around them, so a command that trips a guard is not a repair.

INSTRUMENT COVERAGE: the harness only implements a subset of the CLI's verb
surface in-process. A verb the CLI really has but the harness has not wired is
a HOLE IN THE INSTRUMENT, not a PACT defect, and must never be scored as one —
that is why :func:`coverage` separates ``harness_gap`` from
``unknown_pact_verb`` (the CLI recommended a verb that does not exist) and
``not_a_pact_command`` (the CLI recommended something that is not an `okg
pact` command at all). ``known_verbs`` is read off the REAL argparse tree, so
a verb added to the CLI shows up here as an uncovered gap rather than silently
scoring as a system defect.
"""
from __future__ import annotations

import argparse
import json
import shlex
from dataclasses import dataclass, field
from functools import lru_cache
from pathlib import Path
from typing import Any

from okg.substrate.pact.bench import calls
from okg.substrate.pact.bench.calls import CallResult, Recorder

MAX_COMMAND_CHARS = 500
MAX_STATEMENT_SEPARATORS = 2
STATEMENT_SEPARATORS = ("&&", "||", ";")

_PREFIXES = (["uv", "run", "okg", "pact"], ["okg", "pact"])
# Real `okg` commands the harness has no in-process wrapper for. A bootstrap
# packet legitimately recommends `okg dogfood status`; scoring that as "not a
# command" billed an instrument limit to PACT.
_NON_PACT_OKG_PREFIXES = (["uv", "run", "okg"], ["okg"])


@dataclass
class ParsedCommand:
    raw: str
    argv: list[str] = field(default_factory=list)
    verb: str | None = None
    change: str | None = None
    task: str | None = None
    options: dict[str, str] = field(default_factory=dict)
    parse_ok: bool = False
    reason: str | None = None

    @property
    def executable(self) -> bool:
        """The harness can make this exact call. See :func:`coverage`."""
        return coverage(self)[0] == EXECUTABLE


def count_separators(raw: str) -> int:
    return sum(raw.count(token) for token in STATEMENT_SEPARATORS)


def fits_guards(raw: str) -> bool:
    return (
        len(raw) <= MAX_COMMAND_CHARS
        and count_separators(raw) <= MAX_STATEMENT_SEPARATORS
    )


def parse(raw: str) -> ParsedCommand:
    """Parse an emitted `okg pact ...` command; never raise."""
    text = raw.strip()
    if "#" in text:
        # A trailing comment is not executable as emitted; the CLI appends one
        # to the deferred-closure command (cli.py:1008).
        text = text.split("#", 1)[0].strip()
    if not text:
        return ParsedCommand(raw=raw, reason="empty")
    try:
        argv = shlex.split(text)
    except ValueError as exc:
        return ParsedCommand(raw=raw, reason=f"unparseable: {exc}")
    for prefix in _PREFIXES:
        if argv[: len(prefix)] == prefix:
            rest = argv[len(prefix) :]
            break
    else:
        # An `okg <verb>` outside the pact surface (`okg dogfood status`, which
        # the blocked-change bootstrap really does recommend) is a COMMAND the
        # harness cannot run in-process, not prose. Distinguished here so
        # `coverage` can bill it to the instrument instead of to PACT — the
        # emitter's own funnel (`repair.is_executable_command`) accepts it.
        for prefix in _NON_PACT_OKG_PREFIXES:
            if argv[: len(prefix)] == prefix and len(argv) > len(prefix):
                return ParsedCommand(
                    raw=raw,
                    argv=argv,
                    reason=f"okg command outside the pact surface: {argv[len(prefix)]}",
                )
        return ParsedCommand(
            raw=raw, argv=argv, reason="not an `okg pact` command"
        )
    if not rest:
        return ParsedCommand(raw=raw, argv=argv, reason="no subcommand")
    positional: list[str] = []
    options: dict[str, str] = {}
    index = 0
    while index < len(rest):
        token = rest[index]
        if token.startswith("--"):
            if "=" in token:
                name, value = token[2:].split("=", 1)
                options[name] = value
            elif index + 1 < len(rest) and not rest[index + 1].startswith("--"):
                options[token[2:]] = rest[index + 1]
                index += 1
            else:
                options[token[2:]] = ""
        else:
            positional.append(token)
        index += 1
    verb = positional[0] if positional else None
    remainder = positional[1:]
    if verb in {"gate", "evidence"} and remainder:
        verb = f"{verb} {remainder[0]}"
        remainder = remainder[1:]
    change = remainder[0] if remainder else None
    task = remainder[1] if len(remainder) > 1 else None
    return ParsedCommand(
        raw=raw,
        argv=argv,
        verb=verb,
        change=change,
        task=task,
        options=options,
        parse_ok=True,
    )


EXECUTABLE_VERBS = frozenset(
    {
        "view",
        "decide",
        "evidence status",
        "evidence attach",
        "evidence sync",
        "evidence waive",
        "gate post-edit",
        "gate task-done",
    }
)

# `view` is one parser verb but many surfaces; the harness wires these formats.
EXECUTABLE_VIEW_FORMATS = frozenset(
    {"bootstrap", "brief", "markdown", "next", "status"}
)

# What the emitted command must carry for the harness to make the call at all.
# Positional names are the parser's; option names are the long flags.
REQUIRED_ARGUMENTS: dict[str, tuple[str, ...]] = {
    "view": ("change",),
    "decide": ("change", "--verdict"),
    "evidence status": ("change",),
    "evidence attach": ("change", "target", "--kind", "--ref"),
    # `change` is `nargs="?"` on the real parser (cli.py:475): a bare
    # `evidence sync` replays every queued intent.
    "evidence sync": (),
    "evidence waive": ("change", "--scope", "--reason"),
    "gate post-edit": ("change",),
    "gate task-done": ("change", "task"),
}

# --- coverage outcomes -----------------------------------------------------
EXECUTABLE = "executable"
NOT_A_PACT_COMMAND = "not_a_pact_command"
UNKNOWN_PACT_VERB = "unknown_pact_verb"
INCOMPLETE_COMMAND = "incomplete_command"
HARNESS_GAP = "harness_gap"


@lru_cache(maxsize=1)
def known_verbs() -> frozenset[str]:
    """Every verb the REAL `okg pact` parser accepts.

    Read off the argparse tree rather than restated, so a verb the CLI gains
    cannot masquerade as a PACT defect the moment it is recommended: it lands
    in ``harness_gap`` until the harness wires it.
    """
    from okg.substrate.cli._options import add_json_flag
    from okg.substrate.pact import cli as pact_cli

    parser = argparse.ArgumentParser(add_help=False)
    top = parser.add_subparsers(dest="command")
    pact_cli.register_subparser(top, add_json_flag, argv=[])
    names: set[str] = set()
    for verb, child in _subparser_choices(top.choices["pact"]).items():
        nested = _subparser_choices(child)
        if nested:
            names.update(f"{verb} {name}" for name in nested)
        else:
            names.add(verb)
    return frozenset(names)


def _subparser_choices(parser: argparse.ArgumentParser) -> dict[str, Any]:
    for action in parser._actions:  # noqa: SLF001 - argparse has no public reader
        if isinstance(action, argparse._SubParsersAction):
            return dict(action.choices)
    return {}


def unimplemented_verbs() -> tuple[str, ...]:
    """CLI verbs the harness cannot execute — the instrument's blind spots."""
    return tuple(sorted(known_verbs() - EXECUTABLE_VERBS))


def _missing_arguments(parsed: ParsedCommand) -> list[str]:
    required = REQUIRED_ARGUMENTS.get(str(parsed.verb), ())
    present = {
        "change": parsed.change,
        # `target` (evidence attach) and `task` (gate task-done) are the same
        # second positional.
        "target": parsed.task,
        "task": parsed.task,
    }
    missing: list[str] = []
    for name in required:
        if name.startswith("--"):
            if not parsed.options.get(name[2:]):
                missing.append(name)
        elif not present.get(name):
            missing.append(name)
    return missing


def coverage(parsed: ParsedCommand) -> tuple[str, str | None]:
    """Classify an emitted command: executable, harness gap, or defect.

    Returns ``(outcome, detail)``. The split matters: ``harness_gap`` is the
    instrument's fault and is excluded from every score, while
    ``unknown_pact_verb`` / ``not_a_pact_command`` / ``incomplete_command``
    are things the CLI actually recommended and are scored.
    """
    if not parsed.parse_ok or parsed.verb is None:
        if str(parsed.reason or "").startswith("okg command outside the pact surface"):
            # The instrument's blind spot, not a defect: it is a real `okg`
            # command, the harness just has no in-process wrapper for it.
            return HARNESS_GAP, parsed.reason
        return NOT_A_PACT_COMMAND, parsed.reason
    verb = parsed.verb
    if verb not in known_verbs():
        return UNKNOWN_PACT_VERB, verb
    if verb == "view":
        fmt = parsed.options.get("format", "status")
        if fmt not in EXECUTABLE_VIEW_FORMATS:
            return HARNESS_GAP, f"view --format={fmt}"
    if verb not in EXECUTABLE_VERBS:
        return HARNESS_GAP, verb
    missing = _missing_arguments(parsed)
    if missing:
        return INCOMPLETE_COMMAND, f"{verb} omits {', '.join(missing)}"
    return EXECUTABLE, None


def execute(
    recorder: Recorder,
    root: Path,
    parsed: ParsedCommand,
) -> CallResult | None:
    """Run a parsed command in-process; None when the harness cannot map it.

    Callers that score an outcome must ask :func:`coverage` first — a ``None``
    here does not say whether the command was unrunnable or merely unwired.
    """
    outcome, _detail = coverage(parsed)
    if outcome != EXECUTABLE:
        return None
    verb = parsed.verb
    change = parsed.change
    if verb == "view":
        fmt = parsed.options.get("format", "status")
        runner = {
            "bootstrap": calls.view_bootstrap,
            "next": calls.view_next,
            "status": calls.view_status,
            "brief": calls.view_brief,
            "markdown": calls.view_markdown,
        }[fmt]
        return runner(recorder, root, change)
    if verb == "evidence status":
        return calls.evidence_status(recorder, root, change)
    if verb == "evidence sync":
        return calls.evidence_sync(recorder, root, change)
    if verb == "gate post-edit":
        return calls.gate_post_edit_summary(recorder, root, change)
    if verb == "gate task-done":
        return calls.gate_task_done(recorder, root, change, parsed.task)
    if verb == "decide":
        return calls.decide(
            recorder,
            root,
            change,
            verdict=parsed.options["verdict"],
            note=parsed.options.get("note"),
            target=parsed.options.get("target"),
            escalate_to=parsed.options.get("to"),
            principal=parsed.options.get("principal"),
        )
    if verb == "evidence waive":
        return calls.evidence_waive(
            recorder,
            root,
            change,
            waiver_id=parsed.options.get("id"),
            scope=parsed.options["scope"],
            reason=parsed.options["reason"],
            owner=parsed.options.get("owner"),
            expires=parsed.options.get("expires"),
        )
    if verb == "evidence attach":
        return calls.evidence_attach(
            recorder,
            root,
            change,
            parsed.task,
            kind=parsed.options["kind"],
            ref=parsed.options["ref"],
            status=parsed.options.get("status", "pass"),
            command=parsed.options.get("command"),
            summary=parsed.options.get("summary"),
            generation_id=_int_or_none(parsed.options.get("generation-id")),
            data=_json_or_none(parsed.options.get("data-json")),
        )
    return None


def _int_or_none(value: str | None) -> int | None:
    try:
        return int(value) if value else None
    except ValueError:
        return None


def _json_or_none(value: str | None) -> dict[str, Any] | None:
    if not value:
        return None
    try:
        parsed = json.loads(value)
    except ValueError:
        return None
    return parsed if isinstance(parsed, dict) else None


# How each replayable origin renders as the command a human would type. The
# CORRECTED RE-INVOCATION fence ("same verb, exactly one argv token differs")
# is checked against these renderings, so the check is on real argv rather than
# on a dict comparison that could hide a second change.
_ORIGIN_ARGV: dict[str, tuple[str, ...]] = {
    "view_status": ("view", "{change}", "--format=status"),
    "view_next": ("view", "{change}", "--format=next"),
    "view_brief": ("view", "{change}", "--format=brief"),
    "view_bootstrap": ("view", "{change}", "--format=bootstrap"),
    "view_markdown": ("view", "{change}", "--format=markdown"),
    "evidence_status": ("evidence", "status", "{change}"),
    "evidence_sync": ("evidence", "sync", "{change}"),
    "gate_post_edit": ("gate", "post-edit", "{change}"),
    "gate_post_edit_summary": ("gate", "post-edit", "{change}"),
    "gate_task_done": ("gate", "task-done", "{change}", "{task}"),
}


def render_origin(origin: dict[str, Any], *, prefix: str = "okg") -> str | None:
    """The `okg pact ...` command an origin stands for, or None if unrenderable."""
    verb = str(origin.get("verb") or "")
    template = _ORIGIN_ARGV.get(verb)
    if template is None:
        if verb == "evidence_attach":
            kwargs = origin.get("kwargs") or {}
            argv = [
                "evidence",
                "attach",
                str(origin.get("change")),
                str(kwargs.get("target")),
                "--kind",
                str(kwargs.get("kind")),
                "--ref",
                str(kwargs.get("ref")),
            ]
            return " ".join([*prefix.split(), "pact", *argv, "--json"])
        return None
    argv = [
        part.format(change=origin.get("change"), task=origin.get("task"))
        for part in template
    ]
    return " ".join([*prefix.split(), "pact", *argv, "--json"])


def differing_argv_tokens(left: str, right: str) -> int | None:
    """How many argv tokens differ, or None when the shapes differ at all."""
    try:
        left_argv, right_argv = shlex.split(left), shlex.split(right)
    except ValueError:
        return None
    if len(left_argv) != len(right_argv):
        return None
    return sum(1 for a, b in zip(left_argv, right_argv) if a != b)


def blocked_by(payload: dict[str, Any]) -> list[str]:
    """Union of today's rotating blocker locations.

    The contract puts every refusal cause in one ``blocked_by`` list. Today
    the cause is spread across ``blockers``, ``missing_proof``,
    ``workflow_authority.blockers``, and validation error codes, so the
    harness unions them; that union is what child 1 collapses.
    """
    if isinstance(payload.get("blocked_by"), list):
        return sorted({str(token) for token in payload["blocked_by"]})
    tokens: set[str] = set()
    for token in payload.get("blockers") or []:
        tokens.add(str(token))
    for token in payload.get("missing_proof") or []:
        tokens.add(f"missing_proof:{token}")
    authority = payload.get("workflow_authority") or {}
    for token in authority.get("blockers") or []:
        tokens.add(str(token))
    validation = payload.get("validation") or {}
    for row in validation.get("errors") or []:
        tokens.add(f"validation:{row.get('code')}")
    for row in payload.get("validation_errors") or []:
        if row.get("severity") == "error":
            tokens.add(f"validation:{row.get('code')}")
    error = payload.get("error") or {}
    if error.get("code"):
        tokens.add(f"error:{error['code']}")
    return sorted(tokens)
