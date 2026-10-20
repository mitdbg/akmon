"""Shared CLI option-registration helpers.

The substrate CLI used to re-roll ``--dsn``, ``--deployment``,
``--json``, and ``--generation`` at 44 + 37 + 30+ + many sites
across ``cli/main.py``, ``cli/federation.py``, ``cli/poros.py``,
``cli/nomos.py``, and ``cli/_read_side.py``. The duplication made
it hard to tighten the DSN-resolution rule (per the
one-db-per-deployment invariant in ``history/openspec/project.md``) and
made grep-for-the-canonical-form impossible.

These helpers are the single source of truth. Every CLI module
that wants a DSN / deployment / json / generation flag MUST call
the matching ``add_*_arg`` here.

DSN resolution follows ``history/openspec/project.md``:

- When ``args.deployment`` is set: the manifest is authoritative
  via ``_resolve_deployment_dsn``. ``OKG_DSN`` is consumed only
  through explicit ``${OKG_DSN}`` interpolation inside the
  manifest, never as a silent default.
- When ``args.deployment`` is unset: DBA mode. Fall back to the
  ``--dsn`` flag, then ``OKG_DSN`` env var, otherwise raise a
  clear error.

Per the wave-1 cleanup that landed in ``cleanup-debt-audit-wave``:
``OKG_TEST_DSN`` is NEVER honored as a silent fallback. Tests
that need a non-default DSN must export ``OKG_DSN`` for the run.
"""
from __future__ import annotations

import argparse
import os
from typing import Any, Callable


def add_dsn_arg(
    parser: argparse.ArgumentParser,
    *,
    help: str = "Postgres DSN; defaults to $OKG_DSN",
    required: bool = False,
    default: Any = None,
) -> None:
    """Register the canonical ``--dsn`` flag on ``parser``.

    Does NOT set a ``default=`` — resolution happens at use time
    via ``resolve_dsn(args)`` so the deployment-manifest-vs-DBA
    policy stays in one place.
    """
    parser.add_argument("--dsn", help=help, required=required, default=default)


def add_deployment_arg(
    parser: argparse.ArgumentParser,
    *,
    default: str | None = None,
    required: bool = False,
    help: str = "Deployment name (manifest-resolved DSN)",
) -> None:
    """Register the canonical ``--deployment`` flag on ``parser``."""
    parser.add_argument(
        "--deployment",
        default=default,
        required=required,
        help=help,
    )


def add_json_flag(
    parser: argparse.ArgumentParser,
    *,
    help: str = "emit machine-readable JSON",
    default: Any = False,
) -> None:
    """Register the canonical ``--json`` flag on ``parser``."""
    parser.add_argument("--json", action="store_true", help=help, default=default)


def add_generation_arg(
    parser: argparse.ArgumentParser,
    *,
    help: str = "pin to a specific generation_id",
    required: bool = False,
) -> None:
    """Register the canonical ``--generation`` flag on ``parser``."""
    parser.add_argument(
        "--generation",
        type=int,
        required=required,
        help=help,
    )


def resolve_dsn(
    args: argparse.Namespace,
    *,
    deployment_resolver: Callable[[argparse.Namespace], str] | None = None,
) -> str:
    """Resolve the DSN per the project policy.

    If ``args.deployment`` (or ``OKG_DEPLOYMENT``) is set and a
    ``deployment_resolver`` is supplied, the manifest path runs:
    the resolver MUST raise on missing manifest or interpolation
    failure rather than silently falling back.

    Otherwise (DBA mode), fall back to ``args.dsn`` → ``OKG_DSN``
    → raise.
    """
    if getattr(args, "deployment", None) or os.environ.get("OKG_DEPLOYMENT"):
        if deployment_resolver is None:
            raise RuntimeError(
                "args.deployment is set but no deployment_resolver "
                "was supplied to resolve_dsn(); call sites that "
                "support --deployment MUST pass _resolve_deployment_dsn"
            )
        return deployment_resolver(args)
    cli_dsn = getattr(args, "dsn", None)
    if cli_dsn:
        return cli_dsn
    env_dsn = os.environ.get("OKG_DSN")
    if env_dsn:
        return env_dsn
    raise RuntimeError(
        "no DSN: pass --deployment <name> (manifest "
        "authoritative), --dsn postgres://..., or set "
        "OKG_DSN for DBA-level operations"
    )


def install_actionable_errors(parser, example: str) -> None:
    """Per `reduce-agent-friction` req.friction.actionable-cli-errors: the
    top mined CLI failure class was argparse usage-dumps agents retried
    against verbatim. Wrap `parser.error` so every parse failure on this
    family prints ONE actionable line — a did-you-mean for invalid
    subcommand/flag choices, else the family's canonical example — BEFORE
    argparse's usage text. Exit codes and argparse behavior are unchanged.

    Idempotent: the first install wins (its example is the family's), so
    the post-build tree pass can wrap every remaining leaf without
    clobbering per-family registrations.
    """
    import difflib
    import re as _re
    import sys as _sys

    if getattr(parser, "_okg_actionable_example", None) is not None:
        return
    parser._okg_actionable_example = example
    original_error = parser.error

    def _actionable_error(message: str):
        hint = f"e.g. `{example}`"
        match = _re.search(r"invalid choice: '([^']+)'", message)
        if match:
            message, choices = _without_hidden_choices(parser, message)
            # The help epilog names the agent-facing hidden verbs (`pact`,
            # `bench`, ...), so suggesting one leaks nothing; unlisted,
            # retired and renamed verbs are named nowhere and stay out.
            choices += sorted(_epilog_named_verbs(parser))
            if choices:
                close = difflib.get_close_matches(
                    match.group(1), choices, n=1, cutoff=0.5
                )
                if close:
                    hint = f"did you mean `{close[0]}`? e.g. `{example}`"
        print(
            f"error: {parser.prog}: {message} — {hint}",
            file=_sys.stderr,
        )
        original_error(message)

    parser.error = _actionable_error


def _without_hidden_choices(parser, message: str) -> tuple[str, list[str]]:
    """argparse's invalid-choice message with the help-hidden verbs removed.

    The top-level usage line already leaves out hidden, retired, renamed and
    unlisted verbs, but argparse's own ``(choose from ...)`` list named every
    one, internal commands included. Returns the message with that list
    rebuilt from the public choices, and those public choices. A subcommand of the same name in another group
    (``okg poros send``) is a different command and stays listed there.
    """
    import re as _re

    choices_match = _re.search(r"\(choose from (.+)\)$", message)
    if not choices_match:
        return message, []
    raw = [token.strip() for token in choices_match.group(1).split(",")]
    quoted = bool(raw) and raw[0][:1] in {"'", '"'}
    choices = [token.strip("'\"") for token in raw]
    hidden = _hidden_top_level_verbs(parser)
    public = [choice for choice in choices if choice not in hidden]
    listed = ", ".join(f"'{c}'" if quoted else c for c in public)
    return (
        message[: choices_match.start()] + f"(choose from {listed})",
        public,
    )


def _epilog_named_verbs(parser) -> frozenset[str]:
    """Hidden verbs the top-level help epilog still names by name."""
    from okg.substrate.cli import entry_roles

    return frozenset(
        verb for verb in _hidden_top_level_verbs(parser)
        if entry_roles.visibility_of(verb) == entry_roles.HIDDEN
        and verb not in entry_roles.RENAMED_VERBS
    )


def _hidden_top_level_verbs(parser) -> frozenset[str]:
    """Verbs this parser's command list leaves out of its help listing."""
    import argparse

    for action in getattr(parser, "_actions", ()):
        if isinstance(action, argparse._SubParsersAction):
            hidden = getattr(action, "_okg_hidden_from_help", None)
            if hidden:
                return frozenset(hidden) & frozenset(action.choices or ())
    return frozenset()


def install_actionable_errors_tree(parser, default_example: str) -> None:
    """Wrap the whole parser tree after registration: leaf subparsers own
    their `error()`, so a family-level wrap alone leaves the most common
    real failures (missing required arg on the leaf verb, bad flag value)
    as bare usage dumps. Each unwrapped parser inherits the nearest
    wrapped ancestor's canonical example."""
    import argparse as _argparse

    def _walk(node, inherited: str) -> None:
        install_actionable_errors(node, inherited)
        example = getattr(node, "_okg_actionable_example", inherited)
        for action in node._actions:
            if isinstance(action, _argparse._SubParsersAction):
                seen = set()
                for child in action.choices.values():
                    if id(child) in seen:  # aliases share the child parser
                        continue
                    seen.add(id(child))
                    _walk(child, example)

    _walk(parser, default_example)


__all__ = [
    "add_deployment_arg",
    "add_dsn_arg",
    "add_generation_arg",
    "add_json_flag",
    "install_actionable_errors",
    "resolve_dsn",
]
