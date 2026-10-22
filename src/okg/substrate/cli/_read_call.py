"""The parse step of the read commands: a parsed command line to one tool
call and the output choices.

`parse_read_call(operator, args, as_json=...)` takes the argparse namespace
of `okg inspect|expand|filter|aggregate|map|query` or of `okg search` (when
it takes the operator path, `_read_operators.search_takes_operator_path`) and
returns a `ReadCall`: the MCP tool name, the argument object, and how the
reply is to be printed. It opens no database and never imports the server
module (`okg.substrate.mcp.server`); a test pins that. It may read standard
input (ids piped in), the environment and the checkout's default deployment,
and it stores the deployment it resolved on `args.deployment`.

What a refused command line raises is `ReadCallRefused`; whatever else
goes wrong is raised as it is. The render step (`_read_render`) prints a
refusal (`render_refusal`) and a reply (`render_reply`); the transport in
between is the caller's: `_read_operators.run_call` builds a read-only server
and calls it in this process.
"""
from __future__ import annotations

import argparse
from dataclasses import dataclass
from typing import Any, Optional

from okg.substrate.errors import SubstrateError


class ReadCallRefused(Exception):
    """The command line as typed cannot be read as one call (no deployment,
    `--args` unreadable, an argument given twice, a shell form it will not
    guess). `error` is the `SubstrateError` to print with
    `_read_render.render_refusal`; `mode` is `json` or `text`."""

    def __init__(self, error: SubstrateError, operator: str, mode: str) -> None:
        super().__init__(error.message)
        self.error = error
        self.operator = operator
        self.mode = mode


@dataclass(frozen=True)
class OutputChoices:
    """How the reply is printed.

    `as_json` is the format asked of the tool (`json` or `text`) and the
    format `render_reply` reads. `mode` is `text`, `json`, `jsonl` or `ids`;
    `all_pages` is `--all`. For `jsonl`, `ids` or `all_pages` the command
    makes more than one call or prints rows, and the reply is rendered by the
    shell layer (`_read_shell.drive`), not by `render_reply`.
    `refusal_hint` is one more stderr line after a text reply the tool
    refused.
    """

    as_json: bool
    mode: str = "text"
    all_pages: bool = False
    refusal_hint: Optional[str] = None


@dataclass
class ReadCall:
    """One read command, parsed: send `arguments` to the MCP tool
    `operator`, print the reply as `choices` say."""

    operator: str
    arguments: dict[str, Any]
    choices: OutputChoices
    #: The shell layer's plan (`_read_shell.Plan`): ids from standard input,
    #: pages, adjusted forms. `plan.shell` is True when the command needs
    #: the shell layer's driver rather than one call and `render_reply`.
    plan: Any
    #: The embedder spec `okg search --embedder` asked for, or None.
    embedder_spec: Optional[str] = None

    @property
    def result_format(self) -> str:
        """The reply format to ask the tool for (`json` or `text`)."""
        return "json" if self.choices.as_json else "text"

    @property
    def needs_driver(self) -> bool:
        return bool(self.plan.shell)


def parse_read_call(
    operator: str, args: argparse.Namespace, *, as_json: bool,
) -> ReadCall:
    """The call a read command makes, from its parsed command line.

    `operator` is `search` (its operator path) or one of
    `_read_operators.OPERATOR_COMMANDS`. Raises `ReadCallRefused`.
    """
    from okg.substrate.cli import _read_operators as ops

    mode = "json" if as_json else "text"
    hint: Optional[str] = None
    embedder: Optional[str] = None
    try:
        if operator == "search":
            arguments = ops.search_operator_arguments(args)
            embedder = getattr(args, "embedder", None)
        else:
            ops.resolve_deployment(args, operator)
            arguments = ops.operator_arguments(operator, args)
            # Words that are not SQL were most likely meant as a search; the
            # line follows the tool's own refusal of them and nothing else.
            if operator == "query" and not ops.looks_like_sql(arguments.get("sql")):
                hint = ops.TEXT_SEARCH_HINT
    except SubstrateError as exc:
        # The deployment is not named, `--args` unreadable or not an object,
        # an argument given twice, a flag pair that sets one argument twice:
        # the call as typed is refused.
        raise ReadCallRefused(exc, operator, mode) from None
    return plan_read_call(
        operator, arguments, args, as_json=as_json, embedder_spec=embedder,
        refusal_hint=hint,
    )


def plan_read_call(
    operator: str,
    arguments: dict[str, Any],
    args: argparse.Namespace,
    *,
    as_json: bool,
    embedder_spec: Optional[str] = None,
    refusal_hint: Optional[str] = None,
) -> ReadCall:
    """`arguments` with the shell layer's flags and bare forms applied
    (`_read_shell.build_plan`), as a `ReadCall`. A shell form refused from
    the command line alone raises `ReadCallRefused`."""
    from okg.substrate.cli import _read_shell

    try:
        plan = _read_shell.build_plan(operator, args, arguments, as_json=as_json)
    except _read_shell.ShellRefusal as exc:
        raise ReadCallRefused(exc, operator, "json" if as_json else "text") from None
    return ReadCall(
        operator=operator,
        arguments=plan.arguments,
        choices=OutputChoices(
            as_json=(plan.mode == "json") if plan.shell else as_json,
            mode=plan.mode,
            all_pages=plan.all_pages,
            refusal_hint=refusal_hint,
        ),
        plan=plan,
        embedder_spec=embedder_spec,
    )
