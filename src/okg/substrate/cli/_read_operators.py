"""Read-operator CLI: `okg inspect|expand|filter|aggregate|map|query`, and
the operator path of `okg search`.

A command here holds no operator logic. It builds the server `okg mcp-serve
--deployment X` would run on stdio, read-only and in this process, connects
one in-memory MCP client to it, calls the tool of the command's name with the
arguments given, prints the reply and closes the server. Argument validation,
unknown-argument refusals, access checks, the size limit and the refusal text
are therefore the MCP server's own.

What this module owns is the shell side only: turning flags into the
argument object, and turning the reply into output and an exit code.

A command serves one call and exits, so it differs from the long-running
server in four ways, none of which changes a reply: its read pool has a
hard limit of the two connections one read holds at once, and its host-wide
claim is for those two; it keeps the server's INFO log off stderr, where the
caller reads the refusal, and prints a warning as one line; it calls the
tool without the client's follow-up tools/list round trip; and, for every
operator but `inspect`, it does not read the facts of the dataset card,
which only `inspect` reports.

Help, and `okg guide` (PACT read-commands-help-3666): `okg <tool> --help`
prints the tool's generic description (as a server built with no database
lists it), a type line per flag from the published input schema and one
worked example, all read from the generated `_read_help_text` module, so the
parser still never imports the server. A deployment's server may name its own
types and offer fewer search methods. A refusal that sends the caller to the tool's input schema points at
`okg <tool> --help` instead, which a shell can run. `okg guide` prints the
instructions a read-only client of the deployment is sent when it connects.
"""
from __future__ import annotations

import argparse
import contextlib
import difflib
import io
import json
import logging
import os
import re
import shlex
import signal
import sys
import textwrap
import threading
import time
import traceback
import warnings
from typing import Any, Iterator

from okg.substrate.cli import _read_render
from okg.substrate.errors import SubstrateError

#: Each operator's argument names, in the MCP tool's own order. Static so the
#: parser is built without importing the server; a test compares this table
#: with the registered tools, so the two cannot drift.
OPERATOR_ARGUMENTS: dict[str, tuple[str, ...]] = {
    "search": (
        "query", "method", "where", "scope", "subtype_filter", "attr_filters",
        "filter", "profile", "source_family", "max_per_parent", "rank_by",
        "limit", "offset", "generation_id", "select", "authority",
        "allow_unverified_embedding_provenance", "max_bytes", "max_chars",
        "expand_concepts", "path_prefix", "continuation_token", "view",
        "matching", "context_chars", "completeness", "retain", "summary", "within",
    ),
    "inspect": (
        "target", "generation_id", "select", "authority", "max_bytes", "next",
    ),
    "expand": (
        "input", "edge_type", "direction", "path", "depth", "target_subtype",
        "continuation_token", "filter", "generation_id", "limit", "offset",
        "select", "order_by", "include_siblings", "live", "authority",
        "max_bytes", "next",
    ),
    "filter": (
        "input", "predicate", "generation_id", "limit", "offset", "select",
        "order_by", "authority", "max_bytes", "retain", "next",
    ),
    "aggregate": (
        "input", "by", "metrics", "generation_id", "limit", "offset",
        "authority", "max_bytes", "predicate",
    ),
    "map": (
        "input", "projection", "generation_id", "limit", "offset",
        "authority", "max_bytes", "next",
    ),
    "query": (
        "sql", "generation_id", "row_cap", "timeout_ms", "authority",
        "max_bytes",
    ),
}

#: Tool arguments with no flag of their own: each reaches the tool inside
#: `--args`. `expand`'s `args` (the values of a live edge's arguments;
#: PACT live-graph-rucio-sites) would be the command's own `--args` flag.
ARGS_OBJECT_ONLY: dict[str, tuple[str, ...]] = {"expand": ("args",)}

#: The commands this module registers itself. `search` is registered by
#: `_read_side`, which adds the operator-only flags below to its parser.
OPERATOR_COMMANDS: tuple[str, ...] = (
    "inspect", "expand", "filter", "aggregate", "map", "query",
)

#: Arguments whose non-JSON flag value is a comma-separated list.
LIST_ARGUMENTS: frozenset[str] = frozenset(
    {"select", "by", "metrics", "path", "summary", "projection"}
)

#: The one positional shorthand of a command: (argument, metavar).
POSITIONAL: dict[str, tuple[str, str]] = {
    "expand": ("input", "NODE_ID"),
    "query": ("sql", "SQL"),
}

_HELP: dict[str, str] = {
    "inspect": "Read one record, text, subtype or session fact (MCP inspect)",
    "expand": "Follow edges from a record (MCP expand)",
    "filter": "List every record meeting a condition (MCP filter)",
    "aggregate": "Count records per value (MCP aggregate)",
    "map": "Read chosen fields of many records (MCP map)",
    "query": "Run one bounded read-only SQL statement (MCP query)",
}

#: The command that prints the MCP server's connect-time instructions.
GUIDE_COMMAND = "guide"

#: The one epilog line of `okg --help` that names the read commands.
READ_COMMANDS_EPILOG = textwrap.fill(
    "Read the graph from a shell: okg search, inspect, expand, filter, "
    "aggregate, map and query call the MCP read tool of the same name, and "
    "okg guide prints the instructions an MCP client gets when it connects. "
    "Each has its own --help.",
    width=78,
    break_on_hyphens=False,
)

#: Every method the MCP `search` tool declares, and the three `okg search`
#: has always run itself. Any other method takes the operator path.
SEARCH_METHODS: tuple[str, ...] = (
    "lexical", "semantic", "alias", "identifier_mentions", "attrs", "grep",
)
LEGACY_SEARCH_METHODS: frozenset[str] = frozenset(
    {"lexical", "semantic", "alias"}
)

#: `search` arguments the flags `okg search` has today already carry on the
#: operator path: argument -> the parsed attribute holding it.
SEARCH_SHARED_FLAGS: dict[str, str] = {
    "method": "method",
    "profile": "profile",
    "source_family": "source_family",
    "max_per_parent": "max_per_parent",
    "rank_by": "rank_by",
    "limit": "limit",
    "offset": "offset",
    "generation_id": "generation_id",
    "select": "select",
    "max_chars": "max_chars",
    "completeness": "completeness",
}

#: `search` arguments with no flag. `--scope` already means the search
#: profile on this command (the MCP `profile` argument), so the MCP `scope`
#: argument is given through `--args`.
SEARCH_ARGS_ONLY: frozenset[str] = frozenset({"scope"})

#: `search` arguments that get a new, operator-only flag.
SEARCH_OPERATOR_FLAGS: tuple[str, ...] = (
    "where", "subtype_filter", "attr_filters", "filter", "authority",
    "max_bytes", "expand_concepts", "path_prefix", "continuation_token",
    "view", "matching", "context_chars", "retain", "summary", "within",
)

#: The variable okg commands read the deployment from when the flag is not
#: given (`okg.substrate.deployment_default.DEPLOYMENT_ENV`).
DEPLOYMENT_ENV = "OKG_DEPLOYMENT"
#: Set to 1 to diagnose a read command: one line of phase times on stderr,
#: the server's own log, and every error with its traceback and with the
#: paths a command otherwise masks.
DEBUG_ENV = "OKG_READ_DEBUG"
#: The parsed attribute `main` stores the parser build and parse time in.
PARSE_SECONDS_ATTR = "read_parse_seconds"
#: Read-pool connections a command opens at the start.
READ_POOL_MIN = 1
#: The operators whose replies state the dataset card or the served
#: instructions (`inspect` kinds `session_start` and `surface`). A command
#: for any other operator builds its server without reading the card's
#: facts, which saves a round of database reads at start.
CARD_OPERATORS: frozenset[str] = frozenset({"inspect"})

#: Parsed-attribute prefix of an operator flag, so `--filter`, `--input` or
#: `--next` cannot collide with an attribute the CLI itself uses.
_DEST = "op_"
#: The parsed attribute naming the flags given on the command line, for the
#: `okg search` flags whose default is not None.
GIVEN_ATTR = "read_flags_given"


class RecordGiven(argparse.Action):
    """A plain store that also records that the flag was given.

    `okg search --limit` defaults to 50 on the path it has always had, while
    the MCP tool resolves an omitted limit per view. The operator path must
    send a limit only when one was typed.
    """

    def __call__(self, parser, namespace, values, option_string=None):  # type: ignore[override]
        setattr(namespace, self.dest, values)
        given = set(getattr(namespace, GIVEN_ATTR, None) or ())
        given.add(self.dest)
        setattr(namespace, GIVEN_ATTR, given)


def flag_of(argument: str) -> str:
    return "--" + argument.replace("_", "-")


def debug_enabled() -> bool:
    return os.environ.get(DEBUG_ENV, "").strip().lower() in {"1", "true", "yes", "on"}


def require_deployment(
    args: argparse.Namespace, operator: str, *, as_json: bool,
) -> int | None:
    """Name the deployment on `args`, or print why none is named.

    The order every okg command follows (`okg.substrate.deployment_default`):
    `--deployment`, then `OKG_DEPLOYMENT`, then the enclosing checkout's
    configured default. None when one is named; it is stored on `args`, so
    every later step reads the same name. Otherwise the usual error object
    (`deployment_not_specified`, which names all three) is printed and the
    exit code is returned: 2, the call as typed cannot run.
    """
    from okg.substrate.cli._read_shell import emit_refusal

    try:
        resolve_deployment(args, operator)
    except SubstrateError as exc:
        # A refusal of the call as typed: exit 2, in a refusal's shape.
        return emit_refusal(exc, operator, args, mode="json" if as_json else "text")
    return None


def resolve_deployment(args: argparse.Namespace, operator: str) -> str:
    """Name the deployment on `args` (the order `require_deployment` says),
    or raise the `SubstrateError` that says why none is named."""
    from okg.substrate.cli._read_shell import TYPED_DEPLOYMENT_ATTR
    from okg.substrate.deployment_default import resolve_command_deployment

    # A `retry:` line names --deployment only when it was typed.
    setattr(args, TYPED_DEPLOYMENT_ATTR, getattr(args, "deployment", None))
    args.deployment = resolve_command_deployment(
        getattr(args, "deployment", None), command=f"okg {operator}",
    )
    return args.deployment


def _usage_error(code: str, message: str, *, fix_hint: str, **data: Any) -> SubstrateError:
    err = SubstrateError(message, data=dict(data), fix_hint=fix_hint)
    err.code = code
    return err


def _reject_constant(name: str) -> Any:
    raise ValueError(name)


def parse_flag_value(argument: str, raw: str) -> Any:
    """JSON when the value parses as JSON, otherwise the string itself.

    A list argument's non-JSON value is split on commas. `NaN` and
    `Infinity` are not JSON and stay strings.
    """
    try:
        return json.loads(raw, parse_constant=_reject_constant)
    except ValueError:
        pass
    if argument in LIST_ARGUMENTS:
        return [part.strip() for part in raw.split(",") if part.strip()]
    return raw


def load_args_object(spec: str | None) -> dict[str, Any]:
    """The `--args` argument object: JSON text, `@path`, or `-` for stdin."""
    if spec is None:
        return {}
    if spec == "-":
        source, text = "standard input", sys.stdin.read()
    elif spec.startswith("@"):
        source = spec[1:]
        try:
            with open(source, encoding="utf-8") as handle:
                text = handle.read()
        except OSError as exc:
            raise _usage_error(
                "read_args_unreadable",
                f"--args could not read {source!r}: {exc}",
                fix_hint="pass --args @<path to a JSON file>, --args '<json>' or --args -",
                path=source,
            ) from exc
    else:
        source, text = "the --args value", spec
    try:
        parsed = json.loads(text)
    except ValueError as exc:
        raise _usage_error(
            "read_args_invalid",
            f"--args expects one JSON object; {source} is not JSON: {exc}",
            fix_hint="for example --args '{\"limit\": 10}'",
            source=source,
        ) from exc
    if not isinstance(parsed, dict):
        raise _usage_error(
            "read_args_invalid",
            f"--args expects one JSON object; {source} holds "
            f"{type(parsed).__name__}",
            fix_hint="for example --args '{\"limit\": 10}'",
            source=source,
        )
    return parsed


def _merge(operator: str, base: dict[str, Any], flags: dict[str, Any]) -> dict[str, Any]:
    twice = sorted(set(base) & set(flags))
    if twice:
        names = ", ".join(twice)
        raise _usage_error(
            "read_argument_given_twice",
            f"okg {operator} received {names} both inside --args and as its "
            "own flag or positional. It will not guess which one you meant.",
            fix_hint=f"give {twice[0]} once",
            arguments=twice,
        )
    return {**base, **flags}


def operator_arguments(operator: str, args: argparse.Namespace) -> dict[str, Any]:
    """The argument object for one of `OPERATOR_COMMANDS`."""
    flags: dict[str, Any] = {}
    for argument in OPERATOR_ARGUMENTS[operator]:
        raw = getattr(args, _DEST + argument, None)
        if raw is not None:
            flags[argument] = parse_flag_value(argument, raw)
    positional = POSITIONAL.get(operator)
    value = getattr(args, "operator_positional", None)
    if positional is not None and value is not None:
        argument, metavar = positional
        if argument in flags:
            raise _usage_error(
                "read_argument_given_twice",
                f"okg {operator} received {argument} both as {metavar} and "
                f"as {flag_of(argument)}.",
                fix_hint=f"give {argument} once",
                arguments=[argument],
            )
        # A positional is the text typed, never JSON: a numeric id stays a
        # string.
        flags[argument] = value
    return _merge(operator, load_args_object(getattr(args, "operator_args", None)), flags)


# ── okg search: which path, and the operator path's arguments ────────────


def _search_operator_flags_given(args: argparse.Namespace) -> list[str]:
    return [
        flag_of(argument)
        for argument in SEARCH_OPERATOR_FLAGS
        if getattr(args, _DEST + argument, None) is not None
    ]


def search_takes_operator_path(args: argparse.Namespace) -> bool:
    """True when `okg search` was given anything only the MCP tool takes.

    With only the flags the command had before the read operators reached
    the command line, this is False and the command runs as it always has.
    """
    if getattr(args, "operator_args", None) is not None:
        return True
    if _search_operator_flags_given(args):
        return True
    from okg.substrate.cli._read_shell import flags_given

    if any(flags_given(args).values()):
        return True
    method = getattr(args, "method", None)
    return method is not None and method not in LEGACY_SEARCH_METHODS


def search_operator_arguments(args: argparse.Namespace) -> dict[str, Any]:
    """Map `okg search` flags onto the MCP `search` argument object.

    A flag that cannot be mapped faithfully is refused by name with the
    working form, never dropped.
    """
    given = set(getattr(args, GIVEN_ATTR, None) or ())
    flags: dict[str, Any] = {}

    def conflict(old: str, new: str) -> SubstrateError:
        return _usage_error(
            "search_flags_conflict",
            f"okg search received {old} and {new}, which set the same "
            "search argument.",
            fix_hint=f"keep {new} and drop {old}",
            flags=[old, new],
        )

    context_source = (
        "--nomos-access-context-json"
        if getattr(args, "nomos_access_context_json", None) is not None
        # The search the command runs itself also reads the context from the
        # environment; the MCP tool never does, so it is refused the same way
        # rather than dropped.
        else "OKG_NOMOS_ACCESS_CONTEXT" if os.getenv("OKG_NOMOS_ACCESS_CONTEXT")
        else None
    )
    if context_source is not None:
        operator_only = _search_operator_flags_given(args) or ["--args"]
        raise _usage_error(
            "search_flags_conflict",
            f"okg search received {context_source} together with "
            f"{', '.join(operator_only)}. The first belongs to the search the "
            "command runs itself; the others run the MCP search tool, which "
            "takes the access context inside its authority argument.",
            fix_hint=(
                "pass --authority '{\"nomos_access_context\": {...}}' "
                f"instead of {context_source}"
            ),
            flags=[context_source, *operator_only],
        )

    positional = getattr(args, "query_positional", None)
    option = getattr(args, "query", None)
    if positional is not None and option is not None and positional != option:
        raise _usage_error(
            "search_query_conflict",
            "okg search received two different queries: the positional "
            "QUERY and --query. It will not guess which one you meant.",
            fix_hint="pass the query once, either as QUERY or as --query",
            positional=positional, option=option,
        )
    query = positional if positional is not None else option
    if query is not None:
        flags["query"] = query

    for argument, dest in SEARCH_SHARED_FLAGS.items():
        value = getattr(args, dest, None)
        if value is None:
            continue
        if argument in {"method", "limit", "offset"} and dest not in given:
            continue
        if argument == "select":
            # `--select` is repeatable here: one JSON value, or comma lists.
            value = (
                parse_flag_value("select", value[0])
                if len(value) == 1
                else [p.strip() for v in value for p in v.split(",") if p.strip()]
            )
        elif argument == "generation_id":
            value = parse_flag_value(argument, str(value))
        flags[argument] = value
    if getattr(args, "allow_unverified_embedding_provenance", False):
        flags["allow_unverified_embedding_provenance"] = True

    where_refused = None
    for argument in SEARCH_OPERATOR_FLAGS:
        raw = getattr(args, _DEST + argument, None)
        if raw is None:
            continue
        if argument == "where":
            where_refused = _search_where(flags, raw)
        else:
            flags[argument] = parse_flag_value(argument, raw)

    subtype = getattr(args, "subtype", None)
    if subtype is not None:
        if "subtype_filter" in flags:
            raise conflict("--subtype", "--subtype-filter")
        flags["subtype_filter"] = subtype
    attr_filter = getattr(args, "attr_filter", None)
    if attr_filter:
        if "attr_filters" in flags:
            raise conflict("--attr-filter", "--attr-filters")
        from okg.substrate.cli._read_side import _parse_attr_filters

        flags["attr_filters"] = _parse_attr_filters(attr_filter)
    if getattr(args, "expand_concepts", None) is False:
        if "expand_concepts" in flags:
            raise conflict("--no-concept-expansion", "--expand-concepts")
        flags["expand_concepts"] = False

    merged = _merge("search", load_args_object(getattr(args, "operator_args", None)), flags)
    if where_refused is not None:
        raise where_refused.refusal(
            "search", merged if where_refused.corrected is not None else None, args,
        )
    return merged


def _search_where(flags: dict[str, Any], terms: list[str]) -> Any:
    """`okg search --where`, repeatable: the condition into `flags`, or the
    refusal of a term (`_read_shell.where_condition`), with the corrected
    condition in `flags` when there is one."""
    from okg.substrate.cli._read_shell import WhereRefusal, where_condition

    try:
        flags["where"] = where_condition(list(terms))
    except WhereRefusal as problem:
        if problem.corrected is not None:
            flags["where"] = problem.corrected
        return problem
    return None


# ── the server, the call, the reply ──────────────────────────────────────


@contextlib.contextmanager
def _read_only_launch() -> Iterator[None]:
    """Build the server read-only whatever the environment says.

    `build_server` also turns writes on from OKG_MCP_ALLOW_WRITES. A read
    command opens no write pool and registers no write tool, so the variable
    is set aside for the build and put back.
    """
    saved = os.environ.pop("OKG_MCP_ALLOW_WRITES", None)
    try:
        yield
    finally:
        if saved is not None:
            os.environ["OKG_MCP_ALLOW_WRITES"] = saved


#: Warnings a one-call read does not print, each named by the logger that
#: emits it and by how its text starts. The one entry is
#: `mcp/upload_admin.py`'s warning that the deployment's upload tool is not
#: provisioned. The server build asks that for every launch; the upload tool
#: is a write tool, which a read command never serves, and the warning
#: carries its cause, which on a host without the deployment's code
#: checkouts is the registry's `code_repos.base` error with the deployment's
#: folder in it. A shell run of the read commands printed it on 1,163 of
#: 1,837 calls. `okg mcp-serve` still prints it.
#: Written with keywords: a sequence whose first item is the bare package
#: name reads as an argv that starts the CLI (`tests/substrate/cli/
#: test_child_argv.py`), and this is not one.
NOISE_WARNINGS: tuple[dict[str, str], ...] = (
    dict(
        logger="okg.substrate.mcp.upload_admin",
        text="upload tool not provisioned for deployment ",
    ),
)

#: One part of a path: no white space and no slash.
_SEGMENT = r"[\w.@+~-]+"
#: A `file://` URL of the host. It runs to white space or a quote; it takes
#: in a bracket pair inside it (`file:///srv/a(b)/c`) but not a closing
#: bracket it did not open, so the one around a URL (`see (file:///srv/x)`)
#: stays. Kept apart from `_HOST_PATH`: a database address is masked only by
#: `okg.substrate.db.dsn`, and no pattern here may look like one.
_FILE_URL_CHAR = r"[^\s'\"<>`()]"
_FILE_URL = re.compile(rf"(?<![\w])file://(?:{_FILE_URL_CHAR}|\({_FILE_URL_CHAR}*\))+")
#: A file or a directory of the host: an absolute path of two or more parts.
#: A part after the first may hold one space (`My Documents`) when another
#: part follows it that ends in a slash or holds a dot (`/srv/My
#: Documents/notes.txt`), so a slash in the words after a path (`/etc/hosts
#: and/or`) does not take them in.
_HOST_PATH = re.compile(
    rf"(?<![\w:/.@<-])/{_SEGMENT}/"
    rf"(?:{_SEGMENT} {_SEGMENT}/(?={_SEGMENT}/|[\w@+~-]*\.\w)|{_SEGMENT}/)*"
    r"[\w.@+~-]*"
)


def mask(text: str) -> str:
    """`text` with the secrets of every database address in it and each path
    of this host replaced.

    A password in a DSN (`user:password@host`, `password=...`) becomes `***`
    and a `--dsn` flag's value `<redacted-dsn>`, by the masking every okg
    error uses (`okg.substrate.db.dsn.redact_text`); a path or a `file://`
    URL becomes `<path>`. The one function everything a read command prints
    on stderr goes through, apart from the tool's own reply, which is
    printed as the tool gave it. With `OKG_READ_DEBUG=1` nothing is masked.
    """
    if debug_enabled():
        return str(text)
    from okg.substrate.db.dsn import redact_text

    return _HOST_PATH.sub("<path>", _FILE_URL.sub("<path>", redact_text(str(text))))


def mask_value(value: Any) -> Any:
    """A JSON-ready copy of `value` with every string in it masked."""
    if isinstance(value, dict):
        return {mask(str(key)): mask_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set, frozenset)):
        return [mask_value(item) for item in value]
    if value is None or isinstance(value, (bool, int, float)):
        return value
    return mask(str(value))


def one_line(text: str) -> str:
    """`text` on one line, masked."""
    return mask(" ".join(str(text).split()))


def say(text: str) -> None:
    """One masked line on stderr."""
    sys.stderr.write(one_line(text) + "\n")


def _is_noise(logger_name: str, message: str) -> bool:
    return any(
        logger_name == noise["logger"] and message.startswith(noise["text"])
        for noise in NOISE_WARNINGS
    )


class _OneLine(logging.Handler):
    """One line on stderr per record at WARNING or above.

    No traceback and no directory of the host; `OKG_READ_DEBUG=1` shows
    both.
    """

    def __init__(self) -> None:
        super().__init__(level=logging.WARNING)

    def emit(self, record: logging.LogRecord) -> None:
        try:
            message = record.getMessage()
            if record.levelno < logging.ERROR and _is_noise(record.name, message):
                return
            line = f"{record.levelname} {record.name}: {message}"
            raised = record.exc_info[0] if record.exc_info else None
            if raised is not None:
                line += f" ({raised.__name__}; set {DEBUG_ENV}=1 for the traceback)"
            say(line)
        except Exception:  # noqa: BLE001 - a log line never fails the command
            self.handleError(record)


def _show_warning(message, category, filename, lineno, file=None, line=None):  # noqa: ANN001
    """A Python warning as one stderr line, without the file it came from."""
    text = str(message)
    if _is_noise("py.warnings", text):
        return
    say(f"warning: {category.__name__}: {text}")


@contextlib.contextmanager
def quiet_stderr() -> Iterator[None]:
    """Keep the server's log noise off stderr while a command runs.

    The MCP library installs a stderr handler at INFO when the server
    object is made, so a fresh process prints a line per request served. A
    shell caller reads stderr with the answer on every call. For the length
    of one command, records at INFO and below are not logged, and the one
    warning in `NOISE_WARNINGS` is dropped. Every other record at WARNING or
    above, and every Python warning, prints as one line. With
    `OKG_READ_DEBUG=1` nothing is changed.
    """
    if debug_enabled():
        yield
        return
    root = logging.getLogger()
    handler = _OneLine()
    disabled_before = root.manager.disable
    # A root handler placed first also makes the MCP library's
    # `logging.basicConfig` a no-op, so it adds no handler of its own.
    root.addHandler(handler)
    logging.disable(max(disabled_before, logging.INFO))
    try:
        with warnings.catch_warnings():
            # The process's own filters decide which warnings are shown;
            # only how one is printed changes.
            warnings.showwarning = _show_warning
            yield
    finally:
        logging.disable(disabled_before)
        root.removeHandler(handler)


@contextlib.contextmanager
def released_on_stop(deployment: str, surface: str) -> Iterator[None]:
    """Give the claim back when the command is told to stop.

    A harness that finds a command slow sends it SIGTERM (`timeout 2 okg
    query ...`). Python's default for that signal ends the process at once:
    no `finally`, no `atexit`, and the command's connection claim would
    count against the host for the ledger's 300 s stale horizon. For the
    length of one command SIGTERM is handled as `okg mcp-serve` handles it
    (`_unwind_default_sigterm`): the claim is released at once from a
    thread of its own, SystemExit(143) unwinds the command so its pool
    closes, and the process is ended 5 s later if something still holds it.
    That hard end writes one line that names `surface` (`okg query did not
    finish ...`), not `okg mcp-serve`, and leaves out the thread stacks
    `okg mcp-serve` adds, which name files of the host; `OKG_READ_DEBUG=1`
    keeps them. SIGHUP is passed on as SIGTERM. A handler another program
    installed is left alone, and the previous handlers are put back.
    """
    from okg.substrate.mcp.server import _unwind_default_sigterm

    def release() -> None:
        from okg.substrate.db.capacity import release_process_reservations

        release_process_reservations(application_name=f"okg-mcp-{deployment}")

    def hang_up(signum: int, frame: Any) -> None:
        os.kill(os.getpid(), signal.SIGTERM)

    hangup = getattr(signal, "SIGHUP", None)
    forwarded = (
        hangup is not None
        and threading.current_thread() is threading.main_thread()
        and signal.getsignal(hangup) is signal.SIG_DFL
    )
    if forwarded:
        signal.signal(hangup, hang_up)
    try:
        with _unwind_default_sigterm(
            release,
            exit_message=f"{surface} did not finish",
            thread_dump=debug_enabled(),
        ):
            yield
    finally:
        if forwarded and signal.getsignal(hangup) is hang_up:
            signal.signal(hangup, signal.SIG_DFL)


class Timing:
    """Wall-clock time of each phase of one command, for `OKG_READ_DEBUG`."""

    def __init__(self, surface: str, *, parse_seconds: float | None = None) -> None:
        self.surface = surface
        self.phases: list[tuple[str, float]] = []
        if parse_seconds is not None:
            self.phases.append(("parse", float(parse_seconds)))

    @contextlib.contextmanager
    def phase(self, name: str) -> Iterator[None]:
        # load: the server module and the stop handler; build: the checks
        # and the server; call; close: the pool and the claim; print.
        started = time.perf_counter()
        try:
            yield
        finally:
            self.phases.append((name, time.perf_counter() - started))

    def line(self) -> str:
        parts = " ".join(
            f"{name}={seconds * 1000:.0f}ms" for name, seconds in self.phases
        )
        return f"{self.surface} timing: {parts}"

    def report(self) -> None:
        if debug_enabled():
            sys.stderr.write(self.line() + "\n")


def build_read_server(
    args: argparse.Namespace,
    *,
    surface: str,
    embedder_spec: str | None = None,
    dataset_card: bool = True,
) -> Any:
    """The server `okg mcp-serve --deployment X` runs on stdio, read-only.

    The same checks in the same order as that command: the database and the
    read role are usable, the release verdict lets this code read the
    database (or serve it read-only, stamped), and semantic search comes
    from the deployment's embeddings block. No transport is started.
    `dataset_card=False` skips reading the card's facts, for a tool whose
    reply states no part of it (`CARD_OPERATORS`).
    """
    from okg.substrate.cli import main as cli_main
    from okg.substrate.cli.dsn_authority import resolve_read_dsn
    from okg.substrate.db.readiness import (
        mcp_read_roles_to_check,
        require_database_ready,
        require_mcp_read_dsn_usable,
    )
    from okg.substrate.deployment_release import (
        enforce_release_contract_if_declared,
    )
    from okg.substrate.local_database_roles import require_role_dsn_usable
    from okg.substrate.mcp.server import (
        MCP_CONNECTIONS_PER_WORKER,
        build_server,
        read_only_release_from_report,
    )

    resolution = resolve_read_dsn(args, surface=surface)
    dsn = resolution.dsn
    deployment = args.deployment
    readiness = require_database_ready(
        dsn, surface=surface, deployment=deployment,
        roles=mcp_read_roles_to_check(),
    )
    require_mcp_read_dsn_usable(surface=surface, deployment=deployment)
    require_role_dsn_usable(
        dsn, role="okg_mcp", env_var="OKG_MCP_RO_DSN", surface=surface,
    )
    release_report = enforce_release_contract_if_declared(
        deployment=deployment, dsn=dsn, process_role="mcp",
        serve_read_only=True,
    )
    # A note that the database is behind this code is the operator's to
    # read. It is printed, through the same masking as every other line.
    notes = io.StringIO()
    with contextlib.redirect_stderr(notes):
        cli_main._report_database_behind(
            readiness, release_report, surface=surface, deployment=deployment,
        )
    for note in notes.getvalue().splitlines():
        sys.stderr.write(mask(note) + "\n")
    read_only_release = None
    if release_report is not None and release_report.get("verdict") == "read_only":
        read_only_release = read_only_release_from_report(release_report)
    plan = cli_main._mcp_serve_embedding_plan(
        deployment, asserted_spec=embedder_spec,
    )
    with _read_only_launch():
        return build_server(
            dsn=dsn,
            dsn_authority={
                "dsn": resolution.dsn,
                "authority": resolution.authority,
                "target": resolution.target,
            },
            deployment_name=deployment,
            embedder=plan if plan.enabled else None,
            branch_name="default",
            allow_writes=False,
            read_only_release=read_only_release,
            # One connection is open from the start. A read holds its
            # session's connection and, inside it, the one that reads the
            # freshness stamp: two at once, the server's own figure for one
            # request worker, and the pool's hard limit here. A higher limit
            # is not a bound: the pool opens one more connection for every
            # request that waits, also while an earlier one is still being
            # established, and a lab run at a limit of four opened three.
            # Work the server does outside a worker (the sample a live
            # relation writes after a call gave up on it) waits for one of
            # the two.
            min_conns=READ_POOL_MIN,
            max_conns=MCP_CONNECTIONS_PER_WORKER,
            # The host-wide claim is for those two connections, not for a
            # server's declared widths, with no heartbeat thread: closing
            # the pool releases it, and a killed command's claim stops
            # counting after the ledger's stale horizon (300 s).
            one_call=True,
            dataset_card=dataset_card,
        )


def close_server(server: Any) -> None:
    try:
        server._okg_pool.close()
    finally:
        write_pool = getattr(server, "_okg_write_pool", None)
        if write_pool is not None:
            write_pool.close()


def call_operator(
    server: Any,
    operator: str,
    arguments: dict[str, Any],
    *,
    result_format: str,
) -> Any:
    """One tool call from an in-memory MCP client; the client's result."""
    import anyio
    from mcp.shared.memory import create_connected_server_and_client_session

    from okg.substrate.mcp.result_text import RESULT_FORMAT_META_KEY

    # Text is the server's default; a client that wants it sends no key.
    meta = {RESULT_FORMAT_META_KEY: "json"} if result_format == "json" else None
    # `ClientSession.call_tool` checks a served reply's structured content
    # against the tool's output schema, and to learn the schema it follows
    # the first call of a tool with a tools/list round trip. The schema is
    # known here, on the tool the server registered, which is where the
    # listing reads it. Handing it to the client keeps the check and drops
    # the round trip. (The read tools publish no output schema, so the
    # check then has nothing to compare; it is kept all the same.)
    schema = output_schema_of(server, operator)

    async def drive() -> Any:
        async with create_connected_server_and_client_session(server) as client:
            cache = getattr(client, "_tool_output_schemas", None)
            if schema is not _NOT_REGISTERED and isinstance(cache, dict):
                cache[operator] = schema
            # Without the cache (another SDK), or for a tool the server does
            # not register, the client lists as it always did.
            return await client.call_tool(operator, arguments, meta=meta)

    return anyio.run(drive)


_NOT_REGISTERED: Any = object()


def output_schema_of(server: Any, operator: str) -> Any:
    """The output schema of the tool the server registered, None when it
    publishes none, or `_NOT_REGISTERED`."""
    manager = getattr(server, "_tool_manager", None)
    tool = manager.get_tool(operator) if manager is not None else None
    if tool is None:
        return _NOT_REGISTERED
    return tool.output_schema


#: The code the MCP server gives an operator's untyped exception.
SERVER_FAULT_CODE = _read_render.SERVER_FAULT_CODE


def reply_text(result: Any) -> tuple[str, int]:
    """The reply's text block, and how many other blocks it carries."""
    reply = _read_render.reply_of(result)
    return reply.text, reply.other_blocks


def reply_refused(result: Any, text: str, *, as_json: bool) -> bool:
    return _read_render.text_is_refused(
        bool(getattr(result, "isError", False)), text, as_json=as_json,
    )


def reply_error_code(text: str, *, as_json: bool) -> Any:
    """The error code a refused reply states, or None
    (`_read_render.reply_error_code`)."""
    return _read_render.reply_error_code(text, as_json=as_json)


def shell_refusal_text(text: str) -> str:
    """A refused reply's text with each pointer to a tool's input schema
    replaced by the command that prints it (`_read_render`)."""
    return _read_render.shell_refusal_text(text)


def print_reply(result: Any, *, as_json: bool) -> int:
    """Print the reply uncut. 0 when served; 2, on stderr, when refused.

    A refusal is printed as the tool gave it, except that a pointer to the
    tool's input schema names `okg <tool> --help` (`shell_refusal_text`).
    """
    return _read_render.render_printed(
        _read_render.reply_of(result), as_json=as_json,
    ).write()


def single_cause(exc: BaseException) -> tuple[BaseException, int]:
    """The exception to report from inside an exception group, and how many
    the group holds.

    The in-memory MCP client runs in task groups, which wrap whatever is
    raised inside them, one group per level: "unhandled errors in a
    TaskGroup" says nothing about what failed. Of several, the one that
    decides what the command does comes first: an interrupt or an exit,
    then a refusal (the connection ledger's), then an error okg names, then
    the first of the rest. Anything that is not a group is returned as it
    is, with a count of one.
    """
    from okg.substrate.db.concurrency_budget import BudgetError

    leaves: list[BaseException] = []

    def walk(node: BaseException) -> None:
        inner = getattr(node, "exceptions", None)
        if isinstance(node, BaseExceptionGroup) and inner:
            for child in inner:
                walk(child)
        else:
            leaves.append(node)

    walk(exc)

    def rank(leaf: BaseException) -> int:
        if not isinstance(leaf, Exception):
            return 0
        if isinstance(leaf, BudgetError):
            return 1
        if isinstance(leaf, SubstrateError):
            return 2
        return 3

    return min(leaves, key=rank), len(leaves)


#: How the MCP client library starts the error it raises when a served
#: reply breaks the output schema the tool declares.
_SHAPE_MISMATCH = "Invalid structured content returned by tool "


def _unexpected(
    surface: str, step: str, exc: BaseException, *, others: int = 0,
) -> SubstrateError:
    """The CLI error for a failure nothing names: one line, the cause's own
    first line, with no directory of the host."""
    text = str(exc).strip()
    first = " ".join(text.splitlines()[0].split()) if text else type(exc).__name__
    data: dict[str, Any] = {"surface": surface, "exception": type(exc).__name__}
    if isinstance(exc, RuntimeError) and first.startswith(_SHAPE_MISMATCH):
        tool, _, why = first[len(_SHAPE_MISMATCH):].partition(": ")
        first = (
            f"the reply of the {tool} tool did not match the output shape "
            f"the tool declares: {why}"
        )
        data = {}
    if others:
        first += f" (and {others} more error{'s' if others > 1 else ''})"
    err = SubstrateError(f"{surface} could not {step}: {first}", data=data)
    err.code = "read_command_failed"
    return err


def report_failure(
    surface: str, step: str, raised: BaseException, *, as_json: bool,
) -> int:
    """Report what stopped a read command at `step`; the exit code.

    An interrupt or an exit, and a database connect failure, are raised
    again for the caller (`guarded` reports the second).
    """
    from okg.substrate.db.concurrency_budget import BudgetError

    def emit(err: SubstrateError, *, as_json: bool) -> int:
        return _read_render.render_failure(err, as_json=as_json).write()

    exc, count = single_cause(raised)
    if not isinstance(exc, Exception):
        # An interrupt or an exit, also one a task group wrapped: it
        # ends the command as itself, after the cleanup around this.
        raise exc
    if isinstance(getattr(exc, "okg_connect_target", None), dict):
        # `guarded` turns a database connect failure into its own error.
        raise exc
    if debug_enabled():
        traceback.print_exception(raised, file=sys.stderr)
    if isinstance(exc, BudgetError):
        # The host-wide ledger has no room for this command's two
        # connections. Nothing was opened; the caller can try again.
        line = one_line(f"{surface}: error: BudgetError: {exc}")
        return _read_render.render_busy(
            _unexpected(surface, step, exc), line, as_json=as_json,
        ).write()
    if isinstance(exc, SubstrateError):
        return emit(exc, as_json=as_json)
    return emit(
        _unexpected(surface, step, exc, others=count - 1), as_json=as_json,
    )


def run_operator(
    operator: str,
    arguments: dict[str, Any],
    args: argparse.Namespace,
    *,
    as_json: bool,
    embedder_spec: str | None = None,
    refusal_hint: str | None = None,
    driver: Any = None,
) -> int:
    """Build the server, make the one call, print the reply.

    `refusal_hint` is one more line for stderr after a reply the TOOL
    refused, in text output; no other failure is followed by it. A text
    refusal that carries a corrected call is followed first by `retry: okg
    ...`. `driver(call, server)`, when given, makes the calls and prints the
    replies itself while the server is up (`_read_shell.drive`); its exit
    code is the command's.
    """
    surface = f"okg {operator}"

    def failed(step: str, raised: BaseException) -> int:
        return report_failure(surface, step, raised, as_json=as_json)

    timing = Timing(surface, parse_seconds=getattr(args, PARSE_SECONDS_ATTR, None))
    deployment = str(getattr(args, "deployment", None) or "default")
    try:
        with quiet_stderr(), contextlib.ExitStack() as stop:
            with timing.phase("load"):
                stop.enter_context(released_on_stop(deployment, surface))
            try:
                with timing.phase("build"):
                    server = build_read_server(
                        args, surface=surface, embedder_spec=embedder_spec,
                        dataset_card=operator in CARD_OPERATORS,
                    )
            except BaseException as exc:  # noqa: BLE001 - `failed` re-raises an interrupt
                return failed("start the read server", exc)
            try:
                try:
                    with timing.phase("call"):
                        if driver is not None:
                            return driver(
                                lambda sent, result_format: call_operator(
                                    server, operator, sent, result_format=result_format,
                                ),
                                server,
                            )
                        result = call_operator(
                            server, operator, arguments,
                            result_format="json" if as_json else "text",
                        )
                except BaseException as exc:  # noqa: BLE001 - `failed` re-raises an interrupt
                    return failed("complete the call", exc)
            finally:
                with timing.phase("close"):
                    close_server(server)
        with timing.phase("print"):
            return _read_render.render_reply(
                operator, _read_render.reply_of(result), as_json=as_json,
                arguments=arguments, args=args, refusal_hint=refusal_hint,
            ).write()
    finally:
        timing.report()


#: The exit code of a command ended by Ctrl-C, as a shell reports one (128 +
#: SIGINT).
INTERRUPTED_EXIT = 130


def _forget_unhandled_interrupt() -> None:
    """Clear CPython's record that a KeyboardInterrupt went unhandled.

    CPython runs code given as a string (`exec` or `eval` of a str, not of
    a code object) through `run_eval_code_obj` in Python/pythonrun.c. That
    function resets `_PyRuntime.signals.unhandled_keyboard_interrupt` on
    entry and sets it when a KeyboardInterrupt leaves the code, even when a
    caller catches the interrupt later. `Py_RunMain` (Modules/main.c) reads
    the flag at exit and, when it is set, calls `exit_sigint()`: under
    `python -m` the process then ends by SIGINT (exit -2) instead of with
    the exit code it was given. Ctrl-C while the read server is built often
    lands inside the `exec` of a string that `dataclasses` runs for each
    class a module import defines. Running the empty string once goes
    through the same function and resets the flag.
    """
    exec("")


def emit_failure(err: SubstrateError, *, as_json: bool) -> int:
    """Print a failure (`_read_render.render_failure`); exit 1."""
    return _read_render.render_failure(err, as_json=as_json).write()


def guarded(handler: Any) -> Any:
    """A read command's handler, with nothing left to escape as a traceback.

    Whatever the handler does not report itself is reported here: a
    database connect failure as the error `okg` gives every command, an
    error okg names as itself, anything else as one line. Ctrl-C (SIGINT)
    ends the command with exit 130 and one line that says it was
    interrupted, after the cleanup inside the handler has closed the pool
    and released the claim; Python would print a traceback for it. All of it
    goes through `mask`. With `OKG_READ_DEBUG=1` the exception is left to
    propagate, so its traceback is printed.
    """

    def run(args: argparse.Namespace, *, as_json: bool) -> int:
        _emit_error = emit_failure

        try:
            return handler(args, as_json=as_json)
        except KeyboardInterrupt:
            if debug_enabled():
                raise
            command = str(getattr(args, "command", None) or "read")
            stopped = SubstrateError(f"okg {command} was interrupted")
            stopped.code = "read_command_failed"
            _emit_error(stopped, as_json=as_json)
            _forget_unhandled_interrupt()
            return INTERRUPTED_EXIT
        except Exception as exc:  # noqa: BLE001 - the last resort of a read command
            if debug_enabled():
                raise
            command = str(getattr(args, "command", None) or "read")
            target = getattr(exc, "okg_connect_target", None)
            if isinstance(target, dict):
                from okg.substrate.cli.main import _database_connect_error

                return _emit_error(
                    _database_connect_error(command, exc, target), as_json=as_json,
                )
            if isinstance(exc, SubstrateError):
                return _emit_error(exc, as_json=as_json)
            return _emit_error(
                _unexpected(f"okg {command}", "finish", exc), as_json=as_json,
            )

    return run


def cmd_operator(args: argparse.Namespace, *, as_json: bool) -> int:
    """`okg inspect|expand|filter|aggregate|map|query`."""
    return run_parsed(str(args.command), args, as_json=as_json)


def run_parsed(operator: str, args: argparse.Namespace, *, as_json: bool) -> int:
    """Parse the command line (`_read_call.parse_read_call`), make the call
    (`run_call`) and print the reply (`_read_render`). A command line that
    cannot be read as one call is refused, exit 2, before any start-up
    cost."""
    from okg.substrate.cli._read_call import ReadCallRefused, parse_read_call

    try:
        call = parse_read_call(operator, args, as_json=as_json)
    except ReadCallRefused as refused:
        return _read_render.render_refusal(
            refused.error, refused.operator, args, mode=refused.mode,
        ).write()
    return run_call(call, args)


def run_call(call: Any, args: argparse.Namespace) -> int:
    """Run a parsed `ReadCall` on a read-only server built in this process:
    through the shell layer (`_read_shell`) when the command used one of its
    flags or bare forms, otherwise as one call."""
    from okg.substrate.cli import _read_shell

    if not call.plan.shell:
        return run_operator(
            call.operator, call.arguments, args, as_json=call.choices.as_json,
            embedder_spec=call.embedder_spec, refusal_hint=call.choices.refusal_hint,
        )
    plan = call.plan
    return run_operator(
        call.operator, call.arguments, args, as_json=call.choices.as_json,
        embedder_spec=call.embedder_spec, refusal_hint=call.choices.refusal_hint,
        driver=lambda sent, server: _read_shell.drive(plan, args, sent, server),
    )


def run_planned(
    operator: str,
    arguments: dict[str, Any],
    args: argparse.Namespace,
    *,
    as_json: bool,
    embedder_spec: str | None = None,
    refusal_hint: str | None = None,
) -> int:
    """`run_operator`, through the shell layer (`_read_shell`) when the
    command used one of its flags or bare forms; otherwise exactly as
    before. A shell form refused from the command line alone exits 2
    before any start-up cost."""
    from okg.substrate.cli._read_call import ReadCallRefused, plan_read_call

    try:
        call = plan_read_call(
            operator, arguments, args, as_json=as_json,
            embedder_spec=embedder_spec, refusal_hint=refusal_hint,
        )
    except ReadCallRefused as refused:
        return _read_render.render_refusal(
            refused.error, refused.operator, args, mode=refused.mode,
        ).write()
    return run_call(call, args)


#: The words a SQL statement opens with. The `query` tool serves SELECT,
#: WITH, VALUES and a parenthesised statement and refuses the rest in its
#: own words; none of them is a text search.
_SQL_OPENING = re.compile(
    r"(?:select|with|values|explain|table|show|insert|update|delete|merge|"
    r"create|alter|drop|truncate|grant|revoke|set|reset|begin|start|commit|"
    r"rollback|copy|call|do|analyze|vacuum|lock|declare|fetch|prepare|"
    r"execute|listen|notify|comment|refresh)\b|\(",
    re.IGNORECASE,
)
_SQL_LEADING = re.compile(r"(?:\s+|--[^\n]*(?:\n|$)|/\*.*?\*/)*", re.DOTALL)
TEXT_SEARCH_HINT = (
    'hint: `okg query` takes SQL; a text search is `okg search "<words>"`'
)


def looks_like_sql(text: Any) -> bool:
    """True when `text`, after white space and comments, opens as a SQL
    statement. Anything else is words."""
    if not isinstance(text, str):
        return True  # not text at all: the tool's refusal says what it is
    rest = text[_SQL_LEADING.match(text).end():]
    return _SQL_OPENING.match(rest) is not None


def cmd_search_operator(args: argparse.Namespace, *, as_json: bool) -> int:
    """`okg search` when it was given anything only the MCP tool takes."""
    return run_parsed("search", args, as_json=as_json)


def cmd_guide(args: argparse.Namespace, *, as_json: bool) -> int:
    """`okg guide`: the instructions the MCP server sends at connect.

    The server is built exactly as a read command builds it (the same
    checks, the same read role, the dataset card from the database), its
    instructions are taken and it is closed; no tool is called. The text is
    printed unmasked, after the shell's pipeline lead and a rule: it is what
    `initialize` gives a read-only client of this deployment. A write-enabled or tool-narrowed launch is sent
    different instructions. With --json: the text, its size in bytes and its sha256, the
    same hash `inspect` with target kind `session_start` reports as
    `instructions_hash`.
    """
    from okg.substrate.mcp.surface_freshness import served_instructions_fields

    surface = f"okg {GUIDE_COMMAND}"
    missing = require_deployment(args, GUIDE_COMMAND, as_json=as_json)
    if missing is not None:
        return missing
    with quiet_stderr(), contextlib.ExitStack() as stop:
        stop.enter_context(released_on_stop(str(args.deployment), surface))
        try:
            server = build_read_server(args, surface=surface)
        except BaseException as exc:  # noqa: BLE001 - `report_failure` re-raises an interrupt
            return report_failure(surface, "start the read server", exc, as_json=as_json)
        try:
            # None is what a server built with no instructions holds; a
            # client is then sent none, which is the empty text.
            text = server.instructions or ""
        finally:
            close_server(server)
    if as_json:
        fields = served_instructions_fields(text)
        print(json.dumps({
            "text": text,
            "bytes": fields["instructions_bytes"],
            "sha256": fields["instructions_hash"],
        }))
    else:
        # The shell lead first (#3666); the served text after the rule, as
        # served. --json keeps the served text alone, for its hash.
        sys.stdout.write(pipeline_text() + "\n\n" + GUIDE_INSTRUCTIONS_RULE + "\n")
        sys.stdout.write(text if text.endswith("\n") else text + "\n")
    return 0


# ── parsers ──────────────────────────────────────────────────────────────

_ARGS_HELP = (
    "the whole argument object as JSON, @PATH to read it from a file, or - "
    "for standard input. An argument given here and as its own flag is "
    "refused"
)


_DEPLOYMENT_HELP = (
    f"the deployment to read. When omitted: ${DEPLOYMENT_ENV}, then the "
    "checkout's configured default (graph_projection.deployment). One is "
    "required, and the flag wins"
)


def add_deployment_flag(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--deployment", default=None, help=_DEPLOYMENT_HELP)


def print_own_usage_on_unknown_arguments(parser: argparse.ArgumentParser) -> None:
    """Report an argument the command does not take with its own usage.

    argparse hands a subcommand's unrecognized arguments back to the
    top-level parser, whose error prints the list of every okg command. The
    subparsers action calls `parse_known_args` on the command's parser, so
    that one method is wrapped on this parser: the error is the command's own
    usage and one line, on stderr, exit 2.

    okg also wraps every parser's `error` to print a line of its own before
    the usage (`install_actionable_errors_tree`), which would state the
    error twice here. That wrap skips a parser that already carries an
    example, so this parser is given one and keeps argparse's own `error`:
    the usage, then the one error line.
    """
    parser._okg_actionable_example = f"{parser.prog} --help"  # type: ignore[attr-defined]
    parse = parser.parse_known_args

    def parse_known_args(args: Any = None, namespace: Any = None) -> Any:
        parsed, unknown = parse(args, namespace)
        if unknown:
            flags = [
                flag for action in parser._actions for flag in action.option_strings
                if flag.startswith("--")
            ]
            message = "unrecognized arguments: " + " ".join(unknown)
            for token in unknown:
                if not token.startswith("--"):
                    continue
                typed = token.split("=", 1)[0]
                # A prefix of exactly one flag, else a near spelling.
                near = [flag for flag in flags if flag.startswith(typed)]
                if len(near) != 1:
                    near = difflib.get_close_matches(typed, flags, n=1, cutoff=0.75)
                if near:
                    message += f" (did you mean {near[0]}?)"
                    break
            parser.error(message)
        return parsed, unknown

    parser.parse_known_args = parse_known_args  # type: ignore[method-assign]


def abbreviate_only(
    parser: argparse.ArgumentParser, *, never: list[argparse.Action],
) -> None:
    """Let a flag prefix resolve to any flag except those in `never`.

    `okg search` accepted prefixes (`--dep`, `--em`) before it gained the MCP
    tool's flags. A prefix keeps the meaning it had then: it is matched
    against the earlier flags only, so it can neither resolve to a newer flag
    nor become ambiguous because of one. A newer flag is typed in full.
    """
    matches = parser._get_option_tuples

    def option_tuples(option_string: str) -> list[Any]:
        return [
            match for match in matches(option_string)
            if not any(match[0] is action for action in never)
        ]

    parser._get_option_tuples = option_tuples  # type: ignore[method-assign]


class ReadHelpFormatter(argparse.HelpFormatter):
    """argparse's formatter, except that a description or epilog holding a
    line break is printed as written: a tool's description keeps its lines
    and an example its indentation. Text without one is wrapped as usual."""

    def _fill_text(self, text: str, width: int, indent: str) -> str:
        if "\n" in text:
            return "".join(indent + line for line in text.splitlines(keepends=True))
        return super()._fill_text(text, width, indent)


def _read_help() -> dict[str, dict[str, Any]]:
    """The generated help table, imported when the parser is built, so that
    `scripts/gen_read_help_text.py` can import this module without it.
    Without the table every okg command that builds the full parser fails:
    `register_subparsers` calls `tool_description` for each read command."""
    from okg.substrate.cli._read_help_text import READ_HELP

    return READ_HELP


def tool_description(operator: str) -> str:
    """The MCP tool's generic description, as a server built with no
    database lists it (generated)."""
    return "\n".join(_read_help()[operator]["description"])


def flag_help(operator: str, argument: str) -> str:
    """A flag's help: the argument's type and allowed values or bounds, from
    the tool's published input schema (generated). argparse formats a help
    string with `%`, so a literal one is doubled."""
    return _read_help()[operator]["arguments"][argument].replace("%", "%%")


def example_text(operator: str) -> str:
    """One worked call: the command line, and the argument object it sends."""
    entry = _read_help()[operator]
    command = shlex.join(["okg", operator, *entry["example_argv"]])
    sent = json.dumps(entry["example_arguments"])
    return f"Example:\n  {command}\nsends the {operator} tool:\n  {sent}"


_FLAGS_HELP = textwrap.fill(
    "Every argument of the tool is a flag of the same name; each flag's line "
    "gives the type and the allowed values the tool publishes for it. A value "
    "is JSON when it parses as JSON, otherwise a string; a list argument also "
    "takes a comma-separated list. Output is the reply text, or the JSON "
    "reply with --json. Exit 0 when served, 2 when refused (the refusal is "
    "printed on stderr), 1 on any other failure. A served call prints "
    "nothing on stderr but, past 20 rows in text, one hint line to pipe "
    "them. This help is generic text; the deployment's server "
    "may name its own types and offer fewer search methods.",
    width=78,
    break_on_hyphens=False,
)


#: The one shell tip of `okg filter --help`, `okg inspect --help` and `okg
#: guide --help` (#3666 step S5): agents read a set's texts one call per
#: record, or one batch target of 8 per call, where one pipe reads them all.
TEXTS_PIPE_TIP = "all texts of a set: okg filter ... --ids --all | okg inspect texts -"

#: `okg inspect texts -` worked through: the pipe from a set to a count.
TEXTS_PIPE_EXAMPLE = (
    "Example, every text of a set as JSON Lines (node_id, state, text, coverage;\n"
    "\"cut\": true on a text not delivered whole; exit 2 when a text is cut or\n"
    "refused), counted by a script that names the texts it could not count:\n"
    "  okg filter --type doc --where 'attrs.path~docs/' --ids --all \\\n"
    "    | okg inspect texts - \\\n"
    "    | python3 -c 'import json, sys; rs = [json.loads(line) for line in sys.stdin]; "
    "print(sum(r[\"text\"].count(\"TODO\") for r in rs if r[\"state\"] == \"delivered\"), "
    "\"not whole:\", [r[\"node_id\"] for r in rs if r.get(\"cut\") or r[\"state\"] == \"refused\"])'"
)

#: The pipeline lead (#3666): what `okg guide` prints first and what every
#: read command's --help opens with. Shell agents answered counts, joins and
#: lists by reading rows into context, one command per lookup (19 to 26
#: commands and 0.6M to 0.8M tokens a question; 6% of commands piped); one
#: pipe into a program reads every page and prints only the answer.
PIPELINE_LEAD = (
    "Counts, joins and lists: pipe --jsonl or --ids into a program instead of\n"
    "reading rows. --all reads every page; the program prints only the answer."
)

#: Each example is a question's shape, a command line, and is run exactly as
#: printed against a published graph by
#: `test_the_pipeline_examples_run_as_printed`
#: (tests/substrate/cli/test_read_operator_commands_db.py).
PIPELINE_COUNT = (
    "okg filter --type ticket --where 'attrs.status!=Closed' --all --jsonl"
    " | python3 -c 'import json, sys, collections; "
    "c = collections.Counter(json.loads(l)[\"attrs\"][\"status\"] for l in sys.stdin); "
    "print(sum(c.values()), c.most_common())'"
)
PIPELINE_JOIN = (
    "okg expand ticket:T-1 --direction in --edge-type mentions --ids --all"
    " | okg filter --stdin --all --jsonl"
    " | python3 -c 'import json, sys; "
    "print(\"\\n\".join(r[\"node_id\"] + \" \" + r[\"attrs\"][\"status\"] "
    "for r in map(json.loads, sys.stdin)))'"
)
PIPELINE_TEXTS = (
    "okg filter --type ticket --where attrs.status=Open --ids --all"
    " | okg inspect texts -"
    " | python3 -c 'import json, sys; rs = [json.loads(l) for l in sys.stdin]; "
    "print([r[\"node_id\"] for r in rs if \"timeout\" in (r[\"text\"] or \"\")], "
    "\"unread:\", [r[\"node_id\"] for r in rs if r.get(\"cut\") or r[\"state\"] != \"delivered\"])'"
)
PIPELINE_EXAMPLES: tuple[tuple[str, str], ...] = (
    ("open tickets, counted by status", PIPELINE_COUNT),
    ("tickets that mention ticket:T-1, with their status (a join)", PIPELINE_JOIN),
    ("open tickets whose text names a word", PIPELINE_TEXTS),
)


def pipeline_text() -> str:
    """The lead and its examples, each pipe stage on its own line."""
    lines = [PIPELINE_LEAD]
    for question, command in PIPELINE_EXAMPLES:
        lines.append(f"  # {question}")
        lines.append("  " + command.replace(" | ", " \\\n    | "))
    return "\n".join(lines)


#: The line between the pipeline lead and the served instructions in what
#: `okg guide` prints.
GUIDE_INSTRUCTIONS_RULE = (
    "--- The instructions an MCP client of this deployment is sent "
    "(okg guide --json gives them alone):"
)

#: Shell text after the generated example of a command's help.
SHELL_EPILOG: dict[str, str] = {
    "inspect": "\n\n" + TEXTS_PIPE_EXAMPLE + "\n\n" + TEXTS_PIPE_TIP,
    "filter": "\n\n" + TEXTS_PIPE_TIP,
}


def _add_argument_flag(parser: argparse.ArgumentParser, operator: str, argument: str) -> None:
    parser.add_argument(
        flag_of(argument),
        dest=_DEST + argument,
        default=None,
        metavar="VALUE",
        help=flag_help(operator, argument),
    )


def add_search_operator_flags(parser: argparse.ArgumentParser) -> None:
    """Add the MCP-only `search` arguments to the `okg search` parser, and
    the MCP tool's description and an example to its help."""
    group = parser.add_argument_group(
        "MCP search tool",
        "Any of these runs the MCP search tool itself and prints its reply; "
        "so does --method identifier_mentions, attrs or grep. The flags "
        "above that the tool also takes keep their meaning: --subtype is "
        "subtype_filter, --scope is profile, --attr-filter is attr_filters, "
        "--no-concept-expansion is expand_concepts false. The tool's own "
        "`scope` argument is given through --args. "
        "--nomos-access-context-json is refused here: use --authority.",
    )
    added = [
        group.add_argument(
            "--args", dest="operator_args", default=None, metavar="JSON", help=_ARGS_HELP,
        )
    ]
    from okg.substrate.cli import _read_shell

    for argument in SEARCH_OPERATOR_FLAGS:
        added.append(group.add_argument(
            flag_of(argument),
            dest=_DEST + argument,
            default=None,
            metavar="VALUE",
            help=flag_help("search", argument),
            # `--where` repeats; its help gains the k=v terms below.
            action=_read_shell.search_flag_action(argument),
        ))
        _read_shell.extend_search_flag_help(added[-1])
    added += _read_shell.add_output_flags(group)
    parser.formatter_class = ReadHelpFormatter
    # The pipeline lead first; the description it had, wrapped here, since
    # a text holding a line break is printed as written.
    parser.description = pipeline_text() + "\n\n" + textwrap.fill(
        parser.description or "", width=78, break_on_hyphens=False,
    )
    parser.epilog = (
        "The MCP search tool, which the group above runs, in generic text "
        "(the deployment's server may name its own types and offer fewer "
        "search methods):\n\n" + tool_description("search") + "\n\n"
        + example_text("search")
    )
    # Every flag added above is newer than the command's prefixes: call this
    # function after the flags `okg search` already had.
    abbreviate_only(parser, never=added)
    print_own_usage_on_unknown_arguments(parser)


def _add_output_flags(parser: argparse.ArgumentParser, add_json_flag: Any) -> None:
    if add_json_flag is not None:
        add_json_flag(parser)
    else:
        parser.add_argument("--json", action="store_true", default=argparse.SUPPRESS)
    add_deployment_flag(parser)
    parser.add_argument("--dsn", default=None)


def register_subparsers(sub: Any, *, add_json_flag: Any = None) -> None:
    """Register one command per read operator other than `search`, and
    `okg guide`."""
    for operator in OPERATOR_COMMANDS:
        intro = textwrap.fill(
            f"Run the MCP `{operator}` tool against the deployment and print "
            "its reply. The tool in generic text; the deployment's server may "
            "name its own types and offer fewer search methods:",
            width=78,
            break_on_hyphens=False,
        )
        parser = sub.add_parser(
            operator,
            help=_HELP[operator],
            description=pipeline_text() + "\n\n" + intro + "\n\n" + tool_description(operator),
            epilog=_FLAGS_HELP + "\n\n" + example_text(operator) + SHELL_EPILOG.get(operator, ""),
            formatter_class=ReadHelpFormatter,
            allow_abbrev=False,
        )
        _add_output_flags(parser, add_json_flag)
        parser.add_argument("--args", dest="operator_args", default=None, metavar="JSON", help=_ARGS_HELP)
        positional = POSITIONAL.get(operator)
        if positional is not None:
            argument, metavar = positional
            parser.add_argument(
                "operator_positional",
                nargs="?",
                default=None,
                metavar=metavar,
                help=f"the `{argument}` argument, taken as the text typed",
            )
        for argument in OPERATOR_ARGUMENTS[operator]:
            _add_argument_flag(parser, operator, argument)
        from okg.substrate.cli._read_shell import add_shell_flags

        add_shell_flags(parser, operator)
        print_own_usage_on_unknown_arguments(parser)

    guide = sub.add_parser(
        GUIDE_COMMAND,
        help="Print the instructions an MCP client gets when it connects",
        description=pipeline_text() + "\n\n" + textwrap.fill(
            "Print the lead above, then the instructions a read-only MCP "
            "client of the deployment is sent when it connects: the dataset "
            "card, okg's guidance, the deployment's own guidance and what "
            "this server cannot do. The server is built as a read command "
            "builds it; no tool is called. A write-enabled or tool-narrowed "
            "launch is sent different instructions. With --json: an object "
            "with the served text alone, its bytes and its sha256 (the "
            "instructions_hash that inspect reports for target kind "
            "session_start).",
            width=78,
            break_on_hyphens=False,
        ),
        epilog="Shell tip:\n  " + TEXTS_PIPE_TIP,
        formatter_class=ReadHelpFormatter,
        allow_abbrev=False,
    )
    _add_output_flags(guide, add_json_flag)
    print_own_usage_on_unknown_arguments(guide)


DISPATCH = {
    **{operator: guarded(cmd_operator) for operator in OPERATOR_COMMANDS},
    GUIDE_COMMAND: guarded(cmd_guide),
}
