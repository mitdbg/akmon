"""Shell forms and pipe output of the okg read commands (#3666, step S2).

`_read_operators` turns flags into one MCP argument object and prints the
reply. This module adds what a shell caller types and what a pipe reads,
without a new operator, a new condition language or a change to any reply:

* `--where k=v` (repeatable, joined by and; also `!=`, `>`, `<`, `>=`, `<=`
  and `~` for a prefix) on filter, map, aggregate, expand and search, each
  term written as the short or long condition the tools already accept
  (`okg.substrate.mcp.read_grammar`). A JSON `--where` is sent as typed.
* The bare forms agents typed: `--target <node id>`, `--input <subtype>`,
  `--input '{"subtype": X}'` and `--type X`, each rewritten to the one
  argument object it can only mean, with an `! adjusted:` note on stderr in
  the words step 2a uses. A bare word that is not a declared subtype is
  refused, never guessed.
* Renderers over the same reply: `--jsonl` (one row per line, as the JSON
  reply carries it), `--ids` (one id per line) and `--all` (every page,
  following `next` to the end, or refused when the reply gives no way on).
  In those modes the status line, notes and refusals go to stderr.
* Ids from standard input (`-` or `--stdin`): `inspect` reads them through
  the batch read, 50 per call; `filter` and `map` as `node_id` any of; and
  `expand` one call per id.
* `okg inspect texts -` (step S5): the whole text of each id piped in, one
  JSON line per text, read through the `texts` batch target, 8 ids per call
  and one call at a time, following `next` until every text is delivered;
  `--lines` reads chosen lines of each. A text the reads could not deliver
  whole is printed with `"cut": true` and said on stderr.
* A refusal that carries a corrected call ends with `retry: okg ...`, a
  shell-quoted command line that runs as given. Exit 0 served (also with a
  note), 2 refused, 1 an error.
"""
from __future__ import annotations

import argparse
import difflib
import json
import os
import re
import shlex
import sys
from dataclasses import dataclass, field
from typing import Any, Callable, Optional

from okg.substrate.errors import SubstrateError

#: Most pages one `--all` command reads. At the cap the command stops with
#: a partial result, not a refusal: what was read stays printed (marked not
#: complete under --json), stderr says how many rows and gives a `resume:`
#: call, and the exit is 0.
MAX_ALL_PAGES = 200
#: Ids per `inspect` call from standard input: the batch read's own limit
#: (`operator_models.MAX_NODE_BATCH`; a test pins the two).
INSPECT_STDIN_BATCH = 50
#: Ids per `okg inspect texts` call: the `texts` target's own limit
#: (`operator_models.TTexts.items`; a test pins the two).
TEXTS_STDIN_BATCH = 8
#: Most calls one batch of texts makes, its first and every `next`. A text
#: not delivered by then is printed as cut, and stderr says so.
MAX_TEXT_PAGES = 1000
#: Most ids `okg inspect texts` takes from standard input: 1,250 calls at
#: least, each sequential; a larger set is split by the caller.
MAX_TEXTS_STDIN_IDS = 10_000
#: The word after `okg inspect` that reads the texts of the ids piped in.
TEXTS_WORD = "texts"
#: Ids per `filter` or `map` call from standard input, as one `node_id` any
#: of condition.
SET_STDIN_BATCH = 500
#: Most ids `expand` takes from standard input: it makes one call per id.
MAX_EXPAND_STDIN_IDS = 200
#: The exit code of a command whose reader closed the pipe (128 + SIGPIPE),
#: as a shell reports `yes | head`.
BROKEN_PIPE_EXIT = 141
REFUSED_EXIT = 2

#: Commands that take `--where` from this module. `okg search` has its own
#: `--where`, read by the same parser.
WHERE_COMMANDS: tuple[str, ...] = ("filter", "map", "aggregate", "expand")
#: Commands that take `--type`.
TYPE_COMMANDS: tuple[str, ...] = ("filter", "map", "aggregate")
#: Commands that read ids from standard input.
STDIN_COMMANDS: tuple[str, ...] = ("inspect", "filter", "map", "expand")
#: Commands whose `--input` names a set.
SET_COMMANDS: tuple[str, ...] = ("filter", "map", "aggregate")

#: The list of rows in each tool's JSON reply.
ROWS_KEY: dict[str, str] = {
    "search": "results", "expand": "neighbors", "filter": "items", "map": "items",
    "aggregate": "groups", "query": "rows",
}

#: Parsed attributes of the flags this module adds.
JSONL_ATTR = "shell_jsonl"
IDS_ATTR = "shell_ids"
ALL_ATTR = "shell_all"
STDIN_ATTR = "shell_stdin"
WHERE_ATTR = "shell_where"
TYPE_ATTR = "shell_type"
FROM_ATTR = "shell_from"
#: `okg inspect`'s second positional: the `-` of `okg inspect texts -`.
MORE_ATTR = "shell_from_more"
LINES_ATTR = "shell_lines"
#: The `--deployment` value as typed, before the environment or the
#: checkout's default filled it in; a retry line names it only when typed.
TYPED_DEPLOYMENT_ATTR = "read_typed_deployment"

#: A line of standard input read as an id: no white space, no quote, and
#: not opening as JSON, a note (`!`) or a numbered row (`[1]`).
_PLAIN_ID = re.compile(r"""(?![\[\]{}!#'"])[^\s'"`]{1,2048}\Z""")
#: A word that can only be a subtype name: no `:`, no space, no quote.
_SUBTYPE_WORD = re.compile(r"[A-Za-z_][A-Za-z0-9_\-]{0,95}\Z")


class ShellRefusal(SubstrateError):
    """A shell form the command will not read as exactly one call.

    Printed in the shape of a tool's refusal and ended with exit 2. `retry`
    is `(tool, arguments)` for a whole call that is served as written, or
    None; `retry_mode` is `corrected` or `guess`, as on a tool's
    `retry_call`.
    """

    def __init__(
        self,
        code: str,
        message: str,
        *,
        fix_hint: str,
        retry: Optional[tuple[str, dict[str, Any]]] = None,
        retry_mode: str = "corrected",
        **data: Any,
    ) -> None:
        super().__init__(message, data=dict(data), fix_hint=fix_hint)
        self.code = code
        self.retry = retry
        self.retry_mode = retry_mode
        #: The `retry:` line as the caller typed the command, with only the
        #: refused part replaced (`typed_command`); when set it is printed
        #: instead of a line built from `retry`.
        self.retry_command: Optional[str] = None


# ── --where k=v ──────────────────────────────────────────────────────────

_FIELD = r"[A-Za-z0-9_][A-Za-z0-9_.:\-]{0,95}"
_TERM = re.compile(
    rf"\A\s*(?P<field>{_FIELD})\s*(?P<op>!=|>=|<=|=|>|<|~)(?P<value>.*)\Z", re.DOTALL,
)
_NUMBER = re.compile(r"-?(?:0|[1-9][0-9]*)(?:\.[0-9]+)?(?:[eE][+-]?[0-9]+)?\Z")
#: A value that goes on into another condition, as SQL would write it.
_SQL_TAIL = re.compile(
    rf"\s(?:and|or)\s+{_FIELD}\s*(?:!=|>=|<=|=|>|<|~|\blike\b|\bin\b)", re.IGNORECASE,
)
_OPS = "!=, >=, <=, =, >, < or ~"


class WhereRefusal(Exception):
    """One `--where` term that is not a clean `k OP v`.

    `corrected` is the whole condition with that term written the one way
    it can mean, or None when it can mean more than one thing.
    """

    def __init__(self, message: str, fix_hint: str, term: str) -> None:
        super().__init__(message)
        self.message = message
        self.fix_hint = fix_hint
        self.term = term
        self.corrected: Any = None

    def refusal(
        self,
        operator: str,
        retry_arguments: Optional[dict[str, Any]],
        args: Optional[argparse.Namespace] = None,
    ) -> ShellRefusal:
        """The refusal; with `args`, its retry line is the command as typed
        (every flag, `--type`, `--stdin` and the output flags included) with
        the `--where` terms replaced by the corrected JSON condition."""
        refused = ShellRefusal(
            "read_where_invalid",
            f"okg {operator}: {self.message}",
            fix_hint=self.fix_hint,
            retry=(operator, retry_arguments) if retry_arguments is not None else None,
            term=self.term,
        )
        if self.corrected is not None and args is not None:
            refused.retry_command = typed_command(operator, args, where=self.corrected)
        return refused


def _scalar(text: str) -> Any:
    """`true`, `false`, `null` and a JSON number as themselves; a JSON
    string in double quotes as that string; anything else as the text."""
    if text in ("true", "false", "null"):
        return json.loads(text)
    if _NUMBER.match(text):
        return json.loads(text)
    if len(text) >= 2 and text[0] == '"' and text[-1] == '"':
        try:
            value = json.loads(text)
        except ValueError:
            return text
        if isinstance(value, str):
            return value
    return text


def _one_json_string(text: str) -> bool:
    if len(text) < 2 or text[0] != '"' or text[-1] != '"':
        return False
    try:
        return isinstance(json.loads(text), str)
    except ValueError:
        return False


_ITEM = re.compile(r"""\s*(?:"((?:[^"\\]|\\.)*)"|'([^']*)'|([^,"']*?))\s*(?:,|\Z)""")


def _quoted_items(raw: str) -> Optional[list[Any]]:
    """The items of a comma list whose items carry quotes (`"a","b"`,
    `'a','b'`, `a,"b,c"`), or None when it is not one such list."""
    if raw.rstrip().endswith(","):
        return None
    items: list[Any] = []
    at = 0
    while at < len(raw):
        match = _ITEM.match(raw, at)
        if match is None or match.end() == at:
            return None
        double, single, bare = match.groups()
        if double is not None:
            try:
                items.append(json.loads(f'"{double}"'))
            except ValueError:
                return None
        elif single is not None:
            items.append(single)
        else:
            if not bare.strip():
                return None
            items.append(_scalar(bare.strip()))
        at = match.end()
    return items


def _spelled(condition: Any) -> str:
    return json.dumps(condition, ensure_ascii=False)


def _absent(field_name: str) -> dict[str, Any]:
    return {"op": "not", "clause": {field_name: {"op": "exists"}}}


def _term_clause(term: str) -> dict[str, Any]:
    """One `k OP v` term as the short or long condition the tools take."""
    match = _TERM.match(term)
    if match is None:
        raise WhereRefusal(
            f"--where {term!r} is neither JSON nor one field, an operator "
            f"({_OPS}) and a value",
            "write one condition per --where, for example --where "
            "attrs.status=Open --where 'date.created>=2026-01-01', or the "
            "whole condition as JSON: --where '{\"attrs.status\": \"Open\"}'",
            term,
        )
    name, op, raw = match.group("field"), match.group("op"), match.group("value").strip()

    def corrected_as(clause: dict[str, Any], why: str) -> WhereRefusal:
        refusal = WhereRefusal(
            f"--where {term!r} {why}",
            f"write --where {shlex.quote(_spelled(clause))}",
            term,
        )
        refusal.corrected = clause
        return refusal

    if raw[:1] in ("=", "<", ">", "!", "~"):
        rest = raw[1:].strip()
        if op == "=" and raw[:1] == "=" and rest and rest[:1] not in "=<>!~" and not _SQL_TAIL.search(rest):
            # `k==v`: one reading, the equality (of the text inside SQL's
            # quotes, when it is quoted so).
            quoted = len(rest) >= 2 and rest[0] == rest[-1] == "'" and "'" not in rest[1:-1]
            value = rest[1:-1] if quoted else _scalar(rest)
            raise corrected_as({name: value}, "uses ==; equality is one =")
        raise WhereRefusal(
            f"--where {term!r} has two operators in a row",
            f"use one of {_OPS} between the field and the value",
            term,
        )
    if not raw:
        raise WhereRefusal(
            f"--where {term!r} has no value",
            f'for an empty string write {name}{op}"", for a missing field '
            f"{name}=null",
            term,
        )
    if (
        len(raw) >= 2 and raw[0] == "'" and raw[-1] == "'" and "'" not in raw[1:-1]
        and not _SQL_TAIL.search(raw)
    ):
        # SQL's quotes around one string: one reading.
        inner = raw[1:-1]
        if op in ("=", "!="):
            clause = {name: inner} if op == "=" else {"op": "not", "clause": {name: inner}}
        elif op == "~":
            clause = {name: {"op": "prefix", "value": inner}}
        else:
            clause = _compare(name, op, inner, term)
        raise corrected_as(clause, "quotes its value as SQL does; the shell form takes it bare")
    if _SQL_TAIL.search(raw):
        raise WhereRefusal(
            f"--where {term!r} joins more than one condition",
            "give each condition its own --where (they are joined by and), "
            "or write the condition as JSON with \"op\": \"or\"",
            term,
        )
    if ('"' in raw or "'" in raw) and not _one_json_string(raw):
        # Quote marks the shell passed on: never part of the value.
        items = _quoted_items(raw) if op in ("=", "!=") else None
        if items is not None and len(items) >= 2 and None not in items:
            listed: dict[str, Any] = {name: items}
            raise corrected_as(
                listed if op == "=" else {"op": "not", "clause": listed},
                "quotes the items of its list; a list is written bare (a,b) or as JSON",
            )
        raise WhereRefusal(
            f"--where {term!r} has quote marks inside its value",
            "write the value bare, as one string in double quotes, or the "
            "condition as JSON, for example --where '{\"attrs.title\": \"say \\\"hi\\\"\"}'",
            term,
        )
    if op == "~":
        value = _scalar(raw)
        if not isinstance(value, str):
            value = raw
        return {name: {"op": "prefix", "value": value}}
    if op in ("=", "!="):
        quoted = len(raw) >= 2 and raw[0] == '"' and raw[-1] == '"'
        if "," in raw and not quoted:
            items = [part.strip() for part in raw.split(",")]
            if any(not item for item in items):
                raise WhereRefusal(
                    f"--where {term!r} has an empty item in its list",
                    "a comma list means any of; name each value once, or "
                    f'quote a value holding a comma: {name}{op}"a,b"',
                    term,
                )
            values = [_scalar(item) for item in items]
            if any(value is None for value in values):
                raise WhereRefusal(
                    f"--where {term!r} lists null among its values",
                    f"give null its own term ({name}=null or {name}!=null)",
                    term,
                )
            clause: dict[str, Any] = {name: values}
            return clause if op == "=" else {"op": "not", "clause": clause}
        value = _scalar(raw)
        if value is None:
            # Present or not, never a match on a stored null.
            return _absent(name) if op == "=" else {name: {"op": "exists"}}
        return {name: value} if op == "=" else {"op": "not", "clause": {name: value}}
    return _compare(name, op, _scalar(raw), term)


def _compare(name: str, op: str, value: Any, term: str) -> dict[str, Any]:
    if value is None or isinstance(value, bool):
        raise WhereRefusal(
            f"--where {term!r} compares with {json.dumps(value)}, which has no order",
            "compare with a number, a date or a string",
            term,
        )
    word = {">": "gt", "<": "lt", ">=": "gt", "<=": "lt"}[op]
    clause = {name: {"op": word, "value": value}}
    if op in (">=", "<="):
        # The tools compare strictly; or-equal is the one more test.
        return {"op": "or", "clauses": [clause, {name: value}]}
    return clause


def _joined(clauses: list[Any]) -> Any:
    if len(clauses) == 1:
        return clauses[0]
    short = all(
        isinstance(clause, dict) and clause and "op" not in clause for clause in clauses
    )
    keys = [key for clause in clauses if isinstance(clause, dict) for key in clause]
    if short and len(keys) == len(set(keys)):
        # One short form: several keys are joined by and.
        return {key: value for clause in clauses for key, value in clause.items()}
    return {"op": "and", "clauses": clauses}


def where_condition(terms: list[str]) -> Any:
    """The condition `--where` terms state, joined by and.

    One term that opens as JSON is sent exactly as typed. Several terms are
    each a JSON object or one `k OP v`. Raises :class:`WhereRefusal` for
    the first term that is neither, with the whole corrected condition when
    the term has one reading.
    """
    if len(terms) == 1 and terms[0].lstrip()[:1] in ("{", "["):
        try:
            return json.loads(terms[0])
        except ValueError as exc:
            raise WhereRefusal(
                f"--where opens as JSON but is not JSON: {exc}",
                "check the quotes and braces, or write k=v terms",
                terms[0],
            ) from exc
    clauses: list[Any] = []
    pending: Optional[WhereRefusal] = None
    for term in terms:
        if term.lstrip()[:1] == "{":
            try:
                parsed = json.loads(term)
            except ValueError as exc:
                problem = WhereRefusal(
                    f"--where {term!r} opens as JSON but is not JSON: {exc}",
                    "check the quotes and braces, or write k=v terms", term,
                )
                pending = pending or problem
                clauses.append(None)
                continue
            if not isinstance(parsed, dict) or not parsed:
                problem = WhereRefusal(
                    f"--where {term!r} is not one non-empty JSON object",
                    "beside other --where terms, a JSON term is one condition object",
                    term,
                )
                pending = pending or problem
                clauses.append(None)
                continue
            clauses.append(parsed)
            continue
        try:
            clauses.append(_term_clause(term))
        except WhereRefusal as problem:
            clauses.append(problem.corrected)
            pending = pending or problem
    if pending is not None:
        pending.corrected = (
            _joined(clauses) if all(clause is not None for clause in clauses) else None
        )
        raise pending
    return _joined(clauses)


# ── the plan of one command ──────────────────────────────────────────────


@dataclass
class Plan:
    """What one command sends and how it prints the replies."""

    operator: str
    arguments: dict[str, Any]
    mode: str = "text"  # text, json, jsonl or ids
    all_pages: bool = False
    #: `{"sent", "served"}` for each bare form read as the argument it means.
    adjusted: list[dict[str, str]] = field(default_factory=list)
    #: `(flag, word)` for each word read as a subtype; checked against the
    #: deployment's catalog once the server is up.
    subtype_words: list[tuple[str, str]] = field(default_factory=list)
    stdin_ids: Optional[list[str]] = None
    shell: bool = False
    #: `okg inspect texts -`: the stdin ids are read as texts (`drive_texts`).
    texts: bool = False
    #: `--lines` of `okg inspect texts`, as the `lines` of each text item.
    text_lines: Optional[list[list[Optional[int]]]] = None

    def calls(self) -> list[dict[str, Any]]:
        """Each call's argument object: one, or one per batch of ids."""
        if self.stdin_ids is None:
            return [self.arguments]
        ids = self.stdin_ids
        if self.operator == "inspect":
            return [
                {**self.arguments, "target": {"kind": "nodes", "node_ids": ids[at:at + INSPECT_STDIN_BATCH]}}
                for at in range(0, len(ids), INSPECT_STDIN_BATCH)
            ]
        if self.operator == "expand":
            return [{**self.arguments, "input": node_id} for node_id in ids]
        return [
            _with_ids(self.arguments, ids[at:at + SET_STDIN_BATCH])
            for at in range(0, len(ids), SET_STDIN_BATCH)
        ]


def _with_ids(arguments: dict[str, Any], ids: list[str]) -> dict[str, Any]:
    """`arguments` narrowed to `ids`, joined by and to the condition the
    call already has, in the argument that holds it."""
    clause = {"node_id": list(ids)}
    out = dict(arguments)
    for name in ("predicate", "where"):
        existing = out.get(name)
        if existing not in (None, {}):
            out[name] = _joined([existing, clause]) if isinstance(existing, dict) else {
                "op": "and", "clauses": [existing, clause],
            }
            return out
    out["where"] = clause
    return out


def flags_given(args: argparse.Namespace) -> dict[str, bool]:
    return {
        "jsonl": bool(getattr(args, JSONL_ATTR, False)),
        "ids": bool(getattr(args, IDS_ATTR, False)),
        "all": bool(getattr(args, ALL_ATTR, False)),
        "stdin": bool(getattr(args, STDIN_ATTR, False)),
    }


def output_mode(args: argparse.Namespace, *, as_json: bool) -> tuple[str, bool]:
    """`(mode, all_pages)`. `--all` alone prints JSON Lines: a page of text
    has its status line on top, which a pipe would have to strip."""
    given = flags_given(args)
    if given["jsonl"] and given["ids"]:
        raise _conflict("--jsonl and --ids", "keep one of them")
    if as_json and (given["jsonl"] or given["ids"]):
        flag = "--jsonl" if given["jsonl"] else "--ids"
        raise _conflict(
            f"--json and {flag}",
            f"keep {flag} for one row per line, or --json for the whole reply",
        )
    if given["ids"]:
        return "ids", given["all"]
    if given["jsonl"]:
        return "jsonl", given["all"]
    if as_json:
        return "json", given["all"]
    return ("jsonl" if given["all"] else "text"), given["all"]


def _conflict(flags: str, fix: str) -> ShellRefusal:
    return ShellRefusal(
        "read_output_flags_conflict",
        f"{flags} ask for two outputs of one reply",
        fix_hint=fix,
        flags=flags,
    )


def _note(plan: Plan, sent: str, served: Any) -> None:
    plan.adjusted.append({
        "sent": sent,
        "served": served if isinstance(served, str) else _spelled(served),
    })


def read_stdin_ids(operator: str) -> list[str]:
    """The ids on standard input, in order and once each.

    One id per line, or JSON Lines with a `node_id`, as `--jsonl` and
    `--ids` print them. Empty lines are skipped. Anything else is refused by
    line number.
    """
    stream = sys.stdin
    if stream is None or (hasattr(stream, "isatty") and stream.isatty()):
        raise ShellRefusal(
            "read_stdin_invalid",
            f"okg {operator} was told to read ids from standard input, and "
            "nothing is piped in",
            fix_hint=(
                f"pipe ids in: okg filter ... --ids | okg {operator} "
                + ("-" if operator in ("inspect", "expand") else "--stdin")
            ),
        )
    ids: list[str] = []
    seen: set[str] = set()
    for number, line in enumerate(stream.read().splitlines(), 1):
        text = line.strip()
        if not text:
            continue
        node_id: Any = text
        if text[:1] == "{":
            try:
                record = json.loads(text)
            except ValueError:
                record = None
            node_id = record.get("node_id") if isinstance(record, dict) else None
            if not isinstance(node_id, str) or not node_id:
                raise ShellRefusal(
                    "read_stdin_invalid",
                    f"okg {operator}: line {number} of standard input is not one "
                    "JSON object with a string node_id",
                    fix_hint="pipe one id per line, or JSON Lines that carry node_id",
                    line=number,
                )
        elif not _PLAIN_ID.match(text):
            # A status line, a note, a line of indented JSON or text with
            # spaces: not an id. An id with a space comes as JSON Lines.
            raise ShellRefusal(
                "read_stdin_invalid",
                f"okg {operator}: line {number} of standard input is not an id "
                "(one word without spaces or quotes) and not JSON Lines",
                fix_hint=(
                    "pipe the output of --ids or --jsonl, not the text reply; an "
                    'id with a space goes in as {"node_id": "..."}'
                ),
                line=number,
            )
        if node_id not in seen:
            seen.add(node_id)
            ids.append(node_id)
    if operator == "expand" and len(ids) > MAX_EXPAND_STDIN_IDS:
        raise ShellRefusal(
            "read_stdin_invalid",
            f"okg expand makes one call per id and takes at most "
            f"{MAX_EXPAND_STDIN_IDS} ids from standard input; {len(ids)} came in",
            fix_hint=f"split the ids (split -l {MAX_EXPAND_STDIN_IDS}) or narrow the set",
            count=len(ids),
        )
    if operator == "inspect texts" and len(ids) > MAX_TEXTS_STDIN_IDS:
        raise ShellRefusal(
            "read_stdin_invalid",
            f"okg inspect texts reads at most {MAX_TEXTS_STDIN_IDS} ids from "
            f"standard input; {len(ids)} came in",
            fix_hint=f"split the ids (split -l {MAX_TEXTS_STDIN_IDS}) or narrow the set",
            count=len(ids),
        )
    return ids


def _select_names(value: Any) -> list[str]:
    if isinstance(value, list):
        return [name for name in value if isinstance(name, str)]
    if isinstance(value, dict):
        return [name for name in value.values() if isinstance(name, str)]
    return []


def _node_set(arguments: dict[str, Any]) -> bool:
    scope = arguments.get("input")
    return isinstance(scope, dict) and scope.get("kind", "nodes") == "nodes" and "ref" not in scope


def build_plan(
    operator: str,
    args: argparse.Namespace,
    arguments: dict[str, Any],
    *,
    as_json: bool,
) -> Plan:
    """The plan for `arguments`, the object `_read_operators` built from the
    tool's own flags, with this module's flags and bare forms applied.

    Everything decided from the command line alone is decided here, before
    any start-up cost; only whether a word is a declared subtype waits for
    the server (:func:`resolve_subtypes`).
    """
    mode, all_pages = output_mode(args, as_json=as_json)
    plan = Plan(operator=operator, arguments=dict(arguments), mode=mode, all_pages=all_pages)
    plan.shell = mode in ("jsonl", "ids") or all_pages
    out = plan.arguments

    terms = getattr(args, WHERE_ATTR, None)
    if terms and operator in WHERE_COMMANDS:
        if "where" in out:
            raise ShellRefusal(
                "read_argument_given_twice",
                f"okg {operator} received where both inside --args and as --where",
                fix_hint="give where once",
                arguments=["where"],
            )
        try:
            out["where"] = where_condition(list(terms))
        except WhereRefusal as problem:
            retry = None
            if problem.corrected is not None:
                retry = {**out, "where": problem.corrected}
            raise problem.refusal(operator, retry, args) from None

    if operator == "inspect":
        _plan_inspect(plan, args)
    if operator in SET_COMMANDS:
        _plan_set_input(plan, args)
    if operator == "expand" and (out.get("input") == "-" or getattr(args, STDIN_ATTR, False)):
        if out.get("input") not in (None, "-"):
            raise _stdin_twice(operator, "input")
        out.pop("input", None)
        plan.stdin_ids = read_stdin_ids(operator)
    if operator in ("filter", "map") and getattr(args, STDIN_ATTR, False):
        plan.stdin_ids = read_stdin_ids(operator)
        out.setdefault("input", {"kind": "nodes"})
    if plan.stdin_ids is not None and getattr(args, "operator_args", None) == "-":
        raise _conflict("--args - and ids from standard input", "pass --args as JSON text or @file")

    if operator in ("filter", "map") and (plan.shell or plan.stdin_ids is not None):
        _plan_row_fields(plan)
    plan.shell = plan.shell or bool(
        plan.adjusted or plan.subtype_words or plan.stdin_ids is not None
        or (terms and operator in WHERE_COMMANDS)
    )
    return plan


def _stdin_twice(operator: str, argument: str) -> ShellRefusal:
    return ShellRefusal(
        "read_argument_given_twice",
        f"okg {operator} received {argument} both on the command line and as "
        "ids from standard input",
        fix_hint=f"drop {argument}, or do not pipe ids in",
        arguments=[argument],
    )


def _plan_inspect(plan: Plan, args: argparse.Namespace) -> None:
    out = plan.arguments
    given = getattr(args, FROM_ATTR, None)
    more = getattr(args, MORE_ATTR, None)
    from_stdin = bool(getattr(args, STDIN_ATTR, False))
    if given == TEXTS_WORD and (more == "-" or (more is None and from_stdin)):
        _plan_texts(plan, args)
        return
    if more is not None:
        raise ShellRefusal(
            "read_argument_given_twice",
            f"okg inspect takes one NODE_ID and received two: {given} and {more}",
            fix_hint=(
                "pipe many ids in, one per line: okg inspect - reads the records, "
                "okg inspect texts - their texts"
            ),
            arguments=["target"],
        )
    lines = getattr(args, LINES_ATTR, None)
    if lines is not None:
        ranges = parse_lines_flag(lines)
        if given == TEXTS_WORD and out.get("target") is None:
            # `okg inspect texts --lines ...`: the texts form without its `-`.
            # The read of a record named `texts` is never what was meant.
            typed = getattr(args, TYPED_DEPLOYMENT_ATTR, None)
            refusal = ShellRefusal(
                "read_lines_invalid",
                "okg inspect texts reads the ids piped in and needs - (or "
                "--stdin) after texts",
                fix_hint="pipe the ids in: okg filter ... --ids --all | okg inspect texts - --lines ...",
                argument="--lines",
            )
            refusal.retry_command = "okg " + shlex.join([
                "inspect", *(["--deployment", str(typed)] if typed else []),
                TEXTS_WORD, "-", "--lines", str(lines),
            ])
            raise refusal
        retry = None
        if given not in (None, "-") and out.get("target") is None and not from_stdin:
            # `okg inspect <id> --lines ...` has one reading: that record's
            # text, those lines. It is a text read, not the node read the
            # command names, so it is offered, not served.
            retry = ("inspect", {
                **{k: v for k, v in out.items() if k != "target"},
                "target": {"kind": "text", "node_id": given, "lines": ranges},
            })
        raise ShellRefusal(
            "read_lines_invalid",
            "okg inspect --lines reads chosen lines of the texts of the ids "
            "piped in (okg inspect texts -); one text's lines go in its text "
            "target",
            fix_hint=(
                "pipe the ids into okg inspect texts - --lines ..., or read one "
                "text: --target '{\"kind\": \"text\", \"node_id\": \"<id>\", "
                "\"lines\": [[first, last]]}'"
            ),
            retry=retry,
            argument="--lines",
        )
    if given == "-" or from_stdin:
        if out.get("target") is not None or given not in (None, "-"):
            raise _stdin_twice("inspect", "target")
        plan.stdin_ids = read_stdin_ids("inspect")
        return
    if given is not None:
        if out.get("target") is not None:
            raise ShellRefusal(
                "read_argument_given_twice",
                "okg inspect received target both as NODE_ID and as --target",
                fix_hint="give target once",
                arguments=["target"],
            )
        out["target"] = {"kind": "node", "node_id": given}
        _note(plan, f"NODE_ID {given}", f"target {_spelled(out['target'])}")
        return
    target = out.get("target")
    if isinstance(target, str) and target and target.lstrip()[:1] not in ("{", "["):
        out["target"] = {"kind": "node", "node_id": target}
        _note(plan, f"--target {target}", f"target {_spelled(out['target'])}")
    elif isinstance(target, (int, float)) and not isinstance(target, bool):
        node_id = json.dumps(target)
        out["target"] = {"kind": "node", "node_id": node_id}
        _note(plan, f"--target {node_id}", f"target {_spelled(out['target'])}")


def _plan_texts(plan: Plan, args: argparse.Namespace) -> None:
    """`okg inspect texts -`: the ids on stdin, each read whole as a text.

    Every text prints as one JSON line, so `--ids` is refused and `--json`
    is read as those lines, with a note. The record is fixed: `--select`,
    `--next` and `--target` name other reads and are refused."""
    out = plan.arguments
    if out.get("target") is not None:
        raise _stdin_twice("inspect", "target")
    for name in ("select", "next"):
        if out.get(name) is not None:
            raise _conflict(
                f"--{name} and okg inspect texts",
                f"drop --{name}: okg inspect texts prints one fixed record per text",
            )
    if flags_given(args)["ids"]:
        raise _conflict(
            "--ids and okg inspect texts", "drop --ids: each text prints as one JSON line",
        )
    if plan.mode == "json":
        _note(plan, "--json", "JSON Lines, one record per text")
    plan.mode = "jsonl"
    lines = getattr(args, LINES_ATTR, None)
    if lines is not None:
        plan.text_lines = parse_lines_flag(lines)
    plan.texts = True
    plan.stdin_ids = read_stdin_ids("inspect texts")


#: One range of `--lines`: FIRST-LAST, FIRST- (to the end) or one line.
_LINE_RANGE = re.compile(r"\s*([1-9][0-9]{0,8})\s*(?:(-)\s*([1-9][0-9]{0,8})?\s*)?\Z")


def parse_lines_flag(raw: str) -> list[list[Optional[int]]]:
    """`--lines 1-40,120-160,300-` as a text target's `lines`:
    `[[1, 40], [120, 160], [300, None]]`; one number is that line alone.

    Only the spelling is checked here. The text read refuses ranges out of
    order, overlapping or too many by name (`record_text.parse_lines`)."""
    ranges: list[list[Optional[int]]] = []
    for part in str(raw).split(","):
        found = _LINE_RANGE.match(part)
        if found is None:
            raise ShellRefusal(
                "read_lines_invalid",
                f"--lines {raw}: {part.strip() or 'an empty range'} is not a "
                "line range",
                fix_hint=(
                    "write --lines FIRST-LAST, comma-separated, e.g. --lines "
                    "1-40,120-160; FIRST- reads to the end, one number one line"
                ),
                argument="--lines",
            )
        first = int(found.group(1))
        last: Optional[int]
        if found.group(3) is not None:
            last = int(found.group(3))
        else:
            last = None if found.group(2) else first
        ranges.append([first, last])
    return ranges


def _plan_set_input(plan: Plan, args: argparse.Namespace) -> None:
    out = plan.arguments
    scope = out.get("input")
    if isinstance(scope, str) and _SUBTYPE_WORD.match(scope):
        out["input"] = {"kind": "nodes", "subtype": scope}
        plan.subtype_words.append(("--input", scope))
        _note(plan, f"--input {scope}", f"input {_spelled(out['input'])}")
    elif (
        plan.operator in ("filter", "map") and isinstance(scope, dict)
        and "subtype" in scope and "kind" not in scope
        and set(scope) <= {"subtype", "filter"}
    ):
        out["input"] = {"kind": "nodes", **scope}
        _note(plan, f"input {_spelled(scope)}", f"input {_spelled(out['input'])}")
    subtype = getattr(args, TYPE_ATTR, None)
    if subtype is None:
        return
    scope = out.get("input")
    if scope is None:
        out["input"] = {"kind": "nodes", "subtype": subtype}
    elif (
        isinstance(scope, dict) and scope.get("kind", "nodes") == "nodes"
        and "ref" not in scope and scope.get("subtype") in (None, subtype)
    ):
        out["input"] = {"kind": "nodes", **scope, "subtype": subtype}
    else:
        raise ShellRefusal(
            "read_input_unresolved",
            f"okg {plan.operator} received --type {subtype} and an input that "
            f"is not a node set of that subtype: {_spelled(scope)}",
            fix_hint="drop --type, or drop the subtype from --input",
            argument="--type",
        )
    plan.subtype_words.append(("--type", subtype))
    _note(plan, f"--type {subtype}", f"input {_spelled(out['input'])}")


def _plan_row_fields(plan: Plan) -> None:
    """`filter` and `map` rows printed one per line carry their id.

    A row has the fields `select` (filter) or `projection` (map) names. For
    `--jsonl` and `--ids` on a node set, `node_id` and `subtype` are added to
    a list that lacks them, with a note. Record text is refused: these rows
    carry only its digest (`text_hash`).
    """
    out = plan.arguments
    argument = "select" if plan.operator == "filter" else "projection"
    names = _select_names(out.get(argument)) + _select_names(out.get("select"))
    if "text" in names:
        raise ShellRefusal(
            "read_select_text_unavailable",
            f"okg {plan.operator} rows carry no record text, only its digest "
            "(text_hash); a `text` field would print null for every row",
            fix_hint=(
                "read every text of the set as JSON Lines: okg filter ... --ids "
                "--all | okg inspect texts -; or up to 8 records per call: okg "
                "inspect --target '{\"kind\": \"texts\", \"items\": [{\"key\": "
                "\"1\", \"node_id\": \"<id>\"}]}'"
            ),
            argument=argument,
        )
    if plan.mode not in ("jsonl", "ids") or not _node_set(out):
        return
    wanted = ["node_id", "subtype"] if plan.mode == "jsonl" else ["node_id"]
    for name in (argument, "select"):
        value = out.get(name)
        if isinstance(value, list) and value and all(isinstance(v, str) for v in value):
            missing = [field_name for field_name in wanted if field_name not in value]
            if missing:
                out[name] = [*missing, *value]
                _note(plan, f"{name} {_spelled(value)}", f"{_spelled(out[name])} for one row per line")


def resolve_subtypes(plan: Plan, server: Any) -> None:
    """Refuse a word read as a subtype that the catalog does not declare."""
    if not plan.subtype_words:
        return
    words = sorted({word for _flag, word in plan.subtype_words})
    declared = declared_subtypes(server)
    for flag, word in plan.subtype_words:
        if word in declared:
            continue
        near = difflib.get_close_matches(word, sorted(declared), n=3, cutoff=0.6)
        shown = near or sorted(declared)[:8]
        raise ShellRefusal(
            "read_input_unresolved",
            f"okg {plan.operator}: {flag} {word} is not a subtype this "
            "deployment declares, so it names no set",
            fix_hint=(
                "name a declared subtype ("
                + ("nearest: " if near else "some: ")
                + ", ".join(shown)
                + "), or the set as JSON: --input '{\"kind\": \"nodes\", \"subtype\": \"<subtype>\"}'"
            ),
            argument=flag,
            value=word,
            declared_count=len(declared),
            checked=words,
        )


def declared_subtypes(server: Any) -> set[str]:
    """Every subtype name the deployment's catalog declares, read through
    the server's own read pool."""
    with server._okg_pool.connection() as conn, conn.cursor() as cur:
        cur.execute("SELECT DISTINCT name FROM okg.subtypes")
        return {str(row[0]) for row in cur.fetchall()}


# ── printing ─────────────────────────────────────────────────────────────


def emit_adjusted(plan: Plan) -> None:
    from okg.substrate.cli._read_operators import say

    if plan.adjusted:
        say("! adjusted: " + "; ".join(
            f"{record['sent']} read as {record['served']}" for record in plan.adjusted
        ))


def shell_command(
    tool: str, arguments: dict[str, Any], args: argparse.Namespace,
) -> Optional[str]:
    """`okg <tool> ...` that sends exactly `arguments`, shell-quoted.

    Each argument the command has a flag for is that flag with its value as
    JSON (a flag value is read as JSON first, so a string keeps its quotes);
    the rest go in `--args`. `--deployment` is named when it was typed, and
    the output flags typed are kept. A `--dsn` is never repeated: its
    password would be printed.
    """
    from okg.substrate.cli._read_operators import OPERATOR_ARGUMENTS, flag_of

    if tool not in OPERATOR_ARGUMENTS:
        return None
    # The words after `okg`: this is a line for a person or an agent to run,
    # not an argv this process starts (`cli/child_argv.py` covers those).
    parts = [tool]
    typed = getattr(args, TYPED_DEPLOYMENT_ATTR, None)
    if typed:
        parts += ["--deployment", str(typed)]
    given = flags_given(args)
    parts += [f"--{name}" for name in ("jsonl", "ids", "all") if given[name]]
    rest: dict[str, Any] = {}
    for name, value in arguments.items():
        own = tool != "search" and (
            name in OPERATOR_ARGUMENTS[tool]
            or (name == "where" and tool in WHERE_COMMANDS and isinstance(value, dict))
        )
        if own:
            parts += [flag_of(name), json.dumps(value, ensure_ascii=False)]
        else:
            rest[name] = value
    if rest:
        parts += ["--args", json.dumps(rest, ensure_ascii=False)]
    return "okg " + shlex.join(parts)


#: Parsed attributes never repeated on a `retry:` line: `--dsn` holds a
#: password; `--json` is the stream's format, said before the command.
_NOT_REPEATED = frozenset({"help", "dsn", "json"})


#: `typed_command`'s `where` that keeps the `--where` terms as typed.
KEEP_WHERE: Any = object()


def typed_command(
    operator: str, args: argparse.Namespace, *, where: Any, drop: tuple[str, ...] = (),
) -> str:
    """`okg <operator> ...` as the caller typed it, every flag and
    positional kept as typed, with the `--where` terms replaced by `where`
    as one JSON condition (`KEEP_WHERE`: as typed). `--deployment` only
    when typed; never `--dsn`; no flag whose dest is in `drop`."""
    from okg.substrate.cli.main import _build_parser

    parser = _build_parser()
    sub = next(a for a in parser._actions if isinstance(a, argparse._SubParsersAction))
    command = sub.choices[operator]
    flags: list[str] = []
    positionals: list[str] = []
    for action in command._actions:
        if (
            action.dest in _NOT_REPEATED or action.dest == argparse.SUPPRESS
            or action.dest in drop
        ):
            continue
        if action.dest == "deployment":
            value: Any = getattr(args, TYPED_DEPLOYMENT_ATTR, None)
        elif action.dest in (WHERE_ATTR, "op_where") and where is not KEEP_WHERE:
            value = [json.dumps(where, ensure_ascii=False)]
        else:
            value = getattr(args, action.dest, None)
        if not action.option_strings:
            if value is not None:
                positionals.append(str(value))
            continue
        flag = max(action.option_strings, key=len)
        if isinstance(action, argparse._StoreTrueAction):
            if value is True:
                flags.append(flag)
            continue
        if isinstance(action, argparse._StoreFalseAction):
            if value is False:
                flags.append(flag)
            continue
        if value is None or value == action.default:
            continue
        for item in value if isinstance(value, list) else [value]:
            flags += [flag, str(item)]
    return "okg " + shlex.join([operator, *flags, *positionals])


#: A text reply of more rows than this is followed by one stderr line that
#: gives the same call as a pipe (#3666). 20 is under the default page of
#: filter, map, aggregate and expand (50), so a full default page gets the
#: line, and over search's (10), whose top rows are read for their rank.
PIPE_HINT_ROWS = 20

#: The row span of a text reply's status line: `1-50 of 312`, `1-50 of
#: 312+`, `1-50 of count unknown`, `0 of 0`, and a bounded page's
#: `1-25, more follow`.
_SPAN_FACT = re.compile(
    r"(?:([0-9]+)-([0-9]+)|0)(?: of (?:([0-9]+)(\+?))?|(, more follow))"
)

#: The flags a hint never repeats: a page's offset or next token would read
#: the set from that page on, not the whole set.
_HINT_DROPS = ("op_offset", "offset", "op_next", "op_continuation_token")


def _input_from_stdin(args: Any) -> bool:
    """True when the command read its ids or its arguments from standard
    input: `--stdin`, a positional `-`, `--input -` or `--args -`. Its
    hint could not be run as printed, so none is given."""
    if args is None:
        return False
    if getattr(args, STDIN_ATTR, False):
        return True
    return any(
        getattr(args, name, None) == "-"
        for name in ("operator_positional", "operator_args", "op_input")
    )

#: The program the hint pipes into: it counts the rows, ready to change.
PIPE_HINT_PROGRAM = (
    "python3 -c 'import json, sys; rows = [json.loads(line) for line in sys.stdin]; "
    "print(len(rows))'"
)


def pipe_hint(operator: str, text: str, args: argparse.Namespace) -> Optional[str]:
    """One line for a served text reply of more than `PIPE_HINT_ROWS` rows
    (on the page, or in the set it counts, except for search): the call as
    typed with --all --jsonl, piped into a program. None otherwise, and
    None when the input came from standard input (the printed command
    would have nothing piped in)."""
    from okg.substrate.mcp.result_text import status_facts

    if _input_from_stdin(args):
        return None
    for fact in status_facts(text):
        match = _SPAN_FACT.match(fact)
        if match is None:
            continue
        shown = int(match.group(2)) - int(match.group(1)) + 1 if match.group(1) else 0
        total = int(match.group(3)) if match.group(3) and operator != "search" else None
        plus = match.group(4) or ""
        more = match.group(5) is not None
        break
    else:
        return None
    rows = max(shown, total or 0)
    if rows <= PIPE_HINT_ROWS:
        return None
    command = typed_command(operator, args, where=KEEP_WHERE, drop=_HINT_DROPS)
    if total and total > shown:
        said = f"{shown} of {total}{plus} rows shown"
    elif more:
        said = f"{shown} rows shown, more follow"
    else:
        said = f"{shown} rows"
    return (
        f"hint: {said}; to count, join or list them, pipe them instead of "
        f"reading them: {command} --all --jsonl | {PIPE_HINT_PROGRAM}"
    )


def retry_line(
    call: Any, sent: dict[str, Any], args: argparse.Namespace,
) -> Optional[str]:
    """`retry: okg ...` for a reply's `retry_call`, or None.

    Only a call the caller can send as written (`corrected`, or a `guess`,
    which says so); the arguments it names to repeat are taken from the
    call that was sent.
    """
    if not isinstance(call, dict) or call.get("mode") not in ("corrected", "guess"):
        return None
    tool, arguments = call.get("tool"), call.get("arguments")
    if not isinstance(tool, str) or not isinstance(arguments, dict):
        return None
    repeat = call.get("repeat")
    merged = dict(arguments)
    if isinstance(repeat, list):
        for name in repeat:
            if isinstance(name, str) and name in sent and name not in merged:
                merged[name] = sent[name]
    command = shell_command(tool, merged, args)
    if command is None:
        return None
    return "retry: " + command + ("  # a guess" if call.get("mode") == "guess" else "")


def retry_call_in_text(text: str) -> Any:
    """The `retry_call` a text refusal prints: one `retry_call: {...}` line,
    or a `--- retry_call` line followed by the call's JSON."""
    lines = text.splitlines()
    for index, line in enumerate(lines):
        body: Optional[str] = None
        if line.startswith("retry_call: "):
            body = line[len("retry_call: "):]
        elif line.strip() == "--- retry_call" and index + 1 < len(lines):
            body = lines[index + 1]
        if body is not None:
            try:
                found = json.loads(body)
            except ValueError:
                return None
            return found if isinstance(found, dict) else None
    return None


def emit_refusal(
    exc: SubstrateError, operator: str, args: argparse.Namespace, *, mode: str,
) -> int:
    """A read command refused before any call: a shell form of this module,
    an argument the command cannot read (`--args`, an argument given twice)
    or no deployment. One shape, a tool refusal's, defined by the render
    step (`_read_render.render_refusal`): one JSON object (`status`,
    `error`) with `--json`; otherwise `<op> refused <code>: ...`, the fix,
    the data and the `retry:` line. Always exit 2: a refusal is the call as
    typed, not an error."""
    from okg.substrate.cli._read_render import render_refusal

    return render_refusal(exc, operator, args, mode=mode).write()


def reply_rows(operator: str, payload: dict[str, Any]) -> list[Any]:
    """The rows of one JSON reply, as the reply carries them."""
    if operator == "inspect":
        for key in ("records", "items"):
            if isinstance(payload.get(key), list):
                return payload[key]
        return [payload]
    key = ROWS_KEY.get(operator)
    rows = payload.get(key) if key else None
    return rows if isinstance(rows, list) else []


#: Line and paragraph separators Python's `json` leaves raw inside a string.
#: Valid JSON, but `str.splitlines` and other line readers break a line at
#: each, so a JSON Lines row holding one would read as two broken lines.
_RAW_LINE_BREAKS = {"\u0085": "\\u0085", "\u2028": "\\u2028", "\u2029": "\\u2029"}


def jsonl_line(row: Any) -> str:
    """One row as one line of JSON Lines: every control character escaped
    (as `json` does) and every other character a line reader breaks at
    written as its `\\uXXXX` escape. Such a character appears only inside a
    JSON string, where the escape reads back as the same text."""
    line = json.dumps(row, ensure_ascii=False)
    if any(char in line for char in _RAW_LINE_BREAKS):
        for char, escape in _RAW_LINE_BREAKS.items():
            line = line.replace(char, escape)
    return line


def one_document(
    operator: str, pages: list[dict[str, Any]], *, complete: bool = True,
) -> dict[str, Any]:
    """The pages `--all --json` read of ONE set (one call and the pages its
    `next` led to) as one document.

    One page of a set read to its end prints as it came. Several print in
    the one-page shape: the rows of every page in order under the rows key;
    `returned_count`, where a page carries it, the number of those rows;
    `offset` and `limit` the first page's (where the set began and the page
    size asked for); every other field the last page's, which say where the
    set ended (its `next_offset` or `continuation_token` is where to go on
    when the set is not complete); and `all_pages`: the pages and rows
    joined and whether the set was read to its end. Pages that carry no list
    of rows print as `{"pages": [...], "all_pages": ...}`, each page whole.
    Replies of different calls are never joined here (`json_output`).
    """
    if len(pages) == 1 and complete:
        return pages[0]
    if operator == "inspect":
        key = next((k for k in ("records", "items") if isinstance(pages[0].get(k), list)), None)
    else:
        key = ROWS_KEY.get(operator)
    if key is None or not all(isinstance(page.get(key), list) for page in pages):
        return {"pages": pages, "all_pages": {"pages": len(pages), "complete": complete}}
    document = dict(pages[-1])
    rows = [row for page in pages for row in page[key]]
    document[key] = rows
    if any("returned_count" in page for page in pages):
        document["returned_count"] = len(rows)
    for name in ("offset", "limit"):
        if name in pages[0]:
            document[name] = pages[0][name]
        else:
            document.pop(name, None)
    document["all_pages"] = {"pages": len(pages), "rows": len(rows), "complete": complete}
    return document


def json_output(
    operator: str, sets: list[list[dict[str, Any]]], *, all_pages: bool, complete: bool = True,
) -> Any:
    """What a `--json` command over several replies prints: each call's
    reply (its pages joined by `one_document` under `--all`), and, when
    there are several calls (batches of ids from standard input), a JSON
    array of those replies in input order, none of their fields joined.
    `complete` is False when the last call's set was not read to its end."""
    docs = [
        one_document(operator, pages, complete=complete or index < len(sets) - 1)
        if all_pages else pages[0]
        for index, pages in enumerate(sets)
    ]
    return docs[0] if len(docs) == 1 else docs


def _rows_read(plan: Plan, sets: list[list[dict[str, Any]]]) -> int:
    return sum(len(reply_rows(plan.operator, page)) for pages in sets for page in pages)


def _counted(n: int, one: str, many: str) -> str:
    return f"{n} {one if n == 1 else many}"


def _dropped_note(plan: Plan, sets: list[list[dict[str, Any]]], n_calls: int) -> str:
    """What a refused reply under `--json` leaves unprinted: the rows read
    so far, from how many pages (one call), replies (several calls), or
    pages of how many calls (several calls under `--all`), and which reply
    was refused."""
    rows = _counted(_rows_read(plan, sets), "row", "rows")
    pages = sum(len(pages) for pages in sets)
    if n_calls == 1:
        read, refused = _counted(pages, "page", "pages"), f"page {pages + 1}"
    elif not plan.all_pages:
        read, refused = _counted(pages, "reply", "replies"), f"reply {pages + 1}"
    else:
        done = len(sets) - (0 if sets[-1] else 1)
        read = f"{_counted(pages, 'page', 'pages')} of {_counted(done, 'call', 'calls')}"
        refused = f"page {len(sets[-1]) + 1} of call {len(sets)}"
    return (f"okg {plan.operator}: {rows} from {read} were read and not printed, "
            f"because {refused} was refused")


def _ids_of(operator: str, arguments: dict[str, Any]) -> int:
    """How many ids from standard input one planned call carries."""
    if operator == "inspect":
        target = arguments.get("target")
        return len(target.get("node_ids") or []) if isinstance(target, dict) else 0
    if operator == "expand":
        return 1
    for name in ("predicate", "where"):
        found = _ids_in(arguments.get(name))
        if found is not None:
            return found
    return 0


def _ids_in(condition: Any) -> Optional[int]:
    if not isinstance(condition, dict):
        return None
    if isinstance(condition.get("node_id"), list):
        return len(condition["node_id"])
    for clause in condition.get("clauses") or []:
        found = _ids_in(clause)
        if found is not None:
            return found
    return None


def _say_cap(
    plan: Plan, args: argparse.Namespace, current: dict[str, Any], rows: int,
    left: list[dict[str, Any]],
) -> None:
    """The stderr note of `--all` stopped at `MAX_ALL_PAGES`: the true row
    count, the fix, the call that reads on, and the ids from standard input
    whose calls were not made."""
    from okg.substrate.cli._read_operators import say

    say(f"okg {plan.operator} --all: stopped at the cap of {MAX_ALL_PAGES} pages; "
        f"{_counted(rows, 'row was', 'rows were')} printed and the set goes on; "
        f"{_cap_hint(plan.operator, current)}")
    command = shell_command(plan.operator, current, args)
    if command is not None:
        if plan.mode == "json":
            command = command.replace("okg ", "okg --json ", 1)
        sys.stderr.write("resume: " + command + "\n")
    if left:
        unread = sum(_ids_of(plan.operator, call) for call in left)
        total = len(plan.stdin_ids or [])
        say(f"okg {plan.operator}: {unread} of {total} ids from standard input were not "
            f"read ({_counted(len(left), 'call was', 'calls were')} not made)")


def _print_json(plan: Plan, sets: list[list[dict[str, Any]]], *, complete: bool) -> None:
    """Print `json_output` and say on stderr what it holds."""
    from okg.substrate.cli._read_operators import say

    output = json_output(plan.operator, sets, all_pages=plan.all_pages, complete=complete)
    sys.stdout.write(json.dumps(output, ensure_ascii=False) + "\n")
    sys.stdout.flush()
    replies = sum(len(pages) for pages in sets)
    if replies > 1 or not complete:
        shape = "a JSON array, one reply per call" if len(sets) > 1 else "one JSON document"
        say(f"okg {plan.operator}: {_rows_read(plan, sets)} rows from {replies} replies, "
            f"printed as {shape}" + ("" if complete else "; the set goes on"))


def row_id(operator: str, row: Any, arguments: dict[str, Any]) -> Optional[str]:
    """The id `--ids` prints for one row: its `node_id`; on a 1-hop expand,
    the neighbour's end of the link; else its `edge_id`."""
    if not isinstance(row, dict):
        return None
    if isinstance(row.get("node_id"), str):
        return row["node_id"]
    if operator == "expand" and isinstance(row.get("src"), str) and isinstance(row.get("dst"), str):
        direction = row.get("direction") or arguments.get("direction") or "out"
        return row["src"] if direction == "in" else row["dst"]
    if isinstance(row.get("edge_id"), str):
        return row["edge_id"]
    return None


def status_lines(operator: str, payload: dict[str, Any], arguments: dict[str, Any]) -> list[str]:
    """The status line and the `! ` notes the text reply would print."""
    from okg.substrate.mcp.result_text import render

    target = arguments.get("target")
    kind = target.get("kind") if operator == "inspect" and isinstance(target, dict) else None
    # No MCP size facts: they would size a text this shell does not print
    # (PACT read-reply-size-3666).
    text = render(
        payload, tool=operator, kind=kind, select=arguments.get("select"), size_facts=False,
    )
    lines = text.split("\n")
    notes = [line for line in lines[1:] if line.startswith("! ")]
    if operator == "search" and payload.get("search_profile", "") is None:
        notes.insert(0, UNSCOPED_SEARCH_NOTE)
    return [lines[0], *notes]


#: The note under a search's status line when no search profile scoped the
#: page (issue 797: the head never leaves the scope unsaid). `okg search`'s
#: own search prints it too (`_read_side._profile_note`).
UNSCOPED_SEARCH_NOTE = "! search profile: none (unscoped)"


class _TextBlock:
    type = "text"

    def __init__(self, text: str) -> None:
        self.text = text


class _TextReply:
    """`result` with its text block replaced by `text`, its other blocks and
    its error flag kept."""

    def __init__(self, result: Any, text: str) -> None:
        content = list(getattr(result, "content", None) or [])
        at = next((i for i, block in enumerate(content) if getattr(block, "type", None) == "text"), None)
        if at is None:
            content.insert(0, _TextBlock(text))
        else:
            content[at] = _TextBlock(text)
        self.content = content
        self.isError = bool(getattr(result, "isError", False))


def noted_reply(operator: str, result: Any) -> Any:
    """A text reply as the command prints it: `result`, or a copy whose
    text carries `UNSCOPED_SEARCH_NOTE` (`with_unscoped_note`)."""
    from okg.substrate.cli._read_operators import reply_text

    text, _others = reply_text(result)
    noted = with_unscoped_note(operator, text)
    return result if noted == text else _TextReply(result, noted)


def with_unscoped_note(operator: str, text: str) -> str:
    """A served search text reply with `UNSCOPED_SEARCH_NOTE` under its
    status line when its other fields say `search_profile: null`."""
    if operator != "search" or not text.startswith("search ok"):
        return text
    lines = text.split("\n")
    if "--- other fields" not in lines:
        return text
    rest = lines[lines.index("--- other fields") + 1:]
    if "search_profile: null" not in rest or UNSCOPED_SEARCH_NOTE in lines:
        return text
    return "\n".join([lines[0], UNSCOPED_SEARCH_NOTE, *lines[1:]])


def next_call(
    operator: str, arguments: dict[str, Any], payload: dict[str, Any], *, page: int,
) -> Optional[dict[str, Any]]:
    """The call that reads the next page, None after the last one.

    `next_offset` is sent back as `offset`, pinned to the generation the
    first page read; a token as `next` on inspect and expand (which stands
    alone; authority may stay) and as `continuation_token` on search. A
    reply that says the set goes on but gives neither is refused: `--all`
    would otherwise print a part as the whole.
    """
    pin = payload.get("generation_id")
    if payload.get("next_offset") is not None:
        out = {k: v for k, v in arguments.items() if k not in ("continuation_token", "next")}
        out["offset"] = payload["next_offset"]
        if "generation_id" not in out and pin is not None:
            out["generation_id"] = pin
        return out
    token = payload.get("continuation_token")
    if isinstance(token, str) and token:
        if operator in ("inspect", "expand"):
            return {"next": token, **{k: arguments[k] for k in ("authority",) if k in arguments}}
        if operator == "search":
            out = {k: v for k, v in arguments.items() if k != "offset"}
            out["continuation_token"] = token
            if "generation_id" not in out and pin is not None:
                out["generation_id"] = pin
            return out
    reason = payload.get("next_offset_reason")
    if (
        reason or payload.get("continuation_withheld") or payload.get("search_next")
        or payload.get("truncated") is True or payload.get("complete") is False
        or token
    ):
        remedy = payload.get("next_offset_remedy")
        raise ShellRefusal(
            "read_all_incomplete",
            f"okg {operator} --all: page {page} says the set goes on and gives "
            "no next page this command can read"
            + (f" (reason: {reason})" if reason else "")
            + "; the rows printed are part of the set, not all of it",
            fix_hint=(
                remedy if isinstance(remedy, str) and remedy
                else "narrow the set with --where, or read it page by page without --all"
            ),
            page=page,
            reason=reason,
        )
    return None


@dataclass
class _Printer:
    plan: Plan
    args: argparse.Namespace
    rows: int = 0
    pages: int = 0
    said: set[str] = field(default_factory=set)

    def page(self, payload: dict[str, Any], text: str, arguments: dict[str, Any]) -> None:
        from okg.substrate.cli._read_operators import say

        operator, mode = self.plan.operator, self.plan.mode
        rows = reply_rows(operator, payload)
        lines: list[str] = []
        if mode == "ids":
            for index, row in enumerate(rows, 1):
                node_id = row_id(operator, row, arguments)
                if node_id is None:
                    raise ShellRefusal(
                        "read_output_flags_conflict",
                        f"okg {operator} --ids: row {index} of page {self.pages} "
                        "carries no node_id",
                        fix_hint="add node_id to --select, or print rows with --jsonl",
                        row=index,
                    )
                lines.append(node_id)
        else:
            lines = [jsonl_line(row) for row in rows]
        for line in self._status(payload, arguments):
            if line not in self.said:
                self.said.add(line)
                say(line)
        if lines:
            sys.stdout.write("\n".join(lines) + "\n")
            sys.stdout.flush()
        self.rows += len(lines)

    def _status(self, payload: dict[str, Any], arguments: dict[str, Any]) -> list[str]:
        if self.pages > 1:
            # The first page's status says what the set is; later pages
            # add only notes not said yet.
            return [line for line in status_lines(self.plan.operator, payload, arguments)[1:]]
        return status_lines(self.plan.operator, payload, arguments)

    def finish(self) -> None:
        from okg.substrate.cli._read_operators import say

        if self.plan.all_pages and self.plan.mode != "json":
            say(f"okg {self.plan.operator} --all: {self.rows} rows in {self.pages} pages")


def drive(
    plan: Plan,
    args: argparse.Namespace,
    call: Callable[[dict[str, Any], str], Any],
    server: Any,
) -> int:
    """Make the plan's calls on one server and print the replies."""
    from okg.substrate.cli._read_operators import (
        print_reply,
        reply_refused,
        reply_text,
        say,
    )

    if plan.texts:
        return drive_texts(plan, args, call)
    try:
        resolve_subtypes(plan, server)
        emit_adjusted(plan)
        if plan.stdin_ids == []:
            say(f"okg {plan.operator}: no ids on standard input; nothing was read")
            return 0
        printer = _Printer(plan, args)
        calls = plan.calls()
        # `--json` over several replies (`--all`, or batches of ids from
        # standard input) prints them once all are read (`json_output`):
        # one list of pages per call.
        whole: Optional[list[list[dict[str, Any]]]] = (
            [] if plan.mode == "json" and (plan.all_pages or len(calls) > 1) else None
        )
        hinted = False
        for index, arguments in enumerate(calls):
            current = arguments
            if whole is not None:
                whole.append([])
            # The cap counts the pages `--all` follows from one call; each
            # batch of ids from standard input is a call of its own.
            followed = 0
            while True:
                if plan.all_pages and followed >= MAX_ALL_PAGES:
                    # Not a refusal but a partial result: stdout holds only
                    # what was read (under --json one document, marked not
                    # complete, with the last page's next call); stderr says
                    # how much and how to go on. Exit 0: a labelled cut is an
                    # answer; only a refusal exits 2.
                    if whole is not None:
                        _print_json(plan, whole, complete=False)
                        rows = _rows_read(plan, whole)
                    else:
                        rows = printer.rows
                    _say_cap(plan, args, current, rows, calls[index + 1:])
                    return 0
                followed += 1
                result = call(current, "text" if plan.mode == "text" else "json")
                printer.pages += 1
                text, _others = reply_text(result)
                if plan.mode == "text":
                    code = print_reply(noted_reply(plan.operator, result), as_json=False)
                    if code == REFUSED_EXIT:
                        line = retry_line(retry_call_in_text(text), current, args)
                        if line is not None:
                            sys.stderr.write(line + "\n")
                    if code:
                        return code
                    hint = None if hinted else pipe_hint(plan.operator, text, args)
                    if hint is not None:
                        # Not masked: the caller's own call, to be run as printed.
                        # At most one per command, however many calls it makes.
                        sys.stderr.write(hint + "\n")
                        hinted = True
                    break
                if reply_refused(result, text, as_json=True):
                    if whole is not None and any(whole):
                        say(_dropped_note(plan, whole, len(calls)))
                    return _print_refused(plan, args, result, text, current)
                if plan.mode == "json" and whole is None:
                    print_reply(result, as_json=True)
                    break
                try:
                    payload = json.loads(text)
                except ValueError:
                    payload = None
                if not isinstance(payload, dict):
                    raise SubstrateError(
                        f"okg {plan.operator}: the JSON reply is not one object"
                    )
                if whole is not None:
                    whole[-1].append(payload)
                else:
                    printer.page(payload, text, current)
                if not plan.all_pages:
                    break
                following = next_call(plan.operator, current, payload, page=printer.pages)
                if following is None:
                    break
                current = following
        if whole is not None:
            _print_json(plan, whole, complete=True)
        printer.finish()
        return 0
    except ShellRefusal as exc:
        return emit_refusal(exc, plan.operator, args, mode=plan.mode)
    except BrokenPipeError:
        _silence_stdout()
        return BROKEN_PIPE_EXIT


# ── okg inspect texts - ──────────────────────────────────────────────────

#: Fields of a `texts` batch item copied onto the printed record, under the
#: printed name. The item's `source_coverage` is the text's coverage.
_TEXT_FIELDS: tuple[tuple[str, str], ...] = (
    ("subtype", "subtype"),
    ("source_coverage", "coverage"),
    ("order_basis", "order_basis"),
    ("outcome", "outcome"),
    ("lines", "lines"),
    ("read_from", "read_from"),
    ("location_check", "location_check"),
)


def join_fenced(first: str, second: str) -> str:
    """Two served pieces of one chunk's text, the second going on where the
    first was cut, as one piece.

    An untrusted piece is served inside the untrusted-data fence, so a text
    cut by a page arrives as two fences. Joined, they are one fence around
    the two bodies, re-escaped: a fence marker split by the cut is whole
    again and must be escaped, as the read of the uncut text escapes it.
    Pieces in any other form are joined as they are."""
    from okg.substrate.mcp.provenance import FENCE_CLOSE, escape_embedded_fence_markers

    close = "\n" + FENCE_CLOSE
    head, newline, _rest = first.partition("\n")
    if (
        newline and head.startswith("⟦UNTRUSTED_DATA source=") and head.endswith("⟧")
        and first.endswith(close) and second.startswith(head + "\n")
        and second.endswith(close)
        and len(first) >= len(head) + 1 + len(close)
        and len(second) >= len(head) + 1 + len(close)
    ):
        body = first[len(head) + 1:-len(close)] + second[len(head) + 1:-len(close)]
        return f"{head}\n{escape_embedded_fence_markers(body)}{close}"
    return first + second


@dataclass
class _Text:
    """One id's text, put together from the pages of its batch."""

    requested: str
    fields: dict[str, Any] = field(default_factory=dict)
    #: `[chunk_id, end, text]` per piece; a piece cut by a page and the
    #: piece that goes on from it are one.
    pieces: list[list[Any]] = field(default_factory=list)
    provenance: list[Any] = field(default_factory=list)
    redacted: bool = False
    state: str = "deferred"
    error: Optional[dict[str, Any]] = None
    #: The last page that served this text a piece left it partial: the
    #: page's budget may have cut that piece.
    cut_open: bool = False

    def take(self, entry: dict[str, Any]) -> None:
        """Add one page's item. A piece goes on from the piece before it,
        on this page or an earlier one, when it starts where that piece was
        cut in the same chunk; a page's first piece only when the page that
        served the one before left the text partial. A page that serves the
        text nothing (`deferred`) changes neither."""
        cut_before = self.cut_open
        if entry.get("segments"):
            self.cut_open = entry.get("state") == "partial"
        if isinstance(entry.get("state"), str):
            self.state = entry["state"]
        if isinstance(entry.get("node_id"), str):
            self.fields["node_id"] = entry["node_id"]
        for name, printed in _TEXT_FIELDS:
            if name in entry:
                self.fields[printed] = entry[name]
        if isinstance(entry.get("error"), dict):
            self.error = entry["error"]
        for index, piece in enumerate(entry.get("segments") or []):
            if not isinstance(piece, dict):
                continue
            text = piece.get("text") if isinstance(piece.get("text"), str) else ""
            chunk, span = piece.get("chunk_id"), piece.get("text_range")
            start, end = (span[0], span[1]) if isinstance(span, list) and len(span) == 2 else (None, None)
            last = self.pieces[-1] if self.pieces else None
            # A piece goes on from the one before it when it starts where that
            # one was cut in the same chunk. The page that served the last
            # piece left it cut (`cut_before`) for a piece opening a page; a
            # page's own second piece can also be the rest of its first, when
            # the batch gave the same text a second share of the page.
            if (
                (index > 0 or cut_before) and last is not None and chunk is not None
                and last[0] == chunk and last[1] is not None and start == last[1]
            ):
                last[1] = end
                last[2] = join_fenced(last[2], text)
            else:
                self.pieces.append([chunk, end, text])
            if piece.get("redacted"):
                self.redacted = True
            provenance = piece.get("provenance")
            if provenance is not None and provenance not in self.provenance:
                self.provenance.append(provenance)

    def record(self, cut_reason: Optional[str]) -> dict[str, Any]:
        node_id = self.fields.get("node_id", self.requested)
        out: dict[str, Any] = {"node_id": node_id}
        if node_id != self.requested:
            out["requested_node_id"] = self.requested
        state = self.state
        if state not in ("delivered", "refused"):
            state = "cut"
        out["state"] = state
        out["text"] = None if state == "refused" else "".join(piece[2] for piece in self.pieces)
        out.update((k, v) for k, v in self.fields.items() if k != "node_id")
        out["chunks"] = len(self.pieces)
        if self.redacted:
            out["redacted"] = True
        if self.provenance:
            out["provenance"] = self.provenance
        if state == "cut":
            out["cut"] = True
            out["cut_reason"] = cut_reason or "the batch ended before this text was delivered"
        if self.error is not None:
            out["error"] = self.error
        return out


def _page_notes(payload: dict[str, Any], arguments: dict[str, Any]) -> list[str]:
    """The `! ` notes of one texts page, without rendering its texts."""
    page = {key: value for key, value in payload.items() if key != "items"}
    page["items"] = []
    return status_lines("inspect", page, arguments)[1:]


def drive_texts(
    plan: Plan, args: argparse.Namespace, call: Callable[[dict[str, Any], str], Any],
) -> int:
    """`okg inspect texts -`: one JSON line per id on stdin, its whole text.

    The ids go to the `texts` batch target `TEXTS_STDIN_BATCH` at a time,
    one call at a time; each batch follows `next` until the reply says it is
    complete, so a text larger than one call's `max_bytes` arrives in its
    continuation pages. A batch's records print in the order of the ids once
    the batch is done. Later batches read the generation the first read. A
    text the tool refused prints with its error; one not delivered whole
    prints with `"cut": true`; both are said on stderr. Once every text is
    printed the command exits 2 if a text was refused, else 0: a cut text is
    a labelled cut, an answer. A refusal of a whole call stops
    the command with exit 2; the texts printed stay printed. Two ids served
    as one record print it once."""
    from okg.substrate.cli._read_operators import reply_refused, reply_text, say

    ids = plan.stdin_ids or []
    try:
        emit_adjusted(plan)
        if not ids:
            say("okg inspect texts: no ids on standard input; nothing was read")
            return 0
        base = {
            name: plan.arguments[name]
            for name in ("generation_id", "authority", "max_bytes")
            if plan.arguments.get(name) is not None
        }
        said: set[str] = set()
        served: set[str] = set()
        calls = printed = refused = cut = 0
        for at in range(0, len(ids), TEXTS_STDIN_BATCH):
            batch = {
                str(number): _Text(node_id)
                for number, node_id in enumerate(ids[at:at + TEXTS_STDIN_BATCH], 1)
            }
            items = [
                {"key": key, "node_id": text.requested,
                 **({"lines": plan.text_lines} if plan.text_lines is not None else {})}
                for key, text in batch.items()
            ]
            current: dict[str, Any] = {**base, "target": {"kind": "texts", "items": items}}
            cut_reason: Optional[str] = None
            pages = 0
            while True:
                if pages >= MAX_TEXT_PAGES:
                    cut_reason = (
                        f"okg inspect texts makes at most {MAX_TEXT_PAGES} calls per "
                        f"batch of {TEXTS_STDIN_BATCH} texts; raise --max-bytes"
                    )
                    break
                result = call(current, "json")
                pages += 1
                calls += 1
                text, _others = reply_text(result)
                if reply_refused(result, text, as_json=True):
                    code = _print_refused(plan, args, result, text, current)
                    say(
                        f"okg inspect texts: stopped at text {at + 1} of {len(ids)}; "
                        f"{printed} texts were printed before"
                    )
                    return code
                try:
                    payload = json.loads(text)
                except ValueError:
                    payload = None
                if not isinstance(payload, dict):
                    raise SubstrateError("okg inspect texts: the JSON reply is not one object")
                if "generation_id" not in base and payload.get("generation_id") is not None:
                    base["generation_id"] = payload["generation_id"]
                for line in _page_notes(payload, current):
                    if line not in said:
                        said.add(line)
                        say(line)
                for entry in payload.get("items") or []:
                    if isinstance(entry, dict) and entry.get("key") in batch:
                        batch[entry["key"]].take(entry)
                token = payload.get("continuation_token")
                if isinstance(token, str) and token:
                    current = {"next": token, **{k: base[k] for k in ("authority",) if k in base}}
                    continue
                if payload.get("complete") is not True:
                    cut_reason = "the reply said the batch goes on and gave no next page"
                break
            lines: list[str] = []
            for text_of in batch.values():
                record = text_of.record(cut_reason)
                if record["state"] != "refused" and record["node_id"] in served:
                    # Two ids read as one record (a record and its text
                    # twin): its text prints once.
                    say(
                        f"! okg inspect texts: {text_of.requested} reads "
                        f"{record['node_id']}, whose text is printed already"
                    )
                    continue
                served.add(record["node_id"])
                lines.append(jsonl_line(record))
                if record["state"] == "refused":
                    refused += 1
                    error = record.get("error") or {}
                    say(
                        f"! okg inspect texts: {record['node_id']} refused "
                        f"{error.get('code')}: {error.get('message')}"
                    )
                elif record["state"] == "cut":
                    cut += 1
                    say(
                        f"! okg inspect texts: {record['node_id']} is cut, "
                        f"{len(record['text'])} characters printed: {record['cut_reason']}"
                    )
            if lines:
                sys.stdout.write("\n".join(lines) + "\n")
                sys.stdout.flush()
            printed += len(lines)
        summary = f"okg inspect texts: {printed} texts in {calls} calls"
        if refused:
            summary += f"; {refused} refused"
        if cut:
            summary += f"; {cut} cut"
        say(summary)
        # Every text is printed. A refused text exits 2; a cut one is a
        # labelled cut (`"cut": true` and a stderr note), an answer: exit 0.
        return REFUSED_EXIT if refused else 0
    except ShellRefusal as exc:
        return emit_refusal(exc, plan.operator, args, mode=plan.mode)
    except BrokenPipeError:
        _silence_stdout()
        return BROKEN_PIPE_EXIT


#: The largest page each tool serves (`filter`'s and `map`'s page is
#: `min(limit, 1000)`). Only these get "raise --limit" as a fix.
_MAX_PAGE_ROWS = {"filter": 1000, "map": 1000}


def _cap_hint(operator: str, arguments: dict[str, Any]) -> str:
    """What to change when `--all` reached `MAX_ALL_PAGES` and the set goes
    on: a larger page where one is allowed, else a narrower set."""
    largest = _MAX_PAGE_ROWS.get(operator)
    limit = arguments.get("limit")
    if largest is not None and limit is None:
        return f"pass --limit up to {largest} rows per page, or narrow the set with --where"
    if largest is not None and isinstance(limit, int) and limit < largest:
        return f"raise --limit (now {limit} rows per page, at most {largest}), or narrow the set with --where"
    return "narrow the set with --where; the pages cannot be larger"


def _print_refused(
    plan: Plan, args: argparse.Namespace, result: Any, text: str, sent: dict[str, Any],
) -> int:
    """A tool's refusal in a JSON mode: the JSON with `--json`, otherwise
    the text an MCP client reads, then the `retry:` line."""
    from okg.substrate.cli._read_operators import (
        SERVER_FAULT_CODE,
        reply_error_code,
    )

    payload: Any
    try:
        payload = json.loads(text)
    except ValueError:
        payload = None
    if plan.mode == "json" or not isinstance(payload, dict):
        sys.stderr.write(text if text.endswith("\n") else text + "\n")
    else:
        from okg.substrate.mcp.result_text import render

        rendered = render(
            payload, tool=plan.operator, raised=bool(getattr(result, "isError", False)),
        )
        sys.stderr.write(rendered + "\n")
        error = payload.get("error")
        line = retry_line(
            error.get("retry_call") if isinstance(error, dict) else None, sent, args,
        )
        if line is not None:
            sys.stderr.write(line + "\n")
    if reply_error_code(text, as_json=True) == SERVER_FAULT_CODE:
        return 1
    return REFUSED_EXIT


def _silence_stdout() -> None:
    """The reader of the pipe is gone: later writes, and the flush at exit,
    go nowhere instead of raising again."""
    try:
        devnull = os.open(os.devnull, os.O_WRONLY)
        os.dup2(devnull, sys.stdout.fileno())
    except (OSError, ValueError, AttributeError):
        pass


# ── flags ────────────────────────────────────────────────────────────────

_WHERE_HELP = (
    "a condition, repeatable, joined by and: k=v, k!=v, k>v, k<v, k>=v, "
    "k<=v or k~prefix; v is true, false, null, a number, a string, or "
    "a,b for any of. A JSON condition also works"
)


def add_output_flags(parser: Any) -> list[Any]:
    """`--jsonl`, `--ids` and `--all`; returns the actions added."""
    return [
        parser.add_argument(
            "--jsonl", dest=JSONL_ATTR, action="store_true", default=False,
            help="one JSON row per line on stdout; the status line and notes on stderr",
        ),
        parser.add_argument(
            "--ids", dest=IDS_ATTR, action="store_true", default=False,
            help="one id per line on stdout; the status line and notes on stderr",
        ),
        parser.add_argument(
            "--all", dest=ALL_ATTR, action="store_true", default=False,
            help=(
                f"read every page, following next to the end (at most "
                f"{MAX_ALL_PAGES}); refused when a page gives no way on. "
                "Prints JSON Lines unless --ids, or one JSON document with --json"
            ),
        ),
    ]


def add_shell_flags(parser: Any, operator: str) -> None:
    """This module's flags on one read command's parser (not `search`)."""
    add_output_flags(parser)
    if operator in WHERE_COMMANDS:
        parser.add_argument(
            "--where", dest=WHERE_ATTR, action="append", default=None,
            metavar="CONDITION", help=_WHERE_HELP,
        )
    if operator in TYPE_COMMANDS:
        parser.add_argument(
            "--type", dest=TYPE_ATTR, default=None, metavar="SUBTYPE",
            help="the node set of one declared subtype (input.subtype)",
        )
    if operator in STDIN_COMMANDS:
        parser.add_argument(
            "--stdin", dest=STDIN_ATTR, action="store_true", default=False,
            help=(
                "read node ids from standard input, one per line or JSON "
                "Lines with node_id"
                + (" (also: -)" if operator in ("inspect", "expand") else "")
            ),
        )
    if operator == "inspect":
        parser.add_argument(
            FROM_ATTR, nargs="?", default=None, metavar="NODE_ID",
            help=(
                "a node id, read as --target {\"kind\": \"node\", ...}; - reads ids "
                "from standard input; texts - prints the whole text of each id "
                "piped in, one JSON line per text"
            ),
        )
        parser.add_argument(
            MORE_ATTR, nargs="?", default=None, metavar="-",
            help="after texts: read the ids from standard input (also: --stdin)",
        )
        parser.add_argument(
            "--lines", dest=LINES_ATTR, default=None, metavar="RANGES",
            help=(
                "with texts -: read these lines of each text, FIRST-LAST "
                "comma-separated (1-40,120-160); FIRST- reads to the end"
            ),
        )


def search_where_help() -> str:
    return _WHERE_HELP


def search_flag_action(argument: str) -> str:
    """The argparse action of one `okg search` tool flag: `--where` repeats."""
    return "append" if argument == "where" else "store"


def extend_search_flag_help(action: Any) -> None:
    """Add the k=v terms to `okg search --where`'s help, after whatever
    help the flag was given, so both stay."""
    if action.dest == "op_where":
        action.help = f"{action.help}. {_WHERE_HELP}" if action.help else _WHERE_HELP
