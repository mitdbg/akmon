#!/usr/bin/env python3
"""Write `src/okg/substrate/cli/_read_help_text.py` from the MCP server.

`okg search|inspect|expand|filter|aggregate|map|query --help` prints what
the MCP server tells a client about the tool of the same name: its
description as served, one line per argument giving its type and its allowed
values or bounds, and one worked example. The parser of every okg command is
built on each call and must not import the server, so this script builds the
server once, with no database, and writes the text into a module the parser
reads.

The build is the one the surface budget test measures: probe mode (the
connection pools are built unopened), the `full` capability fixture, no tool
allowlist and no writes, deployment `default`. A deployment whose server
serves fewer methods or tools serves less than this text shows.

A test (`tests/substrate/cli/test_read_help_text.py`) compares the checked-in
module with a fresh generation, so a change to a tool's description or
schema fails until this script is run again:

    python scripts/gen_read_help_text.py           # write the module
    python scripts/gen_read_help_text.py --check   # exit 1 when it differs
"""
from __future__ import annotations

import argparse
import contextlib
import json
import os
import sys
import tempfile
from pathlib import Path
from typing import Any, Iterator

ROOT = Path(__file__).resolve().parents[1]
TARGET = ROOT / "src" / "okg" / "substrate" / "cli" / "_read_help_text.py"

#: The launch the surface budget test pins (its `_hermetic_probe` fixture):
#: variable -> value, or None to unset it for the build.
HERMETIC_ENV: dict[str, str | None] = {
    "OKG_MCP_SURFACE_PROBE": "1",
    "OKG_MCP_TRACE_ENABLED": "0",
    "OKG_MCP_CAPABILITY_FIXTURE": "full",
    "OKG_MCP_TOOL_ALLOWLIST": None,
    "OKG_MCP_ALLOW_WRITES": None,
    # The server derives its read role's connection string before it builds
    # the (unopened) pools. Like the test suite (`tests/conftest.py`), the
    # build may use the built-in role password, which names the probe
    # database that is never reached; `hermetic_environment` points the role
    # secrets file at a path that does not exist, so no real file is read.
    "OKG_ALLOW_BUILTIN_ROLE_PASSWORDS": "1",
    "OKG_ROLE_SECRETS_FILE": None,
}
DEPLOYMENT = "default"

#: One worked example per tool: the command line after `okg <tool>`, and the
#: argument object it sends. These are the calls of `CASES` in
#: `tests/substrate/cli/test_read_operator_commands_db.py`, which runs each
#: against a published graph; a test there checks the two stay equal.
_TERM = "zirconium"
_WHERE_DOC = {"op": "eq", "field": "type", "value": "doc"}
_TICKETS = {"kind": "nodes", "subtype": "ticket"}
_SQL = "SELECT node_id FROM okg.v_nodes ORDER BY node_id LIMIT 3"
EXAMPLES: dict[str, tuple[tuple[str, ...], dict[str, Any]]] = {
    "search": (
        (_TERM, "--where", json.dumps(_WHERE_DOC), "--limit", "5"),
        {"query": _TERM, "where": _WHERE_DOC, "limit": 5},
    ),
    "inspect": (
        ("--target", '{"kind": "node", "node_id": "doc:000"}'),
        {"target": {"kind": "node", "node_id": "doc:000"}},
    ),
    "expand": (
        ("doc:000", "--edge-type", "contains"),
        {"input": "doc:000", "edge_type": "contains"},
    ),
    "filter": (
        ("--input", json.dumps(_TICKETS), "--predicate", "{}"),
        {"input": _TICKETS, "predicate": {}},
    ),
    "aggregate": (
        ("--input", '{"kind": "nodes"}', "--by", "subtype", "--metrics", "count"),
        {"input": {"kind": "nodes"}, "by": ["subtype"], "metrics": ["count"]},
    ),
    "map": (
        ("--input", json.dumps(_TICKETS), "--projection", "node_id"),
        {"input": _TICKETS, "projection": ["node_id"]},
    ),
    "query": (
        (_SQL, "--row-cap", "10"),
        {"sql": _SQL, "row_cap": 10},
    ),
}

#: The line of an argument the tool accepts but does not publish.
HIDDEN = "accepted, but not in the tool's published arguments"

#: The line of `search`'s `subtype_filter`, `attr_filters` and `filter`
#: (`operator_models.UNPUBLISHED_SEARCH_ARGUMENTS`): the only hidden
#: arguments the tool reads as a `where` condition.
UNPUBLISHED = HIDDEN + "; the tool reads it as a `where` condition"

#: Added to the line of an argument the read-only `surface` listing names
#: (`lean_surface.HIDDEN_ARGUMENTS`) when that listing has no note for it.
SURFACE_POINTER = '; inspect(target={"kind":"surface"}) lists it'


def hidden_line(tool: str | None, name: str) -> str:
    """The line of an argument `tool` accepts but does not publish: what it
    does when the `surface` listing says so (`lean_surface.HIDDEN_NOTES`),
    and the `where` sentence only for the arguments `search` lifts into
    `where`. `tool=None` knows only the second."""
    from okg.substrate.mcp.lean_surface import HIDDEN_ARGUMENTS, HIDDEN_NOTES
    from okg.substrate.mcp.operator_models import UNPUBLISHED_SEARCH_ARGUMENTS

    if tool in (None, "search") and name in UNPUBLISHED_SEARCH_ARGUMENTS:
        return UNPUBLISHED
    if tool is None:
        return HIDDEN
    note = HIDDEN_NOTES.get(f"{tool}({name}=...)")
    if note:
        return f"{HIDDEN}: {note}"
    if name in HIDDEN_ARGUMENTS.get(tool, ()):
        return HIDDEN + SURFACE_POINTER
    return HIDDEN


@contextlib.contextmanager
def hermetic_environment() -> Iterator[None]:
    """`HERMETIC_ENV` for the length of the block, with the role secrets
    file in a new empty directory; the rest put back."""
    saved = {name: os.environ.get(name) for name in HERMETIC_ENV}
    try:
        with tempfile.TemporaryDirectory(prefix="okg-help-role-secrets-") as empty:
            for name, value in HERMETIC_ENV.items():
                if value is None:
                    os.environ.pop(name, None)
                else:
                    os.environ[name] = value
            os.environ["OKG_ROLE_SECRETS_FILE"] = str(Path(empty) / "role-secrets.json")
            yield
    finally:
        for name, value in saved.items():
            if value is None:
                os.environ.pop(name, None)
            else:
                os.environ[name] = value


def build_probe_server() -> Any:
    """The read server of `HERMETIC_ENV`, built without a database. Call it
    inside `hermetic_environment` and close it with `close_probe_server`."""
    from okg.substrate.mcp.server import build_server
    from okg.substrate.mcp.surface_budget import PROBE_DSN

    return build_server(dsn=PROBE_DSN, deployment_name=DEPLOYMENT, allow_writes=False)


def close_probe_server(server: Any) -> None:
    try:
        server._okg_pool.close()
    finally:
        write_pool = getattr(server, "_okg_write_pool", None)
        if write_pool is not None:
            write_pool.close()


def read_surface() -> dict[str, tuple[str, dict[str, Any]]]:
    """Each read tool's served description and input schema, in the order
    of `OPERATOR_ARGUMENTS`."""
    import anyio

    from okg.substrate.cli._read_operators import OPERATOR_ARGUMENTS

    with hermetic_environment():
        server = build_probe_server()
        try:
            listed = {tool.name: tool for tool in anyio.run(server.list_tools)}
        finally:
            close_probe_server(server)
    missing = [name for name in OPERATOR_ARGUMENTS if name not in listed]
    if missing:
        raise RuntimeError(
            f"the probe server lists no tool named {', '.join(missing)}; "
            "the read commands need every one"
        )
    return {
        name: (listed[name].description or "", dict(listed[name].inputSchema or {}))
        for name in OPERATOR_ARGUMENTS
    }


# ── one line per argument, from the published schema ────────────────────


def _resolve(schema: Any, defs: dict[str, Any]) -> Any:
    """`schema` with its `$ref` followed, keeping keys written beside it."""
    for _ in range(32):
        if not isinstance(schema, dict) or "$ref" not in schema:
            return schema
        name = str(schema["$ref"]).rsplit("/", 1)[-1]
        if name not in defs:
            raise LookupError(
                f"the schema refers to {schema['$ref']!r}, which it does not define"
            )
        beside = {key: value for key, value in schema.items() if key != "$ref"}
        schema = {**defs[name], **beside}
    raise LookupError("a $ref chain longer than 32")


def _word(value: Any) -> str:
    return value if isinstance(value, str) else json.dumps(value)


def _count(number: Any, unit: str) -> str:
    return f"{number} {unit[:-1] if number == 1 else unit}"


def _length(schema: dict[str, Any], low: str, high: str, unit: str) -> str:
    least, most = schema.get(low), schema.get(high)
    if least is not None and least == most:
        return f", exactly {_count(most, unit)}"
    if least is not None and most is not None:
        return f", {least} to {_count(most, unit)}"
    if least is not None:
        return f", at least {_count(least, unit)}"
    if most is not None:
        return f", at most {_count(most, unit)}"
    return ""


def _range(schema: dict[str, Any]) -> str:
    least, most = schema.get("minimum"), schema.get("maximum")
    text = ""
    if least is not None and most is not None:
        text = f" from {least} to {most}"
    elif least is not None:
        text = f", at least {least}"
    elif most is not None:
        text = f", at most {most}"
    if schema.get("exclusiveMinimum") is not None:
        text += f", more than {schema['exclusiveMinimum']}"
    if schema.get("exclusiveMaximum") is not None:
        text += f", less than {schema['exclusiveMaximum']}"
    return text


def _members(schema: dict[str, Any]) -> list[Any] | None:
    for key in ("oneOf", "anyOf"):
        if isinstance(schema.get(key), list):
            return [member for member in schema[key] if member != {"type": "null"}]
    return None


def _types(schema: dict[str, Any]) -> list[str]:
    declared = schema.get("type")
    if isinstance(declared, str):
        declared = [declared]
    if not isinstance(declared, list):
        return []
    return [kind for kind in declared if kind != "null"]


def _by_kind(members: list[Any], defs: dict[str, Any]) -> str | None:
    """A union of objects told apart by `kind`, one clause per member, or
    None when the members are not that."""
    rows: list[str] = []
    for member in members:
        member = _resolve(member, defs)
        properties = member.get("properties") if isinstance(member, dict) else None
        if not isinstance(properties, dict) or "kind" not in properties:
            return None
        tag = _resolve(properties["kind"], defs)
        values = [tag["const"]] if "const" in tag else tag.get("enum")
        if not values:
            return None
        required = [name for name in member.get("required") or () if name != "kind"]
        optional = [name for name in properties if name != "kind" and name not in required]
        row = "kind " + " or ".join(_word(value) for value in values if value is not None)
        if required:
            row += " needs " + ", ".join(required)
        if optional:
            row += (", may take " if required else " may take ") + ", ".join(optional)
        rows.append(row)
    return "object, one of: " + "; ".join(rows)


def _object(schema: dict[str, Any], defs: dict[str, Any], depth: int) -> str:
    properties = schema.get("properties") or {}
    extra = schema.get("additionalProperties")
    if not properties:
        if isinstance(extra, dict) and extra:
            return "object whose values are " + summary(extra, defs, depth + 1)
        return "object"
    if depth >= 1:
        return "object with keys " + ", ".join(properties)
    required = set(schema.get("required") or ())
    parts = [
        f"{name}{' (required)' if name in required else ''}: "
        + summary(body, defs, depth + 1)
        for name, body in properties.items()
    ]
    return "object {" + "; ".join(parts) + "}"


def _typed(schema: dict[str, Any], kind: str, defs: dict[str, Any], depth: int) -> str:
    if kind == "string":
        text = "string" + _length(schema, "minLength", "maxLength", "characters")
        if "pattern" in schema:
            text += f" matching {schema['pattern']}"
        return text
    if kind in ("integer", "number"):
        return kind + _range(schema)
    if kind == "boolean":
        return "true or false"
    if kind == "array":
        items = schema.get("items")
        inner = (
            summary(items, defs, depth)
            if isinstance(items, dict) and items else "any JSON value"
        )
        return "list of " + inner + _length(schema, "minItems", "maxItems", "items")
    if kind == "object":
        return _object(schema, defs, depth)
    return kind


def summary(schema: Any, defs: dict[str, Any], depth: int = 0) -> str:
    """One line for a JSON schema: its type and its allowed values or bounds.

    Objects are spelled out key by key at the top level (`depth` 0) and
    named by their keys below it, so a recursive grammar ends.
    """
    schema = _resolve(schema, defs)
    if not isinstance(schema, dict) or not schema:
        return "any JSON value"
    if "const" in schema:
        return _word(schema["const"])
    if isinstance(schema.get("enum"), list):
        return "one of " + ", ".join(
            _word(value) for value in schema["enum"] if value is not None
        )
    members = _members(schema)
    if members is not None:
        beside = {
            key: value for key, value in schema.items()
            if key not in ("oneOf", "anyOf", "discriminator")
        }
        if len(members) == 1:
            return summary({**beside, **_resolve(members[0], defs)}, defs, depth)
        if not members:
            return "null"
        tagged = _by_kind(members, defs) if depth == 0 else None
        if tagged is not None:
            return tagged
        # Sorted, not in schema order: Python's typing cache treats
        # Union[int, str] and Union[str, int] as one key, so the order the
        # server publishes depends on which spelling was imported first.
        return " or ".join(
            sorted({summary(member, defs, depth) for member in members})
        )
    if isinstance(schema.get("allOf"), list) and len(schema["allOf"]) == 1:
        beside = {key: value for key, value in schema.items() if key != "allOf"}
        return summary({**beside, **_resolve(schema["allOf"][0], defs)}, defs, depth)
    kinds = _types(schema)
    if not kinds:
        if "properties" in schema:
            kinds = ["object"]
        elif "items" in schema:
            kinds = ["array"]
        else:
            # An argument published untyped (lean_surface.UNTYPED) carries
            # its meaning in its one-line description.
            described = schema.get("description") if depth == 0 else None
            return described if isinstance(described, str) and described else "any JSON value"
    return " or ".join(sorted({_typed(schema, kind, defs, depth) for kind in kinds}))


def argument_lines(
    arguments: tuple[str, ...], schema: dict[str, Any], tool: str | None = None,
) -> dict[str, str]:
    """`argument -> line` for every argument the command has a flag for;
    `tool` names the tool whose hidden arguments these are (`hidden_line`)."""
    properties = schema.get("properties") or {}
    defs = schema.get("$defs") or schema.get("definitions") or {}
    required = set(schema.get("required") or ())
    lines: dict[str, str] = {}
    for name in arguments:
        if name not in properties:
            lines[name] = hidden_line(tool, name)
            continue
        body = _resolve(properties[name], defs)
        text = summary(body, defs, 0)
        if isinstance(body, dict) and body.get("default") is not None:
            text += f"; default {_word(body['default'])}"
        lines[name] = ("required. " if name in required else "") + text
    return lines


# ── the module ───────────────────────────────────────────────────────────


def _literal(value: Any, indent: int) -> str:
    """`value` as Python source, one item per line, every string a single
    literal (never two adjacent ones)."""
    pad, inner = " " * indent, " " * (indent + 4)
    if isinstance(value, dict):
        if not value:
            return "{}"
        body = "".join(
            f"{inner}{json.dumps(key)}: {_literal(item, indent + 4)},\n"
            for key, item in value.items()
        )
        return "{\n" + body + pad + "}"
    if isinstance(value, (list, tuple)):
        if not value:
            return "()" if isinstance(value, tuple) else "[]"
        body = "".join(f"{inner}{_literal(item, indent + 4)},\n" for item in value)
        opening, closing = ("(", ")") if isinstance(value, tuple) else ("[", "]")
        return opening + "\n" + body + pad + closing
    if isinstance(value, str):
        return json.dumps(value, ensure_ascii=False)
    if value is None or isinstance(value, (bool, int, float)):
        return repr(value)
    raise TypeError(f"cannot write a {type(value).__name__} into the help module")


_HEADER = '''"""What `okg <read tool> --help` prints. GENERATED: do not edit.

Written by `scripts/gen_read_help_text.py` from the MCP server built with no
database (probe mode, the `full` capability fixture, deployment `default`).
`tests/substrate/cli/test_read_help_text.py` fails when this file and a fresh
generation differ; run the script to write it again.

Per tool: `description`, the tool's description exactly as the server lists
it, one string per line; `arguments`, one line per argument from the
published input schema; `example_argv`, a command line after `okg <tool>`;
and `example_arguments`, the argument object that command line sends.
"""
from __future__ import annotations

from typing import Any

'''


def render(surface: dict[str, tuple[str, dict[str, Any]]] | None = None) -> str:
    """The module's text."""
    from okg.substrate.cli._read_operators import OPERATOR_ARGUMENTS

    surface = read_surface() if surface is None else surface
    data: dict[str, Any] = {}
    for name, arguments in OPERATOR_ARGUMENTS.items():
        description, schema = surface[name]
        argv, example = EXAMPLES[name]
        data[name] = {
            "description": tuple(description.split("\n")),
            "arguments": argument_lines(arguments, schema, name),
            "example_argv": tuple(argv),
            "example_arguments": example,
        }
    return (
        _HEADER + "READ_HELP: dict[str, dict[str, Any]] = " + _literal(data, 0) + "\n"
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    parser.add_argument(
        "--check", action="store_true",
        help="write nothing; exit 1 when the checked-in module differs",
    )
    args = parser.parse_args(argv)
    text = render()
    current = TARGET.read_text(encoding="utf-8") if TARGET.exists() else None
    name = TARGET.relative_to(ROOT)
    if args.check:
        if current != text:
            print(f"{name} is out of date; run {Path(__file__).name}", file=sys.stderr)
            return 1
        return 0
    if current != text:
        TARGET.write_text(text, encoding="utf-8")
        print(f"wrote {name}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
