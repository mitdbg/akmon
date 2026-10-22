"""Tool descriptions rendered for the surface that serves them.

PACT dataset-card, req.dataset-card.tool-prose. A tool's docstring is written
once, for every launch, but a launch serves a subset: a discovery profile has
no `query`, a read launch has no `surface` inspect kind, a deployment without
live relations cannot take `expand(live=...)`. A description that names what
the surface does not serve sends an agent to a refusal.

So the sentences that depend on the surface are marked in the docstring and
rendered at build time from the served sets, the way the `search` method enum
is compiled from what the server can run:

- ``[[if COND]]...[[/if]]`` keeps its text only when COND holds, with an
  optional ``[[else]]...``; blocks nest. COND is one or more space-separated
  terms, all of which must hold: ``tool:a|b`` (any of these tools is served),
  ``kind:a|b`` (any of these `inspect` kinds is served), ``live`` (the
  deployment declares a live relation), ``run_live`` (``live``, and the
  operator has not turned `run`'s live steps off).
- ``[[tools a/b/c]]`` renders the served names among a, b, c, joined by ``/``.
- ``[[subtype]]`` renders `aggregate`'s example subtype: one of the
  deployment's own catalog subtypes (`example_subtype_for`).

Malformed markup or an unknown term raises `ToolProseInvalid` at server
start: a marker left in a published description, or a condition that never
matches, would be a silent wrong text.
"""
from __future__ import annotations

import re
from typing import Any, Iterable, Optional

from okg.substrate.errors import SubstrateError
from okg.substrate.mcp.effective_surface import INSPECT_TARGET_KINDS
from okg.substrate.mcp.tool_profiles import ALL_ADMITTED_TOOLS

__all__ = [
    "EXAMPLE_SUBTYPE_PLACEHOLDER",
    "ToolProseInvalid",
    "example_subtype_for",
    "inspect_kinds_named_in",
    "publish_surface_descriptions",
    "render_tool_prose",
    "tools_mentioned_in",
]

#: `aggregate`'s example subtype when the catalog is not known at start (no
#: database, or the caller passed its own instructions).
EXAMPLE_SUBTYPE_PLACEHOLDER = "<subtype>"

#: A catalog subtype name safe to print inside a JSON example. Subtype names
#: are deployment schema, but they reach the model, so anything else is
#: skipped rather than quoted.
_SAFE_SUBTYPE = re.compile(r"^[A-Za-z][A-Za-z0-9_.:-]{0,63}$")

_TOKEN = re.compile(r"\[\[(.*?)\]\]", re.DOTALL)


class ToolProseInvalid(SubstrateError):
    """A tool docstring's surface markup cannot be rendered."""

    code = "tool_prose_invalid"


def _holds(
    cond: str, *, tools: frozenset[str], kinds: frozenset[str], live: bool,
    run_live: bool = True,
) -> bool:
    terms = cond.split()
    if not terms:
        raise ToolProseInvalid("an [[if]] block names no condition")
    for term in terms:
        if term == "run_live":
            ok = live and run_live
        elif term == "live":
            ok = live
        elif term.startswith("tool:"):
            names = term[len("tool:"):].split("|")
            unknown = [n for n in names if n not in ALL_ADMITTED_TOOLS]
            if unknown:
                raise ToolProseInvalid(f"[[if {cond}]] names unknown tools {unknown}")
            ok = any(n in tools for n in names)
        elif term.startswith("kind:"):
            names = term[len("kind:"):].split("|")
            unknown = [n for n in names if n not in INSPECT_TARGET_KINDS]
            if unknown:
                raise ToolProseInvalid(
                    f"[[if {cond}]] names unknown inspect kinds {unknown}"
                )
            ok = any(n in kinds for n in names)
        else:
            raise ToolProseInvalid(
                f"[[if {cond}]]: unknown term {term!r}; use tool:, kind:, live or run_live"
            )
        if not ok:
            return False
    return True


def render_tool_prose(
    text: str,
    *,
    tools: Iterable[str],
    kinds: Iterable[str],
    live: bool,
    example_subtype: str,
    run_live: bool = True,
) -> str:
    """Render one description's surface markup for the served sets."""
    served_tools = frozenset(str(t) for t in tools)
    served_kinds = frozenset(str(k) for k in kinds)
    out: list[str] = []
    # Each frame: [keep_this_branch, parent_keeps, in_else].
    stack: list[list[bool]] = []

    def keeping() -> bool:
        return all(frame[0] and frame[1] for frame in stack) if stack else True

    pos = 0
    for match in _TOKEN.finditer(text):
        if keeping():
            out.append(text[pos:match.start()])
        pos = match.end()
        body = match.group(1).strip()
        if body.startswith("if "):
            parent = keeping()
            stack.append([
                _holds(
                    body[3:], tools=served_tools, kinds=served_kinds,
                    live=live, run_live=run_live,
                ),
                parent,
                False,
            ])
        elif body == "else":
            if not stack or stack[-1][2]:
                raise ToolProseInvalid("[[else]] outside an [[if]] block")
            stack[-1][0] = not stack[-1][0]
            stack[-1][2] = True
        elif body == "/if":
            if not stack:
                raise ToolProseInvalid("[[/if]] without an [[if]]")
            stack.pop()
        elif body == "subtype":
            if keeping():
                out.append(example_subtype)
        elif body.startswith("tools "):
            names = body[len("tools "):].split("/")
            unknown = [n for n in names if n not in ALL_ADMITTED_TOOLS]
            if unknown:
                raise ToolProseInvalid(f"[[{body}]] names unknown tools {unknown}")
            if keeping():
                out.append("/".join(n for n in names if n in served_tools))
        else:
            raise ToolProseInvalid(f"unknown description marker [[{body}]]")
    if stack:
        raise ToolProseInvalid("an [[if]] block is not closed")
    out.append(text[pos:])
    rendered = "".join(out)
    if "[[" in rendered or "]]" in rendered:
        raise ToolProseInvalid("a description marker was left unrendered")
    # Removing a clause can leave doubled spaces or an emptied paragraph.
    rendered = re.sub(r"(?<=\S) {2,}", " ", rendered)
    rendered = re.sub(r"[ \t]+\n", "\n", rendered)
    rendered = re.sub(r"\n{3,}", "\n\n", rendered)
    return rendered.strip()


def publish_surface_descriptions(
    mcp: Any,
    *,
    session_tools: Iterable[str],
    kinds: Iterable[str],
    live: bool,
    example_subtype: str,
    run_live: bool = True,
) -> None:
    """Render every registered tool's description for this surface.

    `session_tools` is what a session starts with; a tool is named only when
    it is both registered and in that set, so a description never sends a
    session to a tool it cannot call yet.
    """
    manager = getattr(mcp, "_tool_manager", None)
    registered = getattr(manager, "_tools", None)
    if not isinstance(registered, dict):
        raise ToolProseInvalid(
            "could not reach the registered tools through the MCP SDK "
            "internals (_tool_manager._tools); refusing to publish tool "
            "descriptions with unrendered surface markup",
            fix_hint="an MCP SDK upgrade likely renamed these internals",
        )
    tools = set(registered) & {str(t) for t in session_tools}
    for name, tool in registered.items():
        # okg's own tools only: a deployment-declared tool's description is
        # the operator's text, and `[[...]]` in it is not this markup.
        if name not in ALL_ADMITTED_TOOLS:
            continue
        description = getattr(tool, "description", None)
        if not isinstance(description, str):
            continue
        try:
            tool.description = render_tool_prose(
                description, tools=tools, kinds=kinds, live=live,
                example_subtype=example_subtype, run_live=run_live,
            )
        except ToolProseInvalid as exc:
            raise ToolProseInvalid(f"tool {name!r}: {exc}") from exc


def example_subtype_for(facts: Optional[Any]) -> str:
    """The subtype `aggregate`'s example names, from the deployment's catalog.

    The subtype with the most rows when the counts were read, else the first
    catalog subtype by name; passage children (a rollup's chunk subtype) are
    skipped while any other subtype is known. No catalog: the placeholder.
    """
    if facts is None:
        return EXAMPLE_SUBTYPE_PLACEHOLDER
    children = {child for _parent, child, _edge in getattr(facts, "nesting", ())}
    ranked: list[str] = []
    counts = getattr(facts, "source_counts", None)
    if counts:
        totals: dict[str, int] = {}
        for source in counts:
            for subtype, n in source.subtypes:
                totals[str(subtype)] = totals.get(str(subtype), 0) + int(n)
        ranked = sorted(totals, key=lambda s: (-totals[s], s))
    else:
        ranked = sorted(str(s) for s in getattr(facts, "catalog_subtypes", ()))
    safe = [s for s in ranked if _SAFE_SUBTYPE.match(s)]
    for subtype in safe:
        if subtype not in children:
            return subtype
    return safe[0] if safe else EXAMPLE_SUBTYPE_PLACEHOLDER


# ---------- scans, for tests and readbacks ----------


def tools_mentioned_in(
    text: str, *, candidates: Iterable[str] | None = None,
) -> tuple[str, ...]:
    """Tool names `text` refers to as tools.

    Stricter than `tools_named_in`, which matches any whole word: `record`,
    `query`, `filter`, `map` and `inspect` are also ordinary words in these
    descriptions ("one card per record", "inspect admission proposals").
    A reference here is a backticked name, a call (`name(`), or a member of
    a slash list (`map/filter/aggregate`), the forms the descriptions use
    when they send an agent to another tool.
    """
    pool = tuple(candidates) if candidates is not None else ALL_ADMITTED_TOOLS
    found = []
    for name in pool:
        n = re.escape(name)
        if re.search(
            rf"`{n}`|`{n}\(|(?<![\w.]){n}\(|(?<![\w.]){n}/(?=\w)|(?<=\w)/{n}(?!\w)",
            text,
        ):
            found.append(name)
    return tuple(sorted(set(found)))


def inspect_kinds_named_in(text: str) -> tuple[str, ...]:
    """`inspect` kinds `text` names: backticked, or as a `kind` value."""
    named = set(re.findall(r"`([a-z_]+)`", text))
    named |= set(re.findall(r"kind['\"]?\s*[:=]\s*['\"]([a-z_]+)", text))
    return tuple(sorted(k for k in named if k in set(INSPECT_TARGET_KINDS)))
