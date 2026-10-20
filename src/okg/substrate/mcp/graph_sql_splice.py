"""Build the SQL a graph-backed ``run_sql`` call executes, without re-rendering.

Issue #2899. The graph path used to re-render the caller's checked query four
times (sqlglot view expansion, two pglast printer passes, and the Chronos
sqlglot table rewrite), then let the Chronos database adapter rewrite ``:name``
and ``%`` in the result with a text scanner. Each re-render is a chance for the
executed SQL to differ from the SQL ``validate_run_sql`` approved. Verified
effects on dev: a caller's string literal ``'%(_chronos_branch_point)s'`` came
back as ``$1``, and ``SELECT v_nodes.node_id FROM okg.v_nodes`` (no alias)
failed with "missing FROM-clause entry".

This module builds the executed SQL from the caller's checked text (the text
``checked_run_sql`` returns, with ``U&`` spellings already decoded). It copies
that text unchanged and changes it at exactly three kinds of place, all found
from parse positions (sqlglot's identifier offsets, cross-checked against the
PostgreSQL-rule lexer in `okg.substrate.postgres_sql`; PACT
``sql-safety-without-pglast`` replaced the GPL pglast parser here):

* a composite ``okg.v_*`` view reference becomes ``(<pg_get_viewdef body>)``
  when the view reads a graph relation;
* a graph relation reference (``okg.v_nodes``/``okg.v_edges`` or a table the
  Chronos checkout registers) becomes ``(<checkout subquery>)``, with the
  branch point written as an integer literal;
* the whole statement is wrapped as
  ``SELECT * FROM (<statement>) AS _okg_bounded LIMIT <row_cap + 1>``.

A replaced reference keeps the caller's alias text. With no alias it gets the
relation name as its alias, which is the name PostgreSQL gives an unaliased
relation, so ``v_nodes.node_id`` still resolves.

The executed text is run with no parameters, so neither psycopg nor the Chronos
adapter scans it for placeholders. Before it runs, `verify_splice` tokenizes the
checked text and the executed text with the lexer written from PostgreSQL's
scanner rules and refuses if any caller token outside a replaced range changed
or moved.

The scanner, the checker and this splice all read ``'...'`` the way PostgreSQL
does under ``standard_conforming_strings = on``. The graph path therefore
refuses a connection where it is off, sets it on for the transaction that runs
the text (``run_sql._enter_graph_read_boundary``), and refuses the string forms
whose meaning depends on escape processing: ``E'...'`` and ``U&'...'``. A
backslash inside a plain ``'...'`` string is an ordinary character under that
setting and is served (issue #3666).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable, Iterable

from okg.substrate.mcp.sql_safety import RunSqlInvalid


__all__ = [
    "BOUNDED_ALIAS",
    "GraphSqlPlan",
    "RunSqlGraphFormUnsupported",
    "RunSqlGraphLiteralRefused",
    "RunSqlGraphSpliceMismatch",
    "RunSqlGraphStringModeRefused",
    "RunSqlGraphViewUnplanned",
    "SplicedGraphSql",
    "assert_standard_conforming_strings",
    "dollar_quoted_rewrite",
    "execute_spliced_sql",
    "plan_graph_sql",
    "refuse_escape_dependent_literals",
    "refuse_unsupported_forms",
    "render_graph_sql",
    "verify_splice",
]


class RunSqlGraphLiteralRefused(RunSqlInvalid):
    """A string literal whose meaning depends on escape processing."""

    code = "run_sql_graph_literal_refused"


class RunSqlGraphStringModeRefused(RunSqlInvalid):
    """The connection does not use standard strings."""

    code = "run_sql_graph_string_mode_refused"


class RunSqlGraphFormUnsupported(RunSqlInvalid):
    """A graph relation named in a form the splice does not replace."""

    code = "run_sql_graph_form_unsupported"


class RunSqlGraphSpliceMismatch(RunSqlInvalid):
    """The executed text changed a caller token outside a replaced range."""

    code = "run_sql_graph_splice_mismatch"


class RunSqlGraphViewUnplanned(RunSqlInvalid):
    """A composite view body names a graph relation the plan did not find."""

    code = "run_sql_graph_view_unplanned"


#: Base graph views and the Chronos-registered relation that answers each.
_GRAPH_VIEW_RELATION = {
    "okg.v_nodes": "okg.graph_nodes",
    "okg.v_edges": "okg.graph_edges",
}
#: Relation keys whose presence routes a query to the Chronos checkout: the
#: same set the previous router (`_rewrite_graph_views` plus
#: `_references_physical_graph_view`) used.
_GRAPH_ROUTING_KEYS = frozenset(_GRAPH_VIEW_RELATION) | frozenset(
    _GRAPH_VIEW_RELATION.values()
)
#: Relation names of `_GRAPH_ROUTING_KEYS`, for the view-body cross-check.
_GRAPH_RELNAMES = frozenset(key.split(".", 1)[1] for key in _GRAPH_ROUTING_KEYS)
_BRANCH_POINT_PARAM = "_chronos_branch_point"
BOUNDED_ALIAS = "_okg_bounded"
_BOUNDED_PREFIX = "SELECT * FROM (\n"
_MAX_VIEW_EXPANSION_DEPTH = 8
# ---------------------------------------------------------------------------
# scanning helpers
#
# Tokens come from `okg.substrate.postgres_sql.lex`, written from PostgreSQL's
# scanner rules (the same lexer `checked_run_sql` cross-checks sqlglot
# against). A token is ``Token(kind, start, end, text, value)`` with character
# offsets and an exclusive end.


def _scan(text: str) -> list[Any]:
    from okg.substrate.postgres_sql import lex

    try:
        return lex(text)
    except Exception as exc:
        raise RunSqlInvalid(
            f"run_sql could not tokenize the graph-backed query: {exc}",
            data={"sql": text},
        ) from exc


def _is_comment(token: Any) -> bool:
    from okg.substrate.postgres_sql import COMMENT_KINDS

    return token.kind in COMMENT_KINDS


def _code_tokens(tokens: list[Any]) -> list[Any]:
    return [tok for tok in tokens if not _is_comment(tok)]


def _is_word(token: Any, word: str) -> bool:
    from okg.substrate.postgres_sql import WORD, canonical_identifier

    return token.kind == WORD and canonical_identifier(token.value, quoted=False) == word


def _is_name_token(token: Any) -> bool:
    """A token that can be one part of a qualified relation name."""
    from okg.substrate.postgres_sql import IDENTIFIER_KINDS

    return token.kind in IDENTIFIER_KINDS


def _statement_span(text: str, tokens: list[Any]) -> tuple[int, int]:
    """``(start, end)`` of the one statement in ``text``: its first and last
    code tokens, leading and trailing comments and semicolons excluded."""
    from okg.substrate.postgres_sql import SEMICOLON, statement_count

    if statement_count(tokens) != 1:
        raise RunSqlInvalid(
            "run_sql graph-backed SQL must be exactly one statement",
            data={"sql": text},
        )
    code = [tok for tok in _code_tokens(tokens) if tok.kind != SEMICOLON]
    return code[0].start, code[-1].end


def _parse_one(text: str, *, strict: bool) -> Any:
    """Parse ``text`` (one statement, no surrounding comments) with sqlglot,
    cross-checked by the PostgreSQL-rule lexer. The tree's identifier offsets
    must refer to ``text`` itself, so a text the front end would rewrite
    (a ``U&`` spelling) is refused here; callers normalize first.

    A backslash in a plain ``'...'`` string is read as an ordinary
    character (``standard_strings``): the graph path runs the text with
    ``standard_conforming_strings`` on (issue #3666)."""
    from okg.substrate.postgres_sql import parse

    try:
        parsed = parse(text, strict=strict, standard_strings=True)
    except Exception as exc:
        raise RunSqlInvalid(
            f"run_sql could not parse the graph-backed query: {exc}",
            data={"sql": text},
        ) from exc
    if parsed.text != text or len(parsed.statements) != 1:
        raise RunSqlInvalid(
            "run_sql graph-backed SQL must be exactly one statement in "
            "normalized form",
            data={"sql": text},
        )
    return parsed.statements[0]


# ---------------------------------------------------------------------------
# plan: find every relation reference in the caller's text and in view bodies


@dataclass
class _Ref:
    start: int  # offset in the fragment text, inclusive
    end: int  # offset in the fragment text, exclusive
    key: str  # lower-cased "schema.relation"
    implicit_alias: str | None  # relation name when the caller gave no alias
    name_parts: int = 1  # dotted name parts in the reference (1 to 3)
    #: A form the splice cannot replace: "ONLY", "*" (inheritance marker
    #: after the name) or "TABLE" (the `TABLE name` statement).
    form: str | None = None
    view: "_Fragment | None" = None


@dataclass
class _Fragment:
    text: str  # the statement text (no trailing semicolon)
    refs: list[_Ref] = field(default_factory=list)

    def keys(self) -> set[str]:
        out: set[str] = set()
        for ref in self.refs:
            out.add(ref.key)
            if ref.view is not None:
                out |= ref.view.keys()
        return out


@dataclass
class GraphSqlPlan:
    """The caller's checked text and every relation reference found in it."""

    checked_sql: str
    span_start: int
    span_end: int
    root: _Fragment

    @property
    def routes_to_graph(self) -> bool:
        return bool(self.root.keys() & _GRAPH_ROUTING_KEYS)


def _reference_end(
    tokens: list[Any], location: int, parts: list[str], text: str
) -> int:
    """Return the exclusive end of the qualified name that starts at `location`."""
    index = next((i for i, tok in enumerate(tokens) if tok.start == location), None)
    if index is None:
        raise RunSqlInvalid(
            "run_sql could not locate a relation reference in the graph-backed query",
            data={"sql": text, "location": location},
        )
    names = 0
    end = location
    expect_name = True
    i = index
    while i < len(tokens):
        tok = tokens[i]
        if _is_comment(tok):
            i += 1
            continue
        if expect_name and _is_name_token(tok):
            names += 1
            end = tok.end
            expect_name = False
            i += 1
            continue
        if not expect_name and tok.text == "." and names < len(parts):
            expect_name = True
            i += 1
            continue
        break
    if names != len(parts) or expect_name:
        raise RunSqlInvalid(
            "run_sql could not read a relation name in the graph-backed query",
            data={"sql": text, "location": location, "name_parts": parts},
        )
    return end


def _unsupported_form(
    only: bool, tokens: list[Any], start: int, end: int
) -> str | None:
    """Name a reference form whose text the splice would leave broken.

    The splice replaces only the qualified name. ``ONLY name``, ``name *``
    and ``TABLE name`` put a keyword or marker next to it that is not valid
    in front of or after a parenthesized subquery. ``ONLY`` is read from the
    parse tree and from the tokens (either one suffices); sqlglot drops the
    ``*`` marker from its tree, so that one is read from the tokens only.
    """
    before = [t for t in tokens if t.end <= start and not _is_comment(t)]
    after = [t for t in tokens if t.start >= end and not _is_comment(t)]
    if only or (before and _is_word(before[-1], "only")):
        return "ONLY"
    if after and after[0].text == "*":
        return "*"
    if before and _is_word(before[-1], "table"):
        return "TABLE"
    return None


def _replaced_refs(fragment: _Fragment) -> Iterable[_Ref]:
    """References the splice replaces: graph relations and views over them."""
    for ref in fragment.refs:
        if ref.key in _GRAPH_ROUTING_KEYS:
            yield ref
        elif ref.view is not None and ref.view.keys() & _GRAPH_ROUTING_KEYS:
            yield ref
            yield from _replaced_refs(ref.view)


def _relation_refs(stmt: Any) -> Iterable[tuple[int, list[str], str, bool, bool]]:
    """``(location, parts, relname, has_alias, only)`` for every named
    relation in a sqlglot tree. ``location`` is the offset where the first
    name part starts; ``parts`` are the canonical names as PostgreSQL resolves
    them (unquoted folded, quoted exact)."""
    from sqlglot import exp

    from okg.substrate.postgres_sql import identifier_name, iter_nodes

    for node in iter_nodes(stmt):
        if not isinstance(node, exp.Table) or not isinstance(node.this, exp.Identifier):
            continue
        idents = [
            node.args.get(key)
            for key in ("catalog", "db", "this")
            if node.args.get(key) is not None
        ]
        starts = []
        for ident in idents:
            start = (getattr(ident, "meta", None) or {}).get("start")
            if not isinstance(ident, exp.Identifier) or not isinstance(start, int):
                raise RunSqlInvalid(
                    "run_sql could not locate a relation reference in the "
                    "graph-backed query",
                    data={"relation": node.sql(dialect="postgres")},
                )
            starts.append(start)
        yield (
            min(starts),
            [identifier_name(ident) for ident in idents],
            identifier_name(node.this),
            node.args.get("alias") is not None,
            bool(node.args.get("only")),
        )


def _table_statement_ref(
    tokens: list[Any], text: str
) -> tuple[int, list[str], str, bool, bool] | None:
    """The relation of a ``TABLE name`` statement, read from the tokens.

    sqlglot cannot parse ``TABLE name``; `checked_run_sql` refuses it before
    the graph path, and ``pg_get_viewdef`` never writes it. It is still
    planned here, so the form refusal names it rather than a parse error.
    Anything after the name refuses.
    """
    from okg.substrate.postgres_sql import canonical_identifier, QIDENT, UIDENT

    code = _code_tokens(tokens)
    if not code or not _is_word(code[0], "table"):
        return None
    names: list[Any] = []
    i = 1
    while i < len(code) and _is_name_token(code[i]):
        names.append(code[i])
        i += 1
        if i < len(code) and code[i].text == "." and len(names) < 3:
            i += 1
            continue
        break
    if not names or i != len(code):
        raise RunSqlInvalid(
            "run_sql could not read the relation of a TABLE statement",
            data={"sql": text},
        )
    parts = [
        canonical_identifier(tok.value, quoted=tok.kind in {QIDENT, UIDENT})
        for tok in names
    ]
    return names[0].start, parts, parts[-1], False, False


def _refuse_unplanned_graph_references(
    fragment: _Fragment, tokens: list[Any], view: str
) -> None:
    """Refuse a view body that may read a relation the plan did not find.

    The plan finds relation references through sqlglot's tree. If sqlglot
    mis-read a view body and left a reference out, the splice would leave
    that name in place and the query would read the live view instead of the
    snapshot, or not be routed to the snapshot at all. This check reads the
    body again with the PostgreSQL-rule lexer, independently of sqlglot, and
    refuses when a code token outside every planned reference

    * names a graph relation (``v_nodes``, ``v_edges``, ``graph_nodes``,
      ``graph_edges``) where a relation can stand: not a function name
      (followed by ``(``) and not a column qualifier (followed by ``.``); or
    * is the schema ``okg`` of a qualified name that is not a function call
      and not a column reference (``okg.x.y``): a relation the plan missed.

    A planned reference written without a schema (``FROM v_nodes``) that
    names a graph relation is refused too: the plan keys it without a
    schema, so it would neither route nor be spliced. The check can refuse
    a harmless body (an alias or column spelled like a graph relation); it
    never lets one through. It runs on view bodies only; the caller's own
    text is judged by `checked_run_sql` before it is planned.
    """
    from okg.substrate.postgres_sql import QIDENT, UIDENT, canonical_identifier

    def refuse(name: str, offset: int) -> RunSqlGraphViewUnplanned:
        return RunSqlGraphViewUnplanned(
            f"run_sql refuses composite view {view}: its body names {name!r} "
            "at a place the graph-path plan did not read as a relation, so "
            "the query could read the live graph instead of the snapshot",
            data={"view": view, "name": name, "offset": offset},
            fix_hint="query the graph views directly, and report this view",
        )

    for ref in fragment.refs:
        if ref.name_parts == 1 and ref.key.split(".", 1)[1] in _GRAPH_RELNAMES:
            raise refuse(ref.key.split(".", 1)[1], ref.start)
    covered = [(ref.start, ref.end) for ref in fragment.refs]
    code = _code_tokens(tokens)
    for index, tok in enumerate(code):
        if not _is_name_token(tok):
            continue
        if any(lo <= tok.start and tok.end <= hi for lo, hi in covered):
            continue
        name = canonical_identifier(
            tok.value, quoted=tok.kind in {QIDENT, UIDENT}
        ).lower()
        following = code[index + 1] if index + 1 < len(code) else None
        if following is not None and following.text in {"(", "."}:
            if name != "okg" or following.text != ".":
                continue
            # okg.<name>: a relation unless a call or a column (okg.x.y).
            third = code[index + 2] if index + 2 < len(code) else None
            fourth = code[index + 3] if index + 3 < len(code) else None
            if third is None or not _is_name_token(third):
                continue
            if fourth is not None and fourth.text in {"(", "."}:
                continue
            raise refuse(f"okg.{third.value}", tok.start)
        if name in _GRAPH_RELNAMES:
            raise refuse(name, tok.start)


def _fragment(
    text: str,
    *,
    viewdef: Callable[[str], str | None],
    depth: int,
    strict: bool = True,
    view: str | None = None,
) -> tuple[_Fragment, int, int]:
    tokens = _scan(text)
    start, end = _statement_span(text, tokens)
    fragment = _Fragment(text=text[start:end])
    tokens = _scan(fragment.text)
    table_ref = _table_statement_ref(tokens, fragment.text)
    if table_ref is not None:
        found = [table_ref]
    else:
        found = list(
            _relation_refs(_parse_one(fragment.text, strict=strict))
        )
    for location, parts, relname, has_alias, only in found:
        key = f"{parts[-2].lower() if len(parts) > 1 else ''}.{relname.lower()}"
        ref_end = _reference_end(tokens, location, parts, fragment.text)
        ref = _Ref(
            start=location,
            end=ref_end,
            key=key,
            implicit_alias=None if has_alias else relname,
            name_parts=len(parts),
            form=_unsupported_form(only, tokens, location, ref_end),
        )
        if key.startswith("okg.v_") and key not in _GRAPH_VIEW_RELATION:
            body = viewdef(key)
            if body is not None:
                if depth >= _MAX_VIEW_EXPANSION_DEPTH:
                    raise RunSqlInvalid(
                        "run_sql exceeded the deployment-view expansion depth "
                        f"({_MAX_VIEW_EXPANSION_DEPTH}); the view graph may be cyclic",
                        data={"view": key},
                    )
                # A view body is catalog text from pg_get_viewdef, not caller
                # text: parsed without the strict backslash-string refusal,
                # as run_sql's view expansion did before #2899.
                ref.view, _, _ = _fragment(
                    _normalize(body),
                    viewdef=viewdef,
                    depth=depth + 1,
                    strict=False,
                    view=key,
                )
        fragment.refs.append(ref)
    fragment.refs.sort(key=lambda r: r.start)
    for left, right in zip(fragment.refs, fragment.refs[1:]):
        if right.start < left.end:
            raise RunSqlInvalid(
                "run_sql found overlapping relation references",
                data={"sql": fragment.text},
            )
    if view is not None:
        _refuse_unplanned_graph_references(fragment, tokens, view)
    return fragment, start, end


def _normalize(text: str) -> str:
    """``text`` with ``U&`` spellings decoded, as `checked_run_sql` returns it."""
    from okg.substrate.postgres_sql import normalize

    try:
        return normalize(text)
    except Exception as exc:
        raise RunSqlInvalid(
            f"run_sql could not tokenize the graph-backed query: {exc}",
            data={"sql": text},
        ) from exc


def _viewdef_reader(conn: Any) -> Callable[[str], str | None]:
    cache: dict[str, str | None] = {}

    def read(key: str) -> str | None:
        if key in cache:
            return cache[key]
        relname = key.split(".", 1)[1]
        with conn.cursor() as cur:
            cur.execute(
                "SELECT pg_get_viewdef(c.oid, true) AS def "
                "FROM pg_class c "
                "JOIN pg_namespace n ON n.oid = c.relnamespace "
                "WHERE n.nspname = 'okg' AND c.relname = %s "
                "AND c.relkind IN ('v', 'm')",
                (relname,),
            )
            row = cur.fetchone()
        definition: str | None = None
        if row is not None:
            definition = row["def"] if isinstance(row, dict) else row[0]
        cache[key] = definition
        return definition

    return read


def plan_graph_sql(sql: str, conn: Any) -> GraphSqlPlan:
    """Parse the checked text once and find every relation reference.

    Composite ``okg.v_*`` views are read with ``pg_get_viewdef`` and planned
    the same way, up to a fixed nesting depth.

    ``sql`` is normally `checked_run_sql`'s output, which already has ``U&``
    spellings decoded; any left are decoded here the same way, so the plan's
    ``checked_sql`` is exactly the text the checker judged. The caller's own
    ``U&'...'`` strings are refused on the graph path by
    `refuse_escape_dependent_literals` on the caller's raw text.
    """
    text = _normalize(sql)
    root, start, end = _fragment(text, viewdef=_viewdef_reader(conn), depth=0)
    return GraphSqlPlan(checked_sql=text, span_start=start, span_end=end, root=root)


# ---------------------------------------------------------------------------
# literal, form and string-mode refusals


def _form_refusal(ref: _Ref) -> RunSqlGraphFormUnsupported:
    form = {
        "ONLY": "ONLY <relation>",
        "*": "<relation> *",
        "TABLE": "TABLE <relation>",
    }[str(ref.form)]
    return RunSqlGraphFormUnsupported(
        f"run_sql graph-backed SQL does not support {form} for {ref.key}",
        data={"form": ref.form, "relation": ref.key},
        fix_hint=f"write SELECT ... FROM {ref.key} instead",
    )


def refuse_unsupported_forms(plan: GraphSqlPlan) -> None:
    """Refuse ``ONLY``, ``name *`` and ``TABLE name`` on a replaced relation.

    These pass the checker, but the splice replaces only the qualified name,
    so the executed text would be a syntax error naming internal tables.
    """
    for ref in _replaced_refs(plan.root):
        if ref.form is not None:
            raise _form_refusal(ref)


def refuse_escape_dependent_literals(sql: str) -> None:
    """Refuse ``E'...'`` and ``U&'...'`` strings.

    Their meaning depends on escape processing, which the splice and its
    checks do not model. Dollar-quoted strings are allowed: PostgreSQL never
    processes escapes in them, whatever ``standard_conforming_strings`` says.
    A backslash in a plain ``'...'`` (or ``N'...'``) string is allowed too
    (issue #3666): run_sql runs the text with ``standard_conforming_strings =
    on``, where it is an ordinary character, as every lexer here reads it.
    """
    from okg.substrate.postgres_sql import ESTRING, USTRING

    for tok in _scan(sql):
        if tok.kind == USTRING:
            form = "U&'...' string"
        elif tok.kind == ESTRING:
            form = "E'...' string"
        else:
            continue
        raise RunSqlGraphLiteralRefused(
            f"run_sql refuses a {form} in graph-backed SQL",
            data={"form": form, "offset": tok.start},
            fix_hint=(
                "write the value as a plain '...' string (a backslash there is "
                "an ordinary character, so '\\d+' is the regex \\d+) or as a "
                "dollar-quoted string such as $$\\d+$$"
            ),
        )


#: The characters after a backslash that start an escape PostgreSQL defines
#: in ``E'...'``; any other is dropped by PostgreSQL (``\\d`` is ``d``).
_KNOWN_ESCAPES = frozenset("\\'bfnrt01234567xuU")
_HEX_DIGITS = frozenset("0123456789abcdefABCDEF")


def dollar_quoted_rewrite(sql: str) -> str | None:
    """``sql`` with each ``E'...'`` string written as ``$$...$$``, or None.

    The retry a refused ``E'...'`` string gets (issue #3666). Offered only
    when it is exact: every backslash in each such string starts an escape
    PostgreSQL defines (a doubled backslash, a quote, ``b f n r t``, an octal,
    ``x`` hex or ``u``/``U`` code point), so the value is what the caller
    meant (``E'\\\\d+'`` is ``\\d+``, ``E'\\n'`` a newline); an unknown escape
    such as ``E'\\d'``, which PostgreSQL reads as the letter d, gets none. No
    value may hold ``$$`` or end in ``$``, no ``U&'...'`` string may be
    present, and the rewritten text must lex to the same tokens with only
    those strings changed into dollar-quoted ones of equal value.
    """
    from okg.substrate.postgres_sql import DOLLAR, ESTRING, USTRING, lex

    try:
        tokens = lex(sql)
    except Exception:  # noqa: BLE001 - no rewrite for text that does not lex
        return None
    pieces: list[str] = []
    cursor = 0
    changed = False
    for tok in tokens:
        if tok.kind == USTRING:
            return None
        if tok.kind != ESTRING:
            continue
        body = tok.text[2:-1]
        i = 0
        while i < len(body):
            if body[i] == "\\":
                nxt = body[i + 1 : i + 2]
                if nxt not in _KNOWN_ESCAPES:
                    return None
                width = {"x": 1, "u": 4, "U": 8}.get(nxt, 0)
                digits = body[i + 2 : i + 2 + width]
                if len(digits) < width or any(c not in _HEX_DIGITS for c in digits):
                    return None
                i += 2
                continue
            i += 1
        value = tok.value
        if "$$" in value or value.endswith("$"):
            return None
        pieces.append(sql[cursor : tok.start])
        pieces.append("$$" + value + "$$")
        cursor = tok.end
        changed = True
    if not changed:
        return None
    pieces.append(sql[cursor:])
    rewritten = "".join(pieces)
    try:
        after = lex(rewritten)
    except Exception:  # noqa: BLE001 - a rewrite that does not lex is not offered
        return None
    if len(after) != len(tokens):
        return None
    for old, new in zip(tokens, after):
        if old.kind == ESTRING:
            if new.kind != DOLLAR or new.value != old.value:
                return None
        elif (old.kind, old.value) != (new.kind, new.value):
            return None
    return rewritten


def assert_standard_conforming_strings(conn: Any) -> None:
    """Refuse unless the connection reads ``'...'`` without escape processing."""
    with conn.cursor() as cur:
        cur.execute("SHOW standard_conforming_strings")
        row = cur.fetchone()
    value = None
    if row is not None:
        value = row["standard_conforming_strings"] if isinstance(row, dict) else row[0]
    if str(value).lower() != "on":
        raise RunSqlGraphStringModeRefused(
            "run_sql graph-backed SQL requires standard_conforming_strings = on",
            data={"standard_conforming_strings": value},
            fix_hint="run SET standard_conforming_strings = on on this connection",
        )


# ---------------------------------------------------------------------------
# render


@dataclass(frozen=True)
class SplicedGraphSql:
    """The executed text and where it differs from the checked text."""

    sql: str
    #: ``(checked_start, checked_end, executed_start, executed_end)`` for each
    #: replaced reference in the caller's statement, in order. Checked offsets
    #: are relative to the statement span.
    replaced: tuple[tuple[int, int, int, int], ...]
    prefix_end: int
    suffix_start: int
    #: Dotted name parts of each replaced reference, from the parse tree, in
    #: the order of `replaced`. `verify_splice` checks each checked range
    #: holds exactly that qualified name.
    replaced_name_parts: tuple[int, ...] = ()


def _quote_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _bind_branch_point(subquery: str, branch_point: int) -> str:
    """Write the branch point into a checkout subquery as an integer literal."""
    tokens = _scan(subquery)
    out: list[str] = []
    cursor = 0
    i = 0
    while i < len(tokens):
        tok = tokens[i]
        if tok.text == ":":
            nxt = tokens[i + 1] if i + 1 < len(tokens) else None
            if nxt is None or nxt.start != tok.end or nxt.text != _BRANCH_POINT_PARAM:
                raise RunSqlInvalid(
                    "the Chronos checkout subquery names an unknown parameter",
                    data={"subquery": subquery},
                )
            out.append(subquery[cursor : tok.start])
            out.append(str(branch_point))
            cursor = nxt.end
            i += 2
            continue
        i += 1
    out.append(subquery[cursor:])
    return "".join(out)


def _alias_suffix(ref: _Ref) -> str:
    if ref.implicit_alias is None:
        return ""
    return f" AS {_quote_ident(ref.implicit_alias)}"


def _replacement(ref: _Ref, relations: dict[str, str]) -> str | None:
    relation = _GRAPH_VIEW_RELATION.get(ref.key, ref.key)
    if relation in relations:
        if ref.form is not None:
            raise _form_refusal(ref)
        return f"({relations[relation]})" + _alias_suffix(ref)
    if ref.key in _GRAPH_ROUTING_KEYS:
        raise RunSqlInvalid(
            "the Chronos checkout does not expose the graph relation this query reads",
            data={"relation": relation, "available": sorted(relations)},
        )
    if ref.view is not None and ref.view.keys() & (
        set(relations) | set(_GRAPH_VIEW_RELATION)
    ):
        if ref.form is not None:
            raise _form_refusal(ref)
        body = _render_fragment(ref.view, relations, record=None, base=0)
        return f"({body}\n)" + _alias_suffix(ref)
    return None


def _render_fragment(
    fragment: _Fragment,
    relations: dict[str, str],
    *,
    record: list[tuple[int, int, int, int]] | None,
    base: int,
    record_parts: list[int] | None = None,
) -> str:
    out: list[str] = []
    cursor = 0
    length = base
    for ref in fragment.refs:
        replacement = _replacement(ref, relations)
        if replacement is None:
            continue
        piece = fragment.text[cursor : ref.start]
        out.append(piece)
        length += len(piece)
        out.append(replacement)
        if record is not None:
            record.append((ref.start, ref.end, length, length + len(replacement)))
        if record_parts is not None:
            record_parts.append(ref.name_parts)
        length += len(replacement)
        cursor = ref.end
    out.append(fragment.text[cursor:])
    return "".join(out)


def render_graph_sql(
    plan: GraphSqlPlan,
    relations: dict[str, str],
    params: dict[str, Any],
    *,
    row_cap: int,
) -> SplicedGraphSql:
    """Splice checkout subqueries and view bodies into the caller's text."""
    branch_point = params.get(_BRANCH_POINT_PARAM)
    if type(branch_point) is not int or set(params) != {_BRANCH_POINT_PARAM}:
        raise RunSqlInvalid(
            "the Chronos checkout returned read parameters run_sql cannot bind",
            data={"params": sorted(params)},
        )
    bound = {
        name: _bind_branch_point(sub, branch_point) for name, sub in relations.items()
    }
    replaced: list[tuple[int, int, int, int]] = []
    name_parts: list[int] = []
    body = _render_fragment(
        plan.root,
        bound,
        record=replaced,
        base=len(_BOUNDED_PREFIX),
        record_parts=name_parts,
    )
    # The newline ends any trailing `--` comment in the caller's statement.
    suffix = f"\n) AS {BOUNDED_ALIAS} LIMIT {int(row_cap) + 1}"
    return SplicedGraphSql(
        sql=_BOUNDED_PREFIX + body + suffix,
        replaced=tuple(replaced),
        prefix_end=len(_BOUNDED_PREFIX),
        suffix_start=len(_BOUNDED_PREFIX) + len(body),
        replaced_name_parts=tuple(name_parts),
    )


# ---------------------------------------------------------------------------
# safety net


def verify_splice(plan: GraphSqlPlan, spliced: SplicedGraphSql) -> None:
    """Refuse unless every caller token outside a replaced range is unchanged.

    Both texts are tokenized with the lexer written from PostgreSQL's scanner
    rules. Every checked token inside the statement and outside a replaced
    reference must appear in the executed text with the same kind and text,
    at the position the splice predicts. Every executed token outside the
    inserted material must be one of those caller tokens. Text outside the
    statement may hold only comments and semicolons, which are not executed.

    Each replaced range of the checked text must hold exactly one qualified
    relation name and nothing else: it starts and ends on a name token, and
    in between holds only names, dots and comments, alternating name and
    dot, with as many name parts as the parse tree gave the reference. (The
    lexer refuses ``UESCAPE`` outright, so no escape clause can follow a
    name.) A range that also swallowed an alias, a keyword or punctuation is
    refused here, not left to PostgreSQL.
    """

    def mismatch(reason: str, **data: Any) -> RunSqlGraphSpliceMismatch:
        return RunSqlGraphSpliceMismatch(
            "run_sql refused a graph-backed query whose executed text differs "
            f"from the checked text: {reason}",
            data={"reason": reason, **data},
            fix_hint="simplify the query and report it; this is a run_sql defect",
        )

    expected: dict[int, tuple[int, int, str, str]] = {}
    in_range: list[list[Any]] = [[] for _ in spliced.replaced]
    for tok in _scan(plan.checked_sql):
        if tok.start >= plan.span_end or tok.end <= plan.span_start:
            if _is_comment(tok) or tok.text == ";":
                continue
            raise mismatch("token outside the statement", offset=tok.start)
        if tok.start < plan.span_start or tok.end > plan.span_end:
            raise mismatch("token crosses the statement boundary", offset=tok.start)
        rel_start = tok.start - plan.span_start
        rel_end = tok.end - plan.span_start
        shift = spliced.prefix_end
        inside = False
        for index, (c_start, c_end, _e_start, e_end) in enumerate(spliced.replaced):
            if rel_end <= c_start:
                break
            if rel_start >= c_end:
                shift = e_end - c_end
                continue
            if rel_start >= c_start and rel_end <= c_end:
                inside = True
                in_range[index].append(tok)
                break
            raise mismatch("token crosses a replaced reference", offset=tok.start)
        if inside:
            continue
        expected[rel_start + shift] = (
            rel_start + shift, rel_end + shift, tok.kind, tok.text
        )

    if len(spliced.replaced_name_parts) != len(spliced.replaced):
        raise mismatch("replaced references and name counts disagree")
    for (c_start, c_end, _e_start, _e_end), parts, range_tokens in zip(
        spliced.replaced, spliced.replaced_name_parts, in_range
    ):
        reason = _replaced_range_defect(
            range_tokens,
            start=c_start + plan.span_start,
            end=c_end + plan.span_start,
            name_parts=parts,
        )
        if reason is not None:
            raise mismatch(reason, offset=c_start + plan.span_start)

    inserted = [(0, spliced.prefix_end), (spliced.suffix_start, len(spliced.sql))]
    inserted.extend((e_start, e_end) for _, _, e_start, e_end in spliced.replaced)
    matched = 0
    for tok in _scan(spliced.sql):
        region = next(
            ((lo, hi) for lo, hi in inserted if tok.start < hi and tok.end > lo),
            None,
        )
        if region is not None:
            if tok.start < region[0] or tok.end > region[1]:
                raise mismatch("token crosses inserted text", offset=tok.start)
            continue
        if expected.get(tok.start) != (tok.start, tok.end, tok.kind, tok.text):
            raise mismatch("caller token changed or moved", offset=tok.start)
        matched += 1
    if matched != len(expected):
        raise mismatch(
            "caller token missing from the executed text",
            expected=len(expected),
            matched=matched,
        )


def _replaced_range_defect(
    tokens: list[Any], *, start: int, end: int, name_parts: int
) -> str | None:
    """Say why a replaced checked range is not exactly one qualified name."""
    if not tokens or tokens[0].start != start or tokens[-1].end != end:
        return "replaced range does not start and end on its name"
    names = 0
    expect_name = True
    i = 0
    while i < len(tokens):
        tok = tokens[i]
        if _is_comment(tok):
            i += 1
            continue
        if expect_name and _is_name_token(tok):
            names += 1
            expect_name = False
            i += 1
            continue
        if not expect_name and tok.text == ".":
            expect_name = True
            i += 1
            continue
        return "replaced range holds more than the relation name"
    if expect_name or names != name_parts:
        return "replaced range holds more than the relation name"
    if _is_comment(tokens[-1]):
        return "replaced range does not start and end on its name"
    return None


# ---------------------------------------------------------------------------
# execution


def execute_spliced_sql(conn: Any, sql: str) -> list[dict[str, Any]]:
    """Run the spliced text as-is and return rows as dictionaries.

    No parameters are passed, so psycopg sends the text unchanged (it scans a
    query for ``%`` placeholders only when parameters are given), and the
    Chronos adapter's ``:name``/``%`` rewriting is not on this path.

    Test hook: tests that replace the Chronos checkout with a fake patch this
    module attribute to capture the executed text. `run_sql` looks it up on
    the module at call time for that reason.
    """
    from psycopg.rows import dict_row

    with conn.cursor(row_factory=dict_row) as cur:
        # Pipeline mode sends the text with the extended protocol, where
        # PostgreSQL itself refuses more than one command -- the same guard
        # run_sql's operational path uses (`_execute_one_statement`). The
        # statement count and the splice check stay too. Results stay on
        # ``cur`` after the pipeline block syncs.
        with conn.pipeline():
            cur.execute(sql)
        return [dict(row) for row in cur.fetchall()]
