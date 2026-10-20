"""SQL-expressible expression DSL shared by the interpreter and SQL lowering.

The relational core (WHERE / JOIN / SELECT / ORDER / GROUP) needs predicates and
scalar expressions that BOTH the reference interpreter and the SQL backend can
evaluate. An expression here is a JSON-native tagged dict, so it serializes inside
``op.args`` and participates in the program checksum. The SAME expression is
evaluated two ways -- ``eval_expr`` (Python, over a row dict) and ``render_sql``
(parameterized SQL) -- so the interpreter and the lowered SQL agree by
construction (task.gtir.normalize-and-lower). Only the relational core uses
expressions; ``CALL`` stays opaque.

Known interpreter/SQL divergences (the SQL backend is authoritative): SQL
three-valued NULL logic is approximated (``eq``/``ne`` compare Python-side, so
``NULL = NULL`` keeps a row the SQL would drop; ``not``/``like`` DO propagate
NULL); ``lower``/``upper`` use Python's casemap, not the database collation;
``json_get`` renders non-string members via ``json.dumps`` (float and key-order
formatting can differ from PG's ``->>``); ``regex_match`` evaluates with
Python ``re`` while the SQL side is POSIX — POSIX classes like
``[[:space:][:punct:]]`` silently mis-evaluate as literal character classes in
the interpreter; boolean casts accept only the full PG spellings, not PG's
unique-prefix forms (``'tr'::boolean``). Row-wise parity for these edges is a
per-consumer concern (the equivalence harness compares SQL-to-SQL, not
interpreter-to-SQL, for exactly this reason).

Column references may be alias-qualified (``"e.dst_id"``). In SQL the alias is
emitted verbatim; in the interpreter (bag of merged row dicts) the alias is
stripped to the bare field when the qualified key is absent.

Forms: ``col`` / ``lit`` / ``param`` / ``is_null``; the comparisons
``eq`` / ``ne`` / ``lt`` / ``le`` / ``gt`` / ``ge``; the booleans
``and_`` / ``or_`` / ``not_`` / ``in_``; the jsonb accessors ``json_get``
(``->>``), ``json_member`` (``->``), ``json_contains`` (``@>``),
``json_exists`` (``jsonb_exists``); ``like`` (SQL LIKE); and scalar ``func``
application (``coalesce`` / ``length`` / ``btrim`` / ``lower`` / ``upper`` /
``jsonb_array_length`` evaluate in the interpreter; other names are SQL-only).
Aggregates are NOT expressions -- they live in the GROUP op's ``agg`` spec
(they are not row-wise).
"""

from __future__ import annotations

import json as _json
from typing import Any, Mapping

# ---- builders (JSON-native tagged dicts) ---------------------------------- #


def col(name: str) -> dict:
    return {"col": name}


def lit(value: Any) -> dict:
    return {"lit": value}


def param(name: str) -> dict:
    return {"param": name}


def is_null(e: dict) -> dict:
    return {"is_null": e}


def eq(a: dict, b: dict) -> dict:
    return {"eq": [a, b]}


def ne(a: dict, b: dict) -> dict:
    return {"ne": [a, b]}


def lt(a: dict, b: dict) -> dict:
    return {"lt": [a, b]}


def le(a: dict, b: dict) -> dict:
    return {"le": [a, b]}


def gt(a: dict, b: dict) -> dict:
    return {"gt": [a, b]}


def ge(a: dict, b: dict) -> dict:
    return {"ge": [a, b]}


def and_(*es: dict) -> dict:
    return {"and": list(es)}


def or_(*es: dict) -> dict:
    return {"or": list(es)}


def not_(e: dict) -> dict:
    return {"not": e}


def in_(e: dict, values: list) -> dict:
    return {"in": [e, list(values)]}


def eq_any(e: dict, param_name: str) -> dict:
    """``e = ANY(%(param)s)`` — set membership against a LIST-valued query
    param (vs ``in_``'s inline literal list). The scoped-execution form: the
    engine binds the derived subject scope as one array parameter."""
    return {"eq_any": [e, param_name]}


def json_get(e: dict, key: str) -> dict:
    """``e ->> 'key'`` -- extract a jsonb member as text."""
    return {"json_get": [e, key]}


def json_contains(e: dict, obj: dict) -> dict:
    """``e @> obj::jsonb`` -- jsonb containment."""
    return {"json_contains": [e, obj]}


def json_exists(e: dict, key: str) -> dict:
    """``jsonb_exists(e, 'key')`` -- top-level key presence (the ``?`` operator,
    spelled as the function form to avoid the psycopg placeholder clash)."""
    return {"json_exists": [e, key]}


def json_member(e: dict, key: str) -> dict:
    """``e -> 'key'`` -- extract a jsonb member AS JSONB (vs ``json_get``'s text),
    for feeding jsonb functions like ``jsonb_array_length``."""
    return {"json_member": [e, key]}


def like(e: dict, pattern: str) -> dict:
    """``e LIKE 'pattern'`` (SQL semantics: ``%`` any run, ``_`` one char)."""
    return {"like": [e, pattern]}


def starts_with(e: dict, prefix: dict) -> dict:
    """``starts_with(e, prefix)`` — string prefix test where BOTH sides are
    expressions (columns allowed), unlike ``like`` whose pattern is a static
    string. NULL on either side yields NULL (SQL-strict, matters under NOT).
    The heading-path subsumption predicate (consistency-model spec §4.1/§5)
    is the motivating consumer: ``starts_with(col("a.scope"),
    concat(col("b.scope"), lit(" > ")))``."""
    return {"starts_with": [e, prefix]}


def regex_match(e: dict, pattern: str, *, case_insensitive: bool = False) -> dict:
    """``e ~ 'pattern'`` (or ``~*`` when case-insensitive) — POSIX regex match.
    Negation composes via ``not_`` (NULL propagates like SQL ``!~``)."""
    return {"regex": [e, pattern, bool(case_insensitive)]}


def case_(whens: list[tuple[dict, dict]], else_: dict | None = None) -> dict:
    """``CASE WHEN c1 THEN v1 … [ELSE e] END`` — first true branch wins."""
    return {"case": [[list(w) for w in whens], else_]}


def concat(*args: dict) -> dict:
    """``a || b || …`` — SQL string concatenation (NULL poisons the result)."""
    return {"concat": list(args)}


def jsonb_array_any(e: dict, pattern: str, *, mode: str = "like") -> dict:
    """``EXISTS (SELECT 1 FROM jsonb_array_elements_text(e) m(v) WHERE v <op> p)``
    — any element of a jsonb text array matches. ``mode`` is ``like`` /
    ``regex`` / ``iregex``. NULL input matches nothing; non-array input errors
    (PG semantics, mirrored by the interpreter)."""
    if mode not in ("like", "regex", "iregex"):
        raise ValueError(f"jsonb_array_any mode must be like/regex/iregex, got {mode!r}")
    return {"jsonb_array_any": [e, pattern, mode]}


def func(name: str, *args: dict) -> dict:
    """A SQL scalar function application, e.g. ``func('coalesce', a, lit('x'))``."""
    return {"func": [name, list(args)]}


def cast(e: dict, type_name: str) -> dict:
    """A SQL cast ``(<e>)::<type>`` -- e.g. ``cast(param('qvec'), 'vector')`` for the
    pgvector distance in semantic search. The interpreter treats it as identity (the
    SQL-only types like ``vector`` are not row-wise Python-evaluable)."""
    return {"cast": [e, type_name]}


# eq/ne carry SQL `=`/`<>`; the four ordinals follow SQL NULL semantics
# (NULL cmp anything -> false) in the interpreter.
_CMP = {"eq": "=", "ne": "<>", "lt": "<", "le": "<=", "gt": ">", "ge": ">="}
_ORD = {"lt": "<", "le": "<=", "gt": ">", "ge": ">="}


# ---- Python evaluation (reference interpreter) ---------------------------- #


def eval_expr(node: dict, row: Mapping[str, Any], params: Mapping[str, Any]) -> Any:
    (tag,) = node.keys()
    v = node[tag]
    if tag == "col":
        if v in row:
            return row[v]
        bare = v.split(".")[-1]
        return row.get(bare)
    if tag == "lit":
        return v
    if tag == "param":
        return params.get(v)
    if tag == "is_null":
        return eval_expr(v, row, params) is None
    if tag == "eq":
        return eval_expr(v[0], row, params) == eval_expr(v[1], row, params)
    if tag == "ne":
        return eval_expr(v[0], row, params) != eval_expr(v[1], row, params)
    if tag in _ORD:
        a = eval_expr(v[0], row, params)
        b = eval_expr(v[1], row, params)
        if a is None or b is None:
            return False  # SQL: NULL comparison is unknown -> excluded by WHERE
        try:
            return {
                "lt": a < b, "le": a <= b, "gt": a > b, "ge": a >= b,
            }[tag]
        except TypeError:
            return False
    if tag == "and":
        return all(eval_expr(e, row, params) for e in v)
    if tag == "or":
        return any(eval_expr(e, row, params) for e in v)
    if tag == "not":
        inner = eval_expr(v, row, params)
        return None if inner is None else not inner  # SQL: NOT NULL is NULL
    if tag == "in":
        return eval_expr(v[0], row, params) in set(v[1])
    if tag == "eq_any":
        values = params.get(v[1])
        return eval_expr(v[0], row, params) in set(values or ())
    if tag == "json_get":
        base = eval_expr(v[0], row, params)
        if not isinstance(base, Mapping) or v[1] not in base:
            return None
        member = base[v[1]]
        if member is None:
            return None
        return member if isinstance(member, str) else _json.dumps(member)
    if tag == "json_contains":
        base = eval_expr(v[0], row, params)
        return isinstance(base, Mapping) and all(
            base.get(k) == val for k, val in v[1].items()
        )
    if tag == "json_exists":
        base = eval_expr(v[0], row, params)
        return isinstance(base, Mapping) and v[1] in base
    if tag == "json_member":
        base = eval_expr(v[0], row, params)
        if not isinstance(base, Mapping):
            return None
        return base.get(v[1])
    if tag == "like":
        val = eval_expr(v[0], row, params)
        if val is None:
            return None  # SQL: NULL LIKE p is NULL (matters under NOT)
        return _like_match(str(val), v[1])
    if tag == "starts_with":
        val = eval_expr(v[0], row, params)
        prefix = eval_expr(v[1], row, params)
        if val is None or prefix is None:
            return None  # SQL starts_with is NULL-strict
        return str(val).startswith(str(prefix))
    if tag == "regex":
        import re as _re

        val = eval_expr(v[0], row, params)
        if val is None:
            return None  # SQL: NULL ~ p is NULL
        flags = _re.IGNORECASE if v[2] else 0
        return _re.search(v[1], str(val), flags=flags) is not None
    if tag == "case":
        whens, else_ = v
        for cond, out in whens:
            if eval_expr(cond, row, params):
                return eval_expr(out, row, params)
        return eval_expr(else_, row, params) if else_ is not None else None
    if tag == "concat":
        parts = [eval_expr(a, row, params) for a in v]
        if any(p is None for p in parts):
            return None  # SQL ||: NULL poisons
        return "".join(str(p) for p in parts)
    if tag == "jsonb_array_any":
        import re as _re

        base = eval_expr(v[0], row, params)
        if base is None:
            return False  # no rows -> EXISTS false
        if not isinstance(base, (list, tuple)):
            raise ValueError(
                "jsonb_array_any: input is not a JSON array "
                f"(got {type(base).__name__})"
            )
        pattern, mode = v[1], v[2]
        for item in base:
            text = _jsonb_element_text(item)
            if text is None:
                continue  # LIKE/~ against SQL NULL is never true
            if mode == "like" and _like_match(text, pattern):
                return True
            if mode == "regex" and _re.search(pattern, text):
                return True
            if mode == "iregex" and _re.search(pattern, text, _re.IGNORECASE):
                return True
        return False
    if tag == "func":
        name, args = v[0], v[1]
        vals = [eval_expr(a, row, params) for a in args]
        fn = name.lower()
        if fn == "coalesce":
            return next((x for x in vals if x is not None), None)
        if fn == "length":
            return None if vals[0] is None else len(str(vals[0]))
        if fn == "btrim":
            # PG btrim(text) strips SPACES only — not tabs/newlines/NBSP.
            # (Python str.strip() would diverge on those inputs.)
            chars = str(vals[1]) if len(vals) > 1 and vals[1] is not None else " "
            return None if vals[0] is None else str(vals[0]).strip(chars)
        if fn == "lower":
            # NOTE: Python casemap, not PG locale collation — divergent on
            # locale-sensitive codepoints (e.g. 'İ'); the SQL backend is
            # authoritative for non-ASCII case folding.
            return None if vals[0] is None else str(vals[0]).lower()
        if fn == "upper":
            return None if vals[0] is None else str(vals[0]).upper()
        if fn == "jsonb_array_length":
            if vals[0] is None:
                return None
            if not isinstance(vals[0], (list, tuple)):
                # PG raises for non-array input; a silent None here would be a
                # silent-fallback divergence. Fail loud like the SQL backend.
                raise ValueError(
                    "jsonb_array_length: input is not a JSON array "
                    f"(got {type(vals[0]).__name__})"
                )
            return len(vals[0])
        raise ValueError(f"unknown func {name!r}")
    if tag == "cast":
        inner = eval_expr(v[0], row, params)
        target = str(v[1]).lower()
        if inner is None:
            return None
        if target in ("boolean", "bool"):
            # PG boolean input truthiness (::boolean on text).
            text = str(inner).strip().lower()
            if text in ("t", "true", "1", "y", "yes", "on"):
                return True
            if text in ("f", "false", "0", "n", "no", "off"):
                return False
            raise ValueError(f"invalid input for boolean cast: {inner!r}")
        if target in ("int", "integer", "bigint", "smallint"):
            return int(str(inner).strip())
        if target in ("text", "varchar"):
            return str(inner)
        return inner  # SQL-only types (vector, jsonb, ...) stay identity
    raise ValueError(f"unknown expr tag {tag!r}")


def _jsonb_element_text(item):
    """``jsonb_array_elements_text`` element semantics: JSON null -> SQL NULL,
    strings unquoted, everything else its jsonb text (``true``, ``1``,
    ``{"a": 1}``)."""
    if item is None:
        return None
    if isinstance(item, str):
        return item
    return _json.dumps(item)


def _like_match(value: str, pattern: str) -> bool:
    """SQL LIKE with the default backslash escape: ``\\%``/``\\_`` are literals."""
    import re as _re

    parts: list[str] = []
    chars = iter(pattern)
    for ch in chars:
        if ch == "\\":
            nxt = next(chars, None)
            parts.append(_re.escape(nxt) if nxt is not None else _re.escape("\\"))
        elif ch == "%":
            parts.append(".*")
        elif ch == "_":
            parts.append(".")
        else:
            parts.append(_re.escape(ch))
    return _re.fullmatch("".join(parts), value, flags=_re.DOTALL) is not None


# ---- SQL rendering (lowering) --------------------------------------------- #


def _sql_string_literal(value: str) -> str:
    """A standard-conforming single-quoted SQL literal (quotes doubled)."""
    return "'" + value.replace("'", "''") + "'"


def _inline_json_key(key) -> str:
    """A json key rendered as an inline literal — RESTRICTED charset.

    Escaping alone is not sufficient: the Chronos session rewriter re-renders
    the query through sqlglot, which parses the arrow key into a JSON-path
    node and re-renders it WITHOUT re-escaping — a doubled quote comes back
    single, turning an escaped key into invalid (or injected) SQL one layer
    down. Keys are deployment-authored program data, so quotes/backslashes/
    NUL are rejected loudly at lowering time instead.
    """
    if not isinstance(key, str):
        raise ValueError(
            f"json key must be a string literal, got {type(key).__name__}"
        )
    if any(ch in key for ch in ("'", "\\", "\x00")):
        raise ValueError(
            "json key must not contain quotes, backslashes, or NUL "
            f"(sqlglot re-render un-escapes them): {key!r}"
        )
    return _sql_string_literal(key)


def render_sql(node: dict, params: dict) -> str:
    """Render to parameterized SQL; literals/params accumulate into ``params``."""
    (tag,) = node.keys()
    v = node[tag]
    if tag == "col":
        return v  # already an (optionally alias-qualified) SQL identifier
    if tag == "lit":
        key = f"p{len(params)}"
        params[key] = v
        return f"%({key})s"
    if tag == "param":
        return f"%({v})s"
    if tag == "is_null":
        return f"({render_sql(v, params)} IS NULL)"
    if tag in _CMP:
        return f"({render_sql(v[0], params)} {_CMP[tag]} {render_sql(v[1], params)})"
    if tag == "and":
        return "(" + " AND ".join(render_sql(e, params) for e in v) + ")"
    if tag == "or":
        return "(" + " OR ".join(render_sql(e, params) for e in v) + ")"
    if tag == "not":
        return f"(NOT {render_sql(v, params)})"
    if tag == "in":
        rendered = []
        for item in v[1]:
            key = f"p{len(params)}"
            params[key] = item
            rendered.append(f"%({key})s")
        return f"({render_sql(v[0], params)} IN ({', '.join(rendered)}))"
    if tag == "eq_any":
        return f"({render_sql(v[0], params)} = ANY(%({v[1]})s))"
    if tag == "json_get":
        # The key renders as an inline SQL literal, NOT a bound parameter: the
        # Chronos session executor round-trips queries through sqlglot, which
        # rewrites `x -> :param` into JSON_EXTRACT_PATH(x, ...) — undefined
        # for jsonb — while `x -> 'literal'` survives verbatim. Keys are
        # static program data (already inside the checksum).
        inner = render_sql(v[0], params)
        return f"({inner} ->> {_inline_json_key(v[1])})"
    if tag == "json_contains":
        inner = render_sql(v[0], params)
        key = f"p{len(params)}"
        params[key] = _json.dumps(v[1])
        return f"({inner} @> %({key})s::jsonb)"
    if tag == "json_exists":
        inner = render_sql(v[0], params)
        key = f"p{len(params)}"
        params[key] = v[1]
        return f"jsonb_exists({inner}, %({key})s)"
    if tag == "json_member":
        # Inline literal key — same sqlglot arrow-with-parameter hazard as
        # json_get above.
        inner = render_sql(v[0], params)
        return f"({inner} -> {_inline_json_key(v[1])})"
    if tag == "like":
        inner = render_sql(v[0], params)
        key = f"p{len(params)}"
        params[key] = v[1]
        return f"({inner} LIKE %({key})s)"
    if tag == "starts_with":
        # The PG function form (not LIKE): both sides are expressions, so a
        # pattern-escaping step would be wrong, and a plain function call
        # survives the sqlglot session-rewriter round-trip verbatim.
        return f"starts_with({render_sql(v[0], params)}, {render_sql(v[1], params)})"
    if tag == "regex":
        inner = render_sql(v[0], params)
        key = f"p{len(params)}"
        params[key] = v[1]
        op = "~*" if v[2] else "~"
        return f"({inner} {op} %({key})s)"
    if tag == "case":
        whens, else_ = v
        rendered = "CASE"
        for cond, out in whens:
            rendered += (
                f" WHEN {render_sql(cond, params)}"
                f" THEN {render_sql(out, params)}"
            )
        if else_ is not None:
            rendered += f" ELSE {render_sql(else_, params)}"
        return f"({rendered} END)"
    if tag == "concat":
        return "(" + " || ".join(render_sql(a, params) for a in v) + ")"
    if tag == "jsonb_array_any":
        inner = render_sql(v[0], params)
        key = f"p{len(params)}"
        params[key] = v[1]
        op = {"like": "LIKE", "regex": "~", "iregex": "~*"}[v[2]]
        return (
            "EXISTS (SELECT 1 FROM jsonb_array_elements_text("
            f"{inner}) __okg_arr(v) WHERE __okg_arr.v {op} %({key})s)"
        )
    if tag == "func":
        name, args = v[0], v[1]
        rendered = ", ".join(render_sql(a, params) for a in args)
        return f"{name}({rendered})"
    if tag == "cast":
        return f"({render_sql(v[0], params)})::{v[1]}"
    raise ValueError(f"unknown expr tag {tag!r}")


__all__ = [
    "col", "lit", "param", "is_null",
    "eq", "ne", "lt", "le", "gt", "ge",
    "and_", "or_", "not_", "in_", "eq_any",
    "json_get", "json_contains", "json_exists", "json_member", "like",
    "starts_with",
    "regex_match", "case_", "concat", "jsonb_array_any",
    "func", "cast",
    "eval_expr", "render_sql",
]
