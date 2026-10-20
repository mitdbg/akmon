"""The read-predicate compiler for MCP operators.

``compile_read_predicate`` is the one compiler from a normalized predicate
tree to SQL (design 3.2a, D16). Each read surface supplies a leaf lowerer:

  - ``string_field_lowerer`` -- search's ``where`` over catalog string
    columns (#3226).
  - ``effective_attr_lowerer`` -- ``filter``, ``map``, ``aggregate`` and
    ``expand`` over the effective-attribute relation (#3201).

Every caller value is a bound parameter; no function here renders a caller
value into SQL text. Also here: ``node_id_from_input`` and ``scope_kind``,
which coerce an MCP ``input`` to a node id or a ``nodes``/``edges`` kind.
"""
from __future__ import annotations

import datetime as _dt
import json as _json
import copy
import hashlib
import math
import re
from dataclasses import dataclass
from decimal import Decimal
from collections.abc import Callable, Mapping
from typing import Any


__all__ = [
    "EFFECTIVE_VIEW_FIELD_COLUMNS",
    "as_operator_timestamp",
    "effective_attr_lowerer",
    "effective_field_map_identity",
    "encode_nonfinite_operands",
    "holds_nul",
    "node_id_from_input",
    "scope_kind",
    "CompiledPredicate", "SqlLeaf", "compile_read_predicate", "string_field_lowerer",
    "RAW_TEXT_CONTAINS",
    "TEXT_CASEFOLD_EQ",
    "DATE_WINDOW_OP",
    "ascii_casefold",
    "is_text_contains",
    "text_contains",
    "text_contains_fields",
    "text_contains_notes",
    "text_operand_problem",
]

#: Internal superset leaf of search's query-less `attrs` lookup
#: (``search_where.lookup_sql_clauses``): true when the stored attribute is a
#: JSON string containing ``value``. The effective lowerer renders it (it was
#: the last op only ``server._effective_view_predicate_sql`` rendered). It
#: must equal ``search_where.RAW_TEXT_CONTAINS``, which cannot be imported
#: here (search_where imports this module); a test pins the two. The public
#: predicate op set (``operator_models.Predicate``) cannot name it.
RAW_TEXT_CONTAINS = "_raw_text_contains"

#: Internal leaf of the query-less lookup's value hint (issue #3666): true
#: when the stored attribute is a JSON string equal to ``value`` ignoring
#: case (SQL ``lower``). Only the hint's one-row read renders it; the public
#: predicate op set cannot name it.
TEXT_CASEFOLD_EQ = "_text_casefold_eq"


#: Internal leaf a ``date.<meaning>`` or ``date.attrs.<key>`` condition is
#: rewritten to before it is compiled (issue #3666,
#: ``date_conditions.rewrite_date_leaves``): the stored ``attrs.<key>`` read
#: as an instant and tested against windows of instants. Defined here, with
#: no import, so both the lowerer and ``date_conditions`` can name it. The
#: public predicate op set (``operator_models.Predicate``) cannot.
DATE_WINDOW_OP = "_date_window"


# -- `contains` with text: a substring of one attribute (issue #3666) --------
#
# `contains` with a string on `attrs.<key>` means "the stored text holds this
# text", ignoring the case of the ASCII letters A-Z. Both the SQL lowering and
# every Python row check fold with this one table (SQL `translate` with the
# same two strings), so they agree on every value whatever the database's
# collation or ctype; any other letter matches as written. The operand is
# found with `strpos`, so `%`, `_` and `\` are plain characters, not patterns.
_ASCII_UPPER = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
_ASCII_LOWER = "abcdefghijklmnopqrstuvwxyz"
_ASCII_FOLD = str.maketrans(_ASCII_UPPER, _ASCII_LOWER)


def ascii_casefold(text: str) -> str:
    """`text` with A-Z written as a-z and every other character kept."""
    return text.translate(_ASCII_FOLD)


def text_contains(stored: Any, needle: str) -> bool:
    """The row check of `contains` with text: `stored` is a string that holds
    `needle`, or a list with such a string element, ASCII letter case
    ignored. A number, true/false, null or an object never does (the reply
    says so: `text_contains_notes`)."""
    folded = ascii_casefold(needle)
    if isinstance(stored, str):
        return folded in ascii_casefold(stored)
    if isinstance(stored, list):
        return any(
            isinstance(item, str) and folded in ascii_casefold(item) for item in stored
        )
    return False


def text_contains_fields(predicate: Any) -> list[str]:
    """The `attrs.<key>` fields a predicate tests with text `contains`, in
    order, anywhere in the tree."""
    out: list[str] = []
    stack = [predicate]
    while stack:
        node = stack.pop()
        if isinstance(node, list):
            stack.extend(reversed(node))
            continue
        if not isinstance(node, Mapping):
            continue
        if is_text_contains(node) and node["field"] not in out:
            out.append(node["field"])
        stack.extend(child for key, child in node.items() if key in ("clause", "clauses"))
    return out


def text_contains_notes(
    predicate: Any, declared: Mapping[str, str] | None, *, empty: bool,
) -> list[str]:
    """What a text `contains` cannot match, for the reply (issue #3666).

    On an attribute the catalog declares a number or true/false, always: no
    stored value of it is text. On any other attribute, when the page has no
    rows: a value there may be a number, true/false or an object, which text
    never matches, and the empty page must not read as "no such text"."""
    notes: list[str] = []
    for field in text_contains_fields(predicate):
        kind = (declared or {}).get(field[len("attrs."):])
        if kind in ("num", "bool"):
            notes.append(
                f"{field} is declared {kind}; contains matches text or a list of "
                f"text, never {_DECLARED_TAKES[kind]}; compare it with eq, lt or gt"
            )
        elif empty:
            notes.append(
                f"contains on {field} matches text or a list of text; a number, "
                "true/false or an object stored there never matches"
            )
    return notes


def is_text_contains(node: Any) -> bool:
    """Whether a predicate node is `contains` with a string on one attribute."""
    if not isinstance(node, Mapping) or node.get("op") != "contains":
        return False
    field = node.get("field")
    return (
        isinstance(field, str) and field.startswith("attrs.")
        and len(field) > len("attrs.") and isinstance(node.get("value"), str)
    )


def text_operand_problem(value: str) -> str | None:
    """Why a `contains` text operand cannot be served, or None.

    Empty text is in every value; PostgreSQL text cannot hold NUL; a lone
    surrogate is not UTF-8. The same three `prefix` and `suffix` refuse."""
    if not value:
        return "a `contains` text value must not be empty"
    if "\x00" in value or any("\ud800" <= ch <= "\udfff" for ch in value):
        return "a `contains` text value cannot hold a NUL character or a lone surrogate"
    return None


READ_COMPILER_VERSION = "reader-predicate-v1"


def _canonical(value: Any) -> bytes:
    return _json.dumps(value, sort_keys=True, ensure_ascii=False,
                       separators=(",", ":"), allow_nan=False).encode("utf-8")


@dataclass(frozen=True)
class SqlLeaf:
    """Adapter proof: SQL TRUE admits exactly/superset reader TRUE rows.

    Negation additionally requires the full truth domain (including unknown)
    to agree. Acceptance exactness alone is insufficient for NOT.
    """
    sql: str
    exact: bool = True
    truth_exact: bool = True

    def __post_init__(self) -> None:
        if self.truth_exact and not self.exact:
            raise ValueError("truth-exact lowering must also be acceptance-exact")


@dataclass(frozen=True)
class CompiledPredicate:
    sql: str
    qualification: str
    truth_exact: bool
    compiler_version: str
    field_map_identity: str
    fingerprint: str
    _predicate: bytes
    _params: bytes

    @property
    def predicate(self) -> Any:
        return _json.loads(self._predicate)

    @property
    def residual(self) -> Any:
        return None if self.qualification == "exact" else self.predicate

    @property
    def params(self) -> dict[str, Any]:
        """A fresh copy of the parameters this compile bound, and only those.

        Names are allocated against the seed mapping given to this compile and
        nothing else. Seed names are never reused, so these never collide with
        a seed name. Two compiles seeded from the same mapping can allocate the
        same name (``w0`` twice), so merging both results with ``dict.update``
        would let the second silently overwrite the first. Merge with
        :meth:`bind_into`, which refuses any name already present.
        """
        return _json.loads(self._params)

    def bind_into(self, params: dict[str, Any]) -> str:
        """Add this compile's parameters to ``params`` and return its SQL.

        Refuses, leaving ``params`` unchanged, when any allocated name is
        already bound there: a second compile seeded from a stale snapshot of
        ``params`` fails loudly instead of overwriting the first one's values.
        """
        own = self.params
        clash = sorted(name for name in own if name in params)
        if clash:
            raise ValueError(
                "compiled predicate parameter names are already bound: "
                + ", ".join(clash)
                + "; seed each compile with the parameters bound so far"
            )
        params.update(own)
        return self.sql


def compile_read_predicate(
    predicate: Mapping[str, Any] | None, *,
    leaf_lowerer: Callable[[Mapping[str, Any], dict[str, Any], str], SqlLeaf | None],
    compiler_version: str = READ_COMPILER_VERSION,
    field_map_identity: str,
    prefix: str = "w",
    params: Mapping[str, Any] | None = None,
    identity_predicate: Any = ...,
) -> CompiledPredicate:
    """Compile a normalized predicate without weakening reader acceptance.

    ``identity_predicate`` (issue #3666), when given, is the predicate the
    fingerprint names instead of ``predicate``: the caller's own predicate
    when ``predicate`` is a rewrite of it that carries stored values (a date
    condition's ``also`` strings), which a fingerprint must never reflect.

    Unknown adapter leaves remain a TRUE prefilter plus full residual. All
    caller structures are copied; returned structures are fresh copies too.
    Existing parameter names may seed the allocator, but are never mutated.
    A predicate that is not canonical JSON (NaN, Infinity, a lone surrogate)
    raises ``ValueError``; callers facing users refuse it before this point.
    Merge the result with :meth:`CompiledPredicate.bind_into`.

    Callers: search's ``where`` (``search_where.compile_where``) and the
    ``filter``/``map``/``aggregate``/``expand`` operators through
    ``effective_attr_lowerer`` (issue #3201).
    """
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", prefix):
        raise ValueError("invalid SQL parameter prefix")
    try:
        full = _json.loads(_canonical(predicate))
    except (TypeError, ValueError) as exc:  # NaN/Infinity, lone surrogates, non-JSON
        raise ValueError(
            "predicate operands must be JSON values: finite numbers and valid Unicode text"
        ) from exc
    seed = dict(params or {})
    # The allocator sees the caller's names only. Seed values are never
    # serialized or returned, so non-JSON values (datetimes, tuples) are safe.
    bound: dict[str, Any] = {name: None for name in seed}

    def lower(node: Any) -> SqlLeaf:
        if node is None:
            return SqlLeaf("TRUE")
        if not isinstance(node, dict):
            raise ValueError("normalized predicate must be an object")
        op = node.get("op")
        if op in {"and", "or"}:
            clauses = node.get("clauses")
            if not isinstance(clauses, list) or not clauses:
                raise ValueError("logical predicate requires nonempty clauses")
            parts = [lower(clause) for clause in clauses]
            return SqlLeaf("(" + (" AND " if op == "and" else " OR ").join(
                part.sql for part in parts) + ")", all(p.exact for p in parts),
                all(p.truth_exact for p in parts))
        if op == "not":
            if "clause" not in node:
                raise ValueError("not requires a clause")
            before = copy.deepcopy(bound)
            child = lower(node["clause"])
            if child.truth_exact:
                return SqlLeaf(f"(NOT {child.sql})")
            bound.clear()
            bound.update(before)
            return SqlLeaf("TRUE", False, False)
        before = copy.deepcopy(bound)
        leaf = leaf_lowerer(node, bound, prefix)
        if leaf is None:
            bound.clear()
            bound.update(before)
            return SqlLeaf("TRUE", False, False)
        return leaf

    lowered = lower(full)
    named = full
    if identity_predicate is not ...:
        try:
            named = _json.loads(_canonical(identity_predicate))
        except (TypeError, ValueError) as exc:
            raise ValueError(
                "predicate operands must be JSON values: finite numbers and valid Unicode text"
            ) from exc
    identity = {"predicate": named, "compiler_version": compiler_version,
                "field_map_identity": field_map_identity}
    return CompiledPredicate(
        lowered.sql, "exact" if lowered.exact else "superset", lowered.truth_exact,
        compiler_version, field_map_identity, hashlib.sha256(_canonical(identity)).hexdigest(),
        _canonical(full), _canonical({k: v for k, v in bound.items() if k not in seed}),
    )


def string_field_lowerer(columns: Mapping[str, str]):
    """Trusted adapter mapping for known, two-valued nullable string fields.

    Mapping expressions come from source code, never caller field names. JSON,
    dates and maskable values require their own proven reader-value adapter.
    """
    fields = dict(columns)

    def bind(params: dict[str, Any], prefix: str, value: Any) -> str:
        index = len(params)
        while f"{prefix}{index}" in params:
            index += 1
        name = f"{prefix}{index}"
        params[name] = value
        return f"%({name})s"

    def text(value: Any) -> bool:
        # PostgreSQL text never holds NUL, so a NUL-bearing operand can never
        # equal or prefix a stored value; it is never sent as a parameter.
        return isinstance(value, str) and "\x00" not in value

    def lower(node: Mapping[str, Any], params: dict[str, Any], prefix: str) -> SqlLeaf | None:
        field, op = node.get("field"), node.get("op")
        if not isinstance(field, str) or field not in fields:
            return None
        column = fields[field]
        if op == "exists":
            return SqlLeaf(f"({column} IS NOT NULL)")
        if op == "eq":
            value = node.get("value")
            if value is None:
                return SqlLeaf(f"({column} IS NULL)")
            if not isinstance(value, str):
                # A column holds text: no lowering for another type, never a
                # constant FALSE (issue #3666, 2c; search's grammar refuses
                # the call, naming the fix, before it gets here).
                return None
            if not text(value):
                return SqlLeaf("FALSE")
            return SqlLeaf(f"COALESCE({column} = {bind(params, prefix, value)}, FALSE)")
        if op == "in":
            values = node.get("values")
            if not isinstance(values, list) or not values:
                raise ValueError("in requires a nonempty values list")
            if any(v is not None and not isinstance(v, str) for v in values):
                return None  # as eq: no lowering for a value that is not text
            texts = [v for v in values if text(v)]
            parts = []
            if texts:
                parts.append(f"COALESCE({column} = ANY({bind(params, prefix, texts)}), FALSE)")
            if any(v is None for v in values):
                parts.append(f"({column} IS NULL)")
            return SqlLeaf("(" + " OR ".join(parts) + ")" if parts else "FALSE")
        if op in {"prefix", "suffix"}:
            raw = node.get("value")
            if not isinstance(raw, str) or not raw:
                raise ValueError(f"{op} requires a nonempty string value")
            if not text(raw):
                return SqlLeaf("FALSE")
            value = bind(params, prefix, raw)
            side = "left" if op == "prefix" else "right"
            return SqlLeaf(f"COALESCE({side}({column}, char_length({value})) = {value}, FALSE)")
        if op == "contains":
            # Text inside the column (issue #3666, e.g. search's `source`):
            # as `text_contains` reads it, ASCII case ignored.
            raw = node.get("value")
            if not isinstance(raw, str) or not raw:
                return None
            if not text(raw):
                return SqlLeaf("FALSE")
            value = bind(params, prefix, ascii_casefold(raw))
            return SqlLeaf(
                f"COALESCE(strpos(translate({column}, '{_ASCII_UPPER}', "
                f"'{_ASCII_LOWER}'), {value}) > 0, FALSE)"
            )
        if op in {"lt", "gt", "between"}:
            values = node.get("values") if op == "between" else [node.get("value")]
            if op == "between" and (not isinstance(values, list) or len(values) != 2):
                raise ValueError("between requires values [low, high]")
            if not all(isinstance(v, str) for v in values):
                return None  # as eq: no lowering for a bound that is not text
            if not all(text(v) for v in values):
                return None  # NUL orders below every character: leave it residual.
            # The reader compares Python code points. COLLATE "C" makes the
            # database compare UTF-8 bytes, which order identically whatever
            # the column or database collation is; a locale order would let
            # the prefilter drop rows the reader accepts.
            if op == "between":
                low, high = (bind(params, prefix, v) for v in values)
                return SqlLeaf(
                    f'COALESCE({column} COLLATE "C" BETWEEN {low} AND {high}, FALSE)')
            value = bind(params, prefix, values[0])
            sign = "<" if op == "lt" else ">"
            return SqlLeaf(f'COALESCE({column} COLLATE "C" {sign} {value}, FALSE)')
        return None
    return lower


def node_id_from_input(input: Any) -> str:
    if isinstance(input, str):
        return input
    if isinstance(input, dict):
        node_id = input.get("node_id") or input.get("start_node")
        if isinstance(node_id, str) and node_id:
            return node_id
    raise ValueError(
        "operator input must be a node_id string or "
        "{node_id: <id>} for this v1 shape"
    )


def scope_kind(input: Any) -> str:
    if isinstance(input, dict):
        kind = input.get("kind")
        if kind in {"nodes", "edges"}:
            return str(kind)
    raise ValueError(
        "v1 filter/map only supports input scopes "
        "{kind: 'nodes'} or {kind: 'edges'}"
    )


#: The effective-attribute relation's stable columns, by operator field name
#: (scale-operator-attr-reads, Layer 2). ``node_id``/``edge_id`` are the
#: relation's ``target_id``; ``text_hash`` is its ``raw_text_hash``.
EFFECTIVE_VIEW_FIELD_COLUMNS: dict[str, dict[str, str]] = {
    "nodes": {
        "node_id": "target_id",
        "subtype": "subtype",
        "text_hash": "raw_text_hash",
    },
    "edges": {
        "edge_id": "target_id",
        "src": "src",
        "dst": "dst",
        "edge_type": "edge_type",
    },
}

#: Version of the effective-attribute leaf lowerer. It is part of every
#: operator predicate fingerprint, so changing leaf semantics changes them.
#: v2 (issue #3666, 2c): a text range on a column compares under COLLATE "C"
#: and an integer range on `edge_id` compares numbers.
EFFECTIVE_LOWERER_VERSION = "effective-attrs-v2"

_RANGE_SQL = {"lt": "<", "gt": ">"}


def as_operator_timestamp(value: Any) -> Any:
    """The instant an operator compares a ``ts``-declared value as, or None.

    The Python evaluator (``server._operator_predicate_matches``) and the SQL
    lowerer below read timestamps through this one function. A naive value is
    pinned to UTC: Postgres resolves a naive literal against the session
    TimeZone, and a naive/aware pair is not comparable in Python.
    """
    if not isinstance(value, str):
        return None
    text = value.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = _dt.datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=_dt.timezone.utc)
    return parsed


#: The JSON type each declared value type is stored as
#: (``declared_attr_indexes._JSON_TYPE``), and how a reply names a JSON type.
_DECLARED_JSON = {"text": "string", "num": "number", "bool": "boolean", "ts": "string"}
_JSON_WORDS = {"string": "text", "number": "number", "boolean": "bool",
               "object": "object", "array": "list"}
#: Text that is one JSON number and nothing else: the one reading a `num`
#: attribute can take of it.
_NUMBER_TEXT = re.compile(r"-?(0|[1-9][0-9]*)(\.[0-9]+)?([eE][+-]?[0-9]+)?")


def _json_type(value: Any) -> str:
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, (int, float)):
        return "number"
    if isinstance(value, str):
        return "string"
    if isinstance(value, Mapping):
        return "object"
    if isinstance(value, list):
        return "array"
    return type(value).__name__


def _declared_leaves(predicate: Any, declared: Mapping[str, str]):
    """Each comparison on a declared attribute: `(node, field, kind)`."""
    stack = [predicate]
    while stack:
        node = stack.pop()
        if not isinstance(node, Mapping):
            continue
        if isinstance(node.get("clauses"), list):
            stack.extend(reversed(node["clauses"]))
        if isinstance(node.get("clause"), Mapping):
            stack.append(node["clause"])
        field = node.get("field")
        if not isinstance(field, str) or not field.startswith("attrs."):
            continue
        kind = declared.get(field.split(".", 1)[1])
        if kind is not None:
            yield node, field, kind


def _bound_guess(bound: Any, kind: str) -> tuple[bool, Any]:
    """`(fits, guess)` for one range bound on an attribute declared `kind`.

    `guess` is the one value the bound can mean in that type (digits on a
    `num` attribute, a whole number on a `text` one), else `...`."""
    bound = _decode_nonfinite(bound)
    if kind == "ts":
        return isinstance(bound, str) and as_operator_timestamp(bound) is not None, ...
    if kind == "num":
        if isinstance(bound, (int, float)) and not isinstance(bound, bool):
            return True, ...
        number = _exact_number(bound)
        return False, (number if number is not None else ...)
    if kind == "text":
        if isinstance(bound, str):
            return True, ...
        if isinstance(bound, int) and not isinstance(bound, bool):
            return False, str(bound)
        return False, ...
    # A bool attribute (or a type this reader does not order) takes no range.
    return False, ...


def declared_bound_mismatch(
    predicate: Any, declared: Mapping[str, str] | None,
) -> tuple[str, str, Any] | None:
    """`(message, fix_hint, fixed)` for the first range bound of the wrong
    type for its attribute's declared type, or None (issue #3666, 2c).

    The effective lowerer gives such a bound no lowering, so the read is
    refused; these words say which bound and what the attribute takes. A
    `ts` attribute takes a date or timestamp the read check reads as an
    instant, a `num` attribute a number, a `text` attribute text, and a
    `bool` attribute no range. `fixed` is `predicate` with every such bound
    written as the one value it can mean in the declared type (digits on a
    `num` attribute, a whole number on a `text` one): a guess a refusal may
    offer. It is None when any such bound has no one reading.
    """
    if not declared:
        return None
    fixed = copy.deepcopy(predicate)
    first: tuple[str, str] | None = None
    guessable = True
    for node, field, kind in _declared_leaves(fixed, declared):
        op = node.get("op")
        if op in ("like", "like_exact") and kind in ("num", "bool"):
            # loose-match-3666: no text to read a substring in.
            if first is None:
                first = (
                    f"{op} on {field}: the attribute is declared {kind}, which "
                    "holds no text to read a substring in",
                    "compare it with eq or in" + (", lt, gt or between" if kind == "num" else ""),
                )
            guessable = False
            continue
        if op not in ("lt", "gt", "between"):
            continue
        key = "values" if op == "between" else "value"
        bounds = node.get("values") if op == "between" else [node.get("value")]
        if not isinstance(bounds, list):
            continue
        new = []
        for bound in bounds:
            fits, guess = _bound_guess(bound, kind)
            if fits:
                new.append(bound)
                continue
            if first is None:
                spelled = _json.dumps(bound, ensure_ascii=False, default=str)
                takes = {
                    "ts": "a date or timestamp (2025-01-01 or 2025-01-01T00:00:00Z)",
                    "num": "a number", "text": "text",
                }.get(kind)
                if takes is None:
                    message = (
                        f"{op} on {field}: the attribute is declared {kind}, which "
                        "takes no range"
                    )
                    first = (
                        message,
                        f'compare it with eq: {{"op": "eq", "field": "{field}", "value": true}}',
                    )
                else:
                    # The guess for a text attribute is the number's digits,
                    # and text orders by character.
                    as_text = (
                        '; as text it compares by character, so "10" sorts before "5"'
                        if kind == "text" else ""
                    )
                    message = (
                        f"{op} on {field}: the attribute is declared {kind}, and "
                        f"{spelled} is not {takes}"
                    )
                    first = (message, f"write the bound as {takes}{as_text}")
            if guess is ...:
                guessable = False
                new.append(bound)
            else:
                new.append(guess)
        node[key] = new if op == "between" else new[0]
    if first is None:
        return None
    return first[0], first[1], fixed if guessable else None


def declared_operand_notes(predicate: Any, declared: Mapping[str, str] | None) -> list[str]:
    """What an `eq` or `in` on a declared attribute compares, when an operand
    is not of the attribute's JSON type (issue #3666, 2c).

    `eq` and `in` compare the stored JSON value exactly, so such a test is
    served as written (a stored value of another type is possible); the
    reply says that it matches only a stored value of the operand's type,
    and what to write for the declared one. `null` ("has no value") needs
    no note.
    """
    if not declared:
        return []
    notes: list[str] = []
    for op, field, kind, operands in _equality_operands(predicate, declared):
        stored = _DECLARED_JSON.get(kind)
        for operand in operands:
            sent = _json_type(operand)
            if operand is None or stored is None or sent == stored:
                continue
            words = _JSON_WORDS.get(sent, sent)
            guess = _equality_guess(operand, kind)
            if kind == "num" and guess is not ...:
                # The operand's own digits: the pattern proves they are one
                # JSON number, and a float would lose digits of a long one.
                then = f"for the number write {_note_text(operand)}"
            elif guess is not ...:
                then = f"for the {_JSON_WORDS[stored]} write {_note_json(guess)}"
            else:
                then = f"a stored value is {_DECLARED_TAKES[kind]}"
            note = (
                f"{field} is declared {kind}; {op} with {_note_json(operand)} matches "
                f"only a stored {words} value; {then}"
            )
            if note not in notes:
                notes.append(note)
    return notes


#: What a declared attribute's stored value is, in a note.
_DECLARED_TAKES = {"text": "text", "num": "a number", "bool": "true or false",
                   "ts": "a date or timestamp as text"}


def _note_json(value: Any) -> str:
    """A value as ASCII JSON, cut to 80 characters: no line or field
    separator of an operand reaches the reply's notice line."""
    return _note_text(_json.dumps(value, ensure_ascii=True, default=str))


def _note_text(spelled: str) -> str:
    """ASCII text (a JSON number's digits, or JSON), cut to 80 characters."""
    return spelled if len(spelled) <= 80 else spelled[:77] + "..."


def _exact_number(text: Any) -> Any:
    """The number `text` writes, when Python holds it exactly: any whole
    number, or a float whose shortest form is the same decimal. None for
    text that is not one JSON number, and for a long decimal a float would
    round (0.1000000000000000055, 12345678901234567890.5): no guessed bound
    may compare another number than the one written."""
    if not isinstance(text, str) or not _NUMBER_TEXT.fullmatch(text):
        return None
    number = _json.loads(text)
    if isinstance(number, int):
        return number
    if not math.isfinite(number):
        return None
    return number if Decimal(repr(number)) == Decimal(text) else None


def _equality_guess(operand: Any, kind: str) -> Any:
    """The one value an equality operand can mean in the declared type, or
    `...`: the number written as text on `num` (the note writes its own
    digits), a whole number's digits on `text`, `true` or `false` written as
    text on `bool`."""
    if kind == "num" and isinstance(operand, str) and _NUMBER_TEXT.fullmatch(operand):
        return operand
    if kind == "text" and isinstance(operand, int) and not isinstance(operand, bool):
        return str(operand)
    if kind == "bool" and operand in ("true", "false"):
        return operand == "true"
    return ...


def _equality_operands(predicate: Any, declared: Mapping[str, str]):
    """`(op, field, kind, operands)` for each eq, in and contains test on a
    declared attribute; a `contains` on `attrs` is one eq per key."""
    stack = [predicate]
    while stack:
        node = stack.pop()
        if not isinstance(node, Mapping):
            continue
        if isinstance(node.get("clauses"), list):
            stack.extend(reversed(node["clauses"]))
        if isinstance(node.get("clause"), Mapping):
            stack.append(node["clause"])
        op, field = node.get("op"), node.get("field")
        if op == "contains" and field == "attrs" and isinstance(node.get("value"), Mapping):
            for key, item in node["value"].items():
                kind = declared.get(key) if isinstance(key, str) else None
                if kind is not None:
                    yield op, f"attrs.{key}", kind, [item]
            continue
        if op not in ("eq", "in") or not isinstance(field, str):
            continue
        if not field.startswith("attrs."):
            continue
        kind = declared.get(field.split(".", 1)[1])
        operands = [node.get("value")] if op == "eq" else node.get("values")
        if kind is not None and isinstance(operands, list):
            yield op, field, kind, operands


def holds_nul(value: Any) -> bool:
    """True when a JSON value carries NUL in any string or object key.

    PostgreSQL text and jsonb never hold NUL (jsonb refuses ``\\u0000``), so
    such an operand can never equal or prefix a stored value. The lowerer
    decides it here and never sends it to the database.
    """
    if isinstance(value, str):
        return "\x00" in value
    if isinstance(value, dict):
        return any(holds_nul(k) or holds_nul(v) for k, v in value.items())
    if isinstance(value, list):
        return any(holds_nul(v) for v in value)
    return False


def _bind_param(params: dict[str, Any], prefix: str, value: Any) -> str:
    index = len(params)
    while f"{prefix}{index}" in params:
        index += 1
    name = f"{prefix}{index}"
    params[name] = value
    return f"%({name})s"


def effective_field_map_identity(kind: str, declared: Mapping[str, str] | None) -> str:
    """The field-map identity an effective-attribute compile is fingerprinted by.

    It names the lowerer version, the scope kind and a digest of the declared
    value types the lowerer typed its comparisons by, so one predicate over a
    different declaration compiles to a different fingerprint.
    """
    typed = {str(key): str(value) for key, value in (declared or {}).items()}
    digest = hashlib.sha256(_canonical(typed)).hexdigest()
    return f"{EFFECTIVE_LOWERER_VERSION}:{kind}:declared={digest[:16]}"


#: The marker key a non-finite float operand is carried under. Canonical
#: JSON has no Infinity or NaN, but a v1 range bound may be one, and the
#: operator surface has always compared it as that float8.
NONFINITE_MARKER = "$float"
_NONFINITE = {"Infinity": float("inf"), "-Infinity": float("-inf"), "NaN": float("nan")}


def encode_nonfinite_operands(predicate: Any) -> Any:
    """``predicate`` with each non-finite float operand replaced by a marker.

    ``{"$float": "Infinity"}`` (or ``-Infinity``, ``NaN``) is canonical JSON,
    so the compile and its fingerprint stay well defined; the effective
    lowerer reads the marker back as the float in a range bound. Everything
    else is returned as it is.
    """
    if isinstance(predicate, float) and not math.isfinite(predicate):
        text = "NaN" if math.isnan(predicate) else ("Infinity" if predicate > 0 else "-Infinity")
        return {NONFINITE_MARKER: text}
    if isinstance(predicate, dict):
        return {key: encode_nonfinite_operands(value) for key, value in predicate.items()}
    if isinstance(predicate, list):
        return [encode_nonfinite_operands(value) for value in predicate]
    return predicate


def _decode_nonfinite(value: Any) -> Any:
    if (
        isinstance(value, dict)
        and set(value) == {NONFINITE_MARKER}
        and value[NONFINITE_MARKER] in _NONFINITE
    ):
        return _NONFINITE[value[NONFINITE_MARKER]]
    return value


def _float_param(value: float) -> Any:
    """A float bound as a JSON-safe parameter: text for a non-finite one,
    which ``::float8`` reads back as Infinity, -Infinity or NaN."""
    if math.isfinite(value):
        return value
    return "NaN" if math.isnan(value) else ("Infinity" if value > 0 else "-Infinity")


def _sides(op_name: str, bounds: list[Any]) -> list[tuple[str, Any]]:
    if op_name == "between":
        return [(">=", bounds[0]), ("<=", bounds[1])]
    return [(_RANGE_SQL[op_name], bounds[0])]


def _text_sides(op_name: str, bounds: list[str]) -> list[tuple[str, str]]:
    """Comparison sides for text bounds, with a NUL-bearing bound rewritten.

    No stored text holds NUL. For a bound ``p + NUL + r`` a value is below it
    exactly when it is at most ``p``, and above it exactly when it is above
    ``p`` (both in the Python evaluator's code-point order). The NUL-bearing
    bound itself is never sent.
    """
    sides: list[tuple[str, str]] = []
    for cmp, bound in _sides(op_name, bounds):
        if "\x00" in bound:
            bound = bound[: bound.index("\x00")]
            cmp = {"<": "<=", "<=": "<=", ">": ">", ">=": ">"}[cmp]
        sides.append((cmp, bound))
    return sides


def effective_attr_lowerer(
    kind: str,
    *,
    declared: Mapping[str, str] | None = None,
    relation: str | None = None,
    nullable: bool = False,
    readable: Callable[[str], str] | None = None,
):
    """The leaf lowerer for ``filter``, ``map``, ``aggregate`` and ``expand``.

    It lowers one v1 leaf (``eq``, ``in``, ``exists``, ``contains``,
    ``prefix``, ``suffix``, ``lt``, ``gt``, ``between``, ``like``,
    ``like_exact``, and the internal date leaf ``DATE_WINDOW_OP``) over the
    effective-attribute relation to a two-valued SQL condition that agrees
    with ``server._operator_predicate_matches`` row for row, so ``not`` means
    what it means in Python. It returns None for a leaf it cannot lower
    exactly; the compile is then a superset, which callers refuse.

    Every caller value is a bound parameter. Column and relation names come
    from this module and the caller's code, never from a request. JSON
    operands are bound as text cast with ``::jsonb`` and timestamps as ISO
    text cast with ``::timestamptz``, so the compiled parameters stay JSON.

    ``nullable`` is for a LEFT JOINed relation (expand's destination node):
    an absent row reads as empty attrs and null columns, as the Python
    evaluator reads it, rather than as SQL NULL.

    ``readable`` (read-model slice 7a, Nomos `audit`/`enforce`) maps an
    attribute key to the reader view's per-row SQL boolean
    (``ReaderView.readable`` as the caller renders it): TRUE when the reader
    is served the value, FALSE when it is masked or dropped, NULL when the
    view cannot tell. Each leaf on ``attrs.<key>`` then lowers to ``CASE WHEN
    <readable> THEN <leaf> END``, so a clause on a withheld or undecided value
    is NULL (unknown) and stays unknown under ``not``; a leaf on the whole
    ``attrs`` object other than ``exists`` is unknown outright; ``contains``
    guards each key. The lowering is then deliberately three-valued: the
    caller admits a row when the tree IS TRUE or a key it reads is undecided,
    and the check on the read row decides.
    """
    columns = EFFECTIVE_VIEW_FIELD_COLUMNS[kind]
    rel = f"{relation}." if relation else ""
    attrs_sql = f"COALESCE({rel}attrs, '{{}}'::jsonb)" if nullable else f"{rel}attrs"

    def declared_type(field: Any) -> str | None:
        if declared is None or not isinstance(field, str) or not field.startswith("attrs."):
            return None
        return declared.get(field.split(".", 1)[1])

    def lower(node: Mapping[str, Any], params: dict[str, Any], prefix: str) -> SqlLeaf | None:
        def bind(value: Any) -> str:
            return _bind_param(params, prefix, value)

        def json_param(value: Any) -> str:
            return f"{bind(_json.dumps(value, sort_keys=True, ensure_ascii=False))}::jsonb"

        def resolve(field: Any) -> tuple[str, str, str] | None:
            """``(expr, form, key_ref)``; form is column, attrs, key or absent.

            A key holding NUL is ``absent``: no stored object can carry it.
            """
            if not isinstance(field, str):
                return None
            if field == "attrs":
                return attrs_sql, "attrs", ""
            if field.startswith("attrs."):
                key = field.split(".", 1)[1]
                if not key:
                    return None
                if "\x00" in key:
                    return "", "absent", ""
                key_ref = bind(key)
                return f"({attrs_sql} -> {key_ref})", "key", key_ref
            column = columns.get(field)
            return (f"{rel}{column}", "column", "") if column else None

        def known(field: Any) -> bool:
            if not isinstance(field, str):
                return False
            if field.startswith("attrs."):
                return bool(field.split(".", 1)[1])
            return field == "attrs" or field in columns

        def eq(field: Any, value: Any) -> str | None:
            if not known(field):
                return None
            if holds_nul(value):
                # Decided before anything is bound: no stored value holds NUL.
                return "FALSE"
            resolved = resolve(field)
            if resolved is None:
                return None
            expr, form, key_ref = resolved
            if form == "absent":
                # dict.get(key) is None for a key no row can hold.
                return "TRUE" if value is None else "FALSE"
            if form == "column":
                if value is None:
                    return f"({expr} IS NULL)"
                if not isinstance(value, str):
                    # A column holds text: a value of another type has no
                    # lowering, so the read is refused, never served as
                    # "matches nothing" (issue #3666, 2c; the grammar names
                    # the fix before a set read gets here).
                    return None
                # Sargable and two-valued: `col = v` alone is NULL on a NULL
                # column, which NOT would keep NULL.
                return f"({expr} = {bind(value)} AND {expr} IS NOT NULL)"
            if form == "attrs":
                if not isinstance(value, Mapping):
                    # The attribute map is an object: it equals no other value.
                    return None
                return f"COALESCE({expr} = {json_param(value)}, FALSE)"
            if value is None:
                # dict.get(key) is None for both an absent key and JSON null.
                return f"((NOT {attrs_sql} ? {key_ref}) OR {expr} = 'null'::jsonb)"
            if declared_type(field) == "ts" and isinstance(value, str):
                parsed = as_operator_timestamp(value)
                text_expr = f"({expr} #>> '{{}}')"
                if parsed is None:
                    return f"COALESCE({text_expr} = {bind(value)}, FALSE)"
                # okg#3112 step 2 (design 2.2): stored values read through
                # okg.attr_ts_v4; the bound is v4's instant when v4 accepts
                # it, else the Python-parsed one. COALESCE keeps an absent
                # key FALSE rather than NULL, so NOT agrees with Python.
                raw_value = bind(value)
                stored = f"okg.attr_ts_v4({text_expr})"
                return (
                    f"COALESCE(CASE WHEN jsonb_typeof({expr}) = 'string' "
                    f"AND {stored} IS NOT NULL THEN {stored} = "
                    f"COALESCE(okg.attr_ts_v4({raw_value}::text), "
                    f"{bind(parsed.isoformat())}::timestamptz) "
                    f"ELSE {text_expr} = {raw_value} END, FALSE)"
                )
            return f"COALESCE({expr} = {json_param(value)}, FALSE)"

        def range_(field: Any, op_name: str, bounds: list[Any]) -> str | None:
            resolved = resolve(field)
            if resolved is None or resolved[1] == "attrs":
                return None
            expr, form, _key_ref = resolved
            if form == "absent":
                return "FALSE"
            if form == "column":
                if field == "edge_id" and kind == "edges" and all(
                    isinstance(v, int) and not isinstance(v, bool) for v in bounds
                ):
                    # A link's id is a number stored as its digits: an integer
                    # range compares it as that number (issue #3666), as the
                    # row check does (`server._operator_predicate_matches`).
                    numeric = " AND ".join(
                        f"({expr})::numeric {cmp} {bind(b)}" for cmp, b in _sides(op_name, bounds)
                    )
                    return f"CASE WHEN {expr} ~ '^[0-9]+$' THEN ({numeric}) ELSE FALSE END"
                if field == "edge_id" or not all(isinstance(v, str) for v in bounds):
                    # A bound of the wrong type for the column: no lowering,
                    # so the caller refuses it instead of serving "matches
                    # nothing" (issue #3666, 2c; the grammar names the fix).
                    return None
                guard = f"{expr} IS NOT NULL"
                # The reader compares Python code points; COLLATE "C" orders
                # UTF-8 bytes the same way, whatever the column's collation
                # (as `string_field_lowerer` does). Issue #3666: filter now
                # serves a range on a column through this lowerer.
                comparison = " AND ".join(
                    f'{expr} COLLATE "C" {cmp} {bind(b)}'
                    for cmp, b in _text_sides(op_name, bounds)
                )
                # CASE prevents a malformed JSON scalar from reaching a cast.
                return f"CASE WHEN {guard} THEN ({comparison}) ELSE FALSE END"
            # A non-finite float bound arrives as its marker (see
            # encode_nonfinite_operands); it compares as that float8.
            bounds = [_decode_nonfinite(v) for v in bounds]
            numeric = all(
                isinstance(v, (int, float)) and not isinstance(v, bool) for v in bounds
            )
            inferred = declared_type(field)
            if inferred is None:
                if numeric:
                    inferred = "num"
                elif all(isinstance(v, str) for v in bounds):
                    inferred = "text"
            if inferred == "num" and numeric:
                # okg#3112 step 2 (design 2.4): a float bound compares in
                # double precision through okg.attr_num_f8, which never
                # raises 22003; an int bound compares exactly.
                value_expr = f"(({expr} #>> '{{}}')::numeric)"
                guard = f"jsonb_typeof({expr}) = 'number'"
                comparison = " AND ".join(
                    f"okg.attr_num_f8({value_expr}) {cmp} {bind(_float_param(b))}::float8"
                    if isinstance(b, float)
                    else f"{value_expr} {cmp} {bind(b)}"
                    for cmp, b in _sides(op_name, bounds)
                )
            elif inferred == "ts" and all(isinstance(v, str) for v in bounds):
                parsed = [as_operator_timestamp(v) for v in bounds]
                if any(v is None for v in parsed):
                    # A text bound the read check cannot read as an instant
                    # (01/02/2024, a leap second, "soon"): no lowering, so
                    # the read is refused, never served as an empty page
                    # (issue #3666, 2c; it compiled to FALSE before).
                    return None
                text_expr = f"({expr} #>> '{{}}')"
                value_expr = f"okg.attr_ts_v4({text_expr})"
                guard = f"jsonb_typeof({expr}) = 'string' AND {value_expr} IS NOT NULL"
                # okg#3112 step 2 (design 2.2): as the ts equality above.
                comparison = " AND ".join(
                    f"{value_expr} {cmp} COALESCE(okg.attr_ts_v4({bind(raw)}::text), "
                    f"{bind(when.isoformat())}::timestamptz)"
                    for (cmp, raw), when in zip(_sides(op_name, bounds), parsed)
                )
            elif inferred == "text" and all(isinstance(v, str) for v in bounds):
                value_expr = f"({expr} #>> '{{}}')"
                guard = f"jsonb_typeof({expr}) = 'string'"
                comparison = " AND ".join(
                    f"{value_expr} {cmp} {bind(b)}" for cmp, b in _text_sides(op_name, bounds)
                )
            else:
                # Bounds of no one type the attribute compares as (a bool,
                # null, an object, a number beside text, or a type other than
                # the declared one): no lowering, so the read is refused,
                # never "matches nothing" (issue #3666, 2c).
                return None
            return f"CASE WHEN {guard} THEN ({comparison}) ELSE FALSE END"

        def guard(key_field: Any, leaf: str | None) -> str | None:
            if readable is None or leaf is None or not isinstance(key_field, str):
                return leaf
            if key_field.startswith("attrs.") and key_field.split(".", 1)[1]:
                return f"(CASE WHEN {readable(key_field.split('.', 1)[1])} THEN {leaf} END)"
            if key_field == "attrs" and leaf != "TRUE":
                return "CAST(NULL AS boolean)"
            return leaf

        op = node.get("op")
        field = node.get("field")
        sql: str | None
        if op == "eq":
            sql = eq(field, node.get("value"))
        elif op == "in":
            values = node.get("values")
            if not isinstance(values, list) or not values:
                return None
            parts = [eq(field, value) for value in values]
            sql = None if any(p is None for p in parts) else "(" + " OR ".join(map(str, parts)) + ")"
        elif op == "exists":
            resolved = resolve(field)
            if resolved is None:
                return None
            expr, form, key_ref = resolved
            sql = {
                "column": f"({expr} IS NOT NULL)",
                "attrs": "TRUE",
                "absent": "FALSE",
                "key": f"({attrs_sql} ? {key_ref})",
            }[form]
        elif op == "contains" and is_text_contains(node):
            # Text inside one attribute (issue #3666): a stored JSON string
            # holding the operand, ASCII case ignored, as `text_contains`
            # reads it. Not index-backed: it is tested on each row the rest
            # of the condition and the scope leave.
            value = node["value"]
            resolved = resolve(field)
            if resolved is None:
                return None
            if resolved[1] == "absent" or "\x00" in value:
                sql = "FALSE"
            else:
                expr = resolved[0]
                needle = bind(ascii_casefold(value))

                def holds(text_sql: str) -> str:
                    return (
                        f"strpos(translate({text_sql}, '{_ASCII_UPPER}', "
                        f"'{_ASCII_LOWER}'), {needle}) > 0"
                    )

                # A list matches when one of its string elements holds the
                # text; the walk is bounded by the stored list's length.
                whole = f"({expr} #>> '{{}}')"
                item = "(text_item.v #>> '{}')"
                sql = (
                    f"CASE jsonb_typeof({expr}) "
                    f"WHEN 'string' THEN {holds(whole)} "
                    f"WHEN 'array' THEN EXISTS (SELECT 1 FROM "
                    f"jsonb_array_elements({expr}) AS text_item(v) "
                    f"WHERE jsonb_typeof(text_item.v) = 'string' AND {holds(item)}) "
                    "ELSE FALSE END"
                )
        elif op == "contains":
            value = node.get("value")
            if field != "attrs" or not isinstance(value, dict):
                return None
            parts = [guard(f"attrs.{key}", eq(f"attrs.{key}", item)) for key, item in value.items()]
            if any(p is None for p in parts):
                return None
            sql = "(" + " AND ".join(map(str, parts)) + ")" if parts else "TRUE"
            return SqlLeaf(sql)
        elif op in ("prefix", "suffix"):
            value = node.get("value")
            if not isinstance(value, str) or not value or not known(field) or field == "attrs":
                return None
            side = "right" if op == "suffix" else "left"
            if "\x00" in value:
                sql = "FALSE"
            elif (resolved := resolve(field)) is None:
                return None
            elif (form := resolved[1]) == "absent":
                sql = "FALSE"
            elif form == "column":
                expr, bound = resolved[0], bind(value)
                sql = f"COALESCE({side}({expr}, char_length({bound})) = {bound}, FALSE)"
            else:
                expr, bound = resolved[0], bind(value)
                sql = (
                    f"CASE WHEN jsonb_typeof({expr}) = 'string' THEN "
                    f"{side}(({expr} #>> '{{}}'), char_length({bound})) = {bound} "
                    "ELSE FALSE END"
                )
        elif op in ("like", "like_exact"):
            # loose-match-3666: the stored string (or a string element of a
            # stored list) contains the operand, both folded for like
            # (`text_fold.fold_sql`) and both in NFC for like_exact. Two-
            # valued, and the same answer as `text_fold.matches` row for
            # row. A key declared a number or boolean holds no text: no
            # lowering, so the read is refused, never "matches nothing".
            from okg.substrate.mcp import text_fold

            value = node.get("value")
            if (
                text_fold.operand_problem(op, value) is not None
                or not known(field) or field == "attrs"
                or declared_type(field) in ("num", "bool")
            ):
                return None
            resolved = resolve(field)
            if resolved is None:
                return None
            expr, form, _key_ref = resolved

            def marks() -> str:
                # One bound pattern serves every like leaf of the statement.
                pattern = text_fold.mark_pattern()
                for name, bound_value in params.items():
                    if bound_value == pattern:
                        return f"%({name})s"
                return bind(pattern)

            def compared(text_expr: str) -> str:
                if op == "like":
                    return text_fold.fold_sql(text_expr, marks())
                return text_fold.exact_sql(text_expr)

            if form == "absent":
                sql = "FALSE"
            else:
                wanted = bind(text_fold.needle(op, value))
                if form == "column":
                    sql = f"COALESCE(strpos({compared(expr)}, {wanted}) > 0, FALSE)"
                else:
                    element = compared("(like_element.value #>> '{}')")
                    text_of = f"({expr} #>> '{{}}')"
                    sql = (
                        f"CASE WHEN jsonb_typeof({expr}) = 'string' THEN "
                        f"strpos({compared(text_of)}, {wanted}) > 0 "
                        f"WHEN jsonb_typeof({expr}) = 'array' THEN EXISTS (SELECT 1 FROM "
                        f"jsonb_array_elements({expr}) AS like_element(value) WHERE "
                        "jsonb_typeof(like_element.value) = 'string' AND "
                        f"strpos({element}, {wanted}) > 0) "
                        "ELSE FALSE END"
                    )
        elif op == RAW_TEXT_CONTAINS:
            # Exact on its own: the stored attribute is a JSON string that
            # contains `value`. A NUL operand is in no stored text.
            value = node.get("value")
            resolved = resolve(field)
            if not isinstance(value, str) or resolved is None or resolved[1] in ("column", "attrs"):
                return None
            if resolved[1] == "absent" or "\x00" in value:
                sql = "FALSE"
            else:
                expr = resolved[0]
                sql = (
                    f"CASE WHEN jsonb_typeof({expr}) = 'string' THEN "
                    f"strpos(({expr} #>> '{{}}'), {bind(value)}) > 0 ELSE FALSE END"
                )
        elif op == TEXT_CASEFOLD_EQ:
            value = node.get("value")
            resolved = resolve(field)
            if not isinstance(value, str) or resolved is None or resolved[1] in ("column", "attrs"):
                return None
            if resolved[1] == "absent" or "\x00" in value:
                sql = "FALSE"
            else:
                expr = resolved[0]
                sql = (
                    f"CASE WHEN jsonb_typeof({expr}) = 'string' THEN "
                    f"lower({expr} #>> '{{}}') = lower({bind(value)}) ELSE FALSE END"
                )
        elif op == DATE_WINDOW_OP:
            # A date condition (issue #3666). Two-valued and exact: see
            # date_conditions for why this SQL and the Python reading of the
            # same stored value agree. Imported here, not at module level:
            # this module stays free of imports so every reader can use it.
            from okg.substrate.mcp.date_conditions import window_leaf_sql

            resolved = resolve(field)
            if resolved is None or resolved[1] not in ("key", "absent"):
                return None
            if not isinstance(node.get("windows"), list):
                return None
            sql = "FALSE" if resolved[1] == "absent" else window_leaf_sql(
                resolved[0], node, bind,
            )
        elif op in ("lt", "gt"):
            sql = range_(field, op, [node.get("value")])
        elif op == "between":
            values = node.get("values")
            if not isinstance(values, list) or len(values) != 2:
                return None
            sql = range_(field, op, list(values))
        else:
            return None
        sql = guard(field, sql)
        return None if sql is None else SqlLeaf(sql)

    return lower
