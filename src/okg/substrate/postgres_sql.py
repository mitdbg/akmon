"""PostgreSQL SQL front end for the substrate's SQL safety checks.

This module replaces the GPL-licensed ``pglast`` binding with ``sqlglot``
(dialect ``postgres``, MIT) plus a small lexer written from PostgreSQL's own
scanner rules (``src/backend/parser/scan.l``). sqlglot builds the tree; the
lexer is the authority on what PostgreSQL will treat as a comment, a string,
a quoted identifier, or code.

Why two lexers: a safety check that parses SQL differently from the database
can be bypassed by text the checker thinks is inert (inside a comment or a
string) but the database executes, or the reverse. `parse` refuses unless
both lexers agree exactly on where every string, quoted identifier and
comment starts and ends, and every character our lexer calls code is covered
by a sqlglot token. Anything either side cannot read raises
`PostgresSqlError`; callers turn that into a refusal. The module never
guesses.

`U&"..."` identifiers and `U&'...'` strings, which sqlglot cannot parse, are
decoded here by PostgreSQL's rules and rewritten as ordinary quoted
identifiers and strings before sqlglot sees the text (`normalize`).
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Iterable, Iterator

__all__ = [
    "PostgresSqlError",
    "Token",
    "ParsedSql",
    "canonical_identifier",
    "cte_names_visible_at",
    "function_names",
    "lex",
    "normalize",
    "parse",
    "table_name_parts",
]


class PostgresSqlError(ValueError):
    """The SQL could not be read with certainty. Callers must refuse."""


# PostgreSQL's NAMEDATALEN is 64: identifiers are truncated to 63 bytes.
_MAX_IDENTIFIER_BYTES = 63

_WHITESPACE = frozenset(" \t\n\r\f\v")
_OPERATOR_CHARS = frozenset("~!@#^&|`?+-*/%<>=")
_SELF_CHARS = frozenset(",()[].;:")
_ASCII_UPPER = str.maketrans(
    "ABCDEFGHIJKLMNOPQRSTUVWXYZ", "abcdefghijklmnopqrstuvwxyz"
)

# Token kinds.
WORD = "word"  # unquoted identifier or keyword
QIDENT = "qident"  # "quoted" identifier (value decoded)
UIDENT = "uident"  # U&"quoted" identifier (value decoded)
STRING = "string"  # '...' or N'...'
ESTRING = "estring"  # E'...'
USTRING = "ustring"  # U&'...'
BSTRING = "bstring"  # B'...' / X'...'
DOLLAR = "dollar"  # $tag$...$tag$
NUMBER = "number"
PARAM = "param"  # $1
OPERATOR = "op"
PUNCT = "punct"
SEMICOLON = "semicolon"
LINE_COMMENT = "line_comment"
BLOCK_COMMENT = "block_comment"

LITERAL_KINDS = frozenset({QIDENT, UIDENT, STRING, ESTRING, USTRING, BSTRING, DOLLAR})
COMMENT_KINDS = frozenset({LINE_COMMENT, BLOCK_COMMENT})
IDENTIFIER_KINDS = frozenset({WORD, QIDENT, UIDENT})


@dataclass(frozen=True)
class Token:
    kind: str
    start: int  # inclusive offset
    end: int  # exclusive offset
    text: str  # the source text of the token
    value: str  # decoded value (strings, identifiers); source text otherwise


def _is_ident_start(ch: str) -> bool:
    return ch.isascii() and (ch.isalpha() or ch == "_") or not ch.isascii()


def _is_ident_char(ch: str) -> bool:
    return _is_ident_start(ch) or (ch.isascii() and ch.isdigit()) or ch == "$"


def _is_dollar_tag_char(ch: str) -> bool:
    return _is_ident_start(ch) or (ch.isascii() and ch.isdigit())


def canonical_identifier(value: str, *, quoted: bool) -> str:
    """Return the name PostgreSQL resolves an identifier to.

    Unquoted identifiers fold ASCII letters to lower case (PostgreSQL leaves
    other letters alone under UTF-8); quoted identifiers keep their text.
    Both are truncated to 63 bytes as PostgreSQL does.
    """
    name = value if quoted else value.translate(_ASCII_UPPER)
    encoded = name.encode("utf-8")
    if len(encoded) > _MAX_IDENTIFIER_BYTES:
        encoded = encoded[:_MAX_IDENTIFIER_BYTES]
        name = encoded.decode("utf-8", errors="ignore")
    return name


def _read_quoted(sql: str, start: int, quote: str, *, backslash: bool) -> tuple[int, str]:
    """Read a quoted body starting AFTER the opening quote at ``start - 1``.

    Returns ``(end_exclusive, decoded_value)``. Doubling the quote escapes it;
    with ``backslash`` a backslash escapes the next character (E'' strings).
    """
    out: list[str] = []
    i = start
    n = len(sql)
    while i < n:
        ch = sql[i]
        if backslash and ch == "\\":
            if i + 1 >= n:
                break
            out.append(_decode_backslash(sql, i))
            i += _backslash_length(sql, i)
            continue
        if ch == quote:
            if i + 1 < n and sql[i + 1] == quote:
                out.append(quote)
                i += 2
                continue
            return i + 1, "".join(out)
        out.append(ch)
        i += 1
    raise PostgresSqlError(f"unterminated quoted text starting at offset {start - 1}")


_SIMPLE_ESCAPES = {"b": "\b", "f": "\f", "n": "\n", "r": "\r", "t": "\t"}


def _backslash_length(sql: str, i: int) -> int:
    nxt = sql[i + 1]
    if nxt in "01234567":
        j = i + 1
        while j < len(sql) and j < i + 4 and sql[j] in "01234567":
            j += 1
        return j - i
    if nxt == "x":
        j = i + 2
        while j < len(sql) and j < i + 4 and sql[j] in "0123456789abcdefABCDEF":
            j += 1
        return j - i if j > i + 2 else 2
    if nxt in "uU":
        width = 4 if nxt == "u" else 8
        return 2 + width
    return 2


def _decode_backslash(sql: str, i: int) -> str:
    nxt = sql[i + 1]
    if nxt in _SIMPLE_ESCAPES:
        return _SIMPLE_ESCAPES[nxt]
    length = _backslash_length(sql, i)
    body = sql[i + 1 : i + length]
    try:
        if nxt in "01234567":
            return chr(int(body, 8))
        if nxt == "x" and length > 2:
            return chr(int(body[1:], 16))
        if nxt in "uU":
            code = int(body[1:], 16)
            if not 0 < code <= 0x10FFFF or 0xD800 <= code <= 0xDFFF:
                raise ValueError(code)
            return chr(code)
    except ValueError as exc:
        raise PostgresSqlError(f"invalid escape in E'' string at offset {i}") from exc
    return nxt


def _decode_unicode_escapes(body: str, *, offset: int) -> str:
    """Decode the ``\\XXXX`` / ``\\+XXXXXX`` / ``\\\\`` escapes of U&'' and U&""."""
    out: list[str] = []
    i = 0
    while i < len(body):
        ch = body[i]
        if ch != "\\":
            out.append(ch)
            i += 1
            continue
        if body[i + 1 : i + 2] == "\\":
            out.append("\\")
            i += 2
            continue
        if body[i + 1 : i + 2] == "+":
            digits, advance = body[i + 2 : i + 8], 8
            expected = 6
        else:
            digits, advance = body[i + 1 : i + 5], 5
            expected = 4
        if len(digits) != expected or any(
            c not in "0123456789abcdefABCDEF" for c in digits
        ):
            raise PostgresSqlError(f"invalid Unicode escape at offset {offset + i}")
        code = int(digits, 16)
        if not 0 < code <= 0x10FFFF or 0xD800 <= code <= 0xDFFF:
            raise PostgresSqlError(f"invalid Unicode code point at offset {offset + i}")
        out.append(chr(code))
        i += advance
    return "".join(out)


def lex(sql: str) -> list[Token]:
    """Tokenize ``sql`` by PostgreSQL's scanner rules. Whitespace is dropped."""
    tokens: list[Token] = []
    i = 0
    n = len(sql)
    while i < n:
        ch = sql[i]
        if ch in _WHITESPACE:
            i += 1
            continue
        start = i
        two = sql[i : i + 2]
        if two == "--":
            j = i
            while j < n and sql[j] not in "\n\r":
                j += 1
            tokens.append(Token(LINE_COMMENT, start, j, sql[start:j], ""))
            i = j
            continue
        if two == "/*":
            depth = 0
            j = i
            while j < n:
                if sql[j : j + 2] == "/*":
                    depth += 1
                    j += 2
                elif sql[j : j + 2] == "*/":
                    depth -= 1
                    j += 2
                    if depth == 0:
                        break
                else:
                    j += 1
            if depth != 0:
                raise PostgresSqlError(f"unterminated block comment at offset {start}")
            tokens.append(Token(BLOCK_COMMENT, start, j, sql[start:j], ""))
            i = j
            continue
        # Prefixed literals: E'', B'', X'', N'', U&'', U&"".
        lower = ch.lower()
        if lower in "bx" and sql[i + 1 : i + 2] == "'":
            # Bit and hex strings have no quote escape at all.
            close = sql.find("'", i + 2)
            if close < 0:
                raise PostgresSqlError(f"unterminated bit string at offset {start}")
            end = close + 1
            if sql[end : end + 1] == "'":
                raise PostgresSqlError(f"quote inside a bit string at offset {start}")
            tokens.append(Token(BSTRING, start, end, sql[start:end], sql[i + 2 : close]))
            i = end
            continue
        if lower in "en" and sql[i + 1 : i + 2] == "'":
            end, value = _read_quoted(sql, i + 2, "'", backslash=lower == "e")
            kind = ESTRING if lower == "e" else STRING
            tokens.append(Token(kind, start, end, sql[start:end], value))
            i = end
            continue
        if lower == "u" and sql[i + 1 : i + 2] == "&" and sql[i + 2 : i + 3] in {"'", '"'}:
            quote = sql[i + 2]
            end, raw = _read_quoted(sql, i + 3, quote, backslash=False)
            value = _decode_unicode_escapes(raw, offset=i + 3)
            kind = USTRING if quote == "'" else UIDENT
            if kind == UIDENT and not value:
                raise PostgresSqlError(f"zero-length identifier at offset {start}")
            tokens.append(Token(kind, start, end, sql[start:end], value))
            i = end
            continue
        if ch == "'":
            end, value = _read_quoted(sql, i + 1, "'", backslash=False)
            tokens.append(Token(STRING, start, end, sql[start:end], value))
            i = end
            continue
        if ch == '"':
            end, value = _read_quoted(sql, i + 1, '"', backslash=False)
            if not value:
                raise PostgresSqlError(f"zero-length identifier at offset {start}")
            tokens.append(Token(QIDENT, start, end, sql[start:end], value))
            i = end
            continue
        if ch == "$":
            j = i + 1
            if j < n and sql[j].isascii() and sql[j].isdigit():
                while j < n and sql[j].isascii() and sql[j].isdigit():
                    j += 1
                if j < n and _is_ident_char(sql[j]):
                    raise PostgresSqlError(f"trailing junk after parameter at offset {start}")
                tokens.append(Token(PARAM, start, j, sql[start:j], sql[start:j]))
                i = j
                continue
            if j < n and _is_ident_start(sql[j]):
                while j < n and _is_dollar_tag_char(sql[j]):
                    j += 1
            if j < n and sql[j] == "$":
                tag = sql[i : j + 1]
                close = sql.find(tag, j + 1)
                if close < 0:
                    raise PostgresSqlError(f"unterminated dollar-quoted string at offset {start}")
                end = close + len(tag)
                tokens.append(Token(DOLLAR, start, end, sql[start:end], sql[j + 1 : close]))
                i = end
                continue
            raise PostgresSqlError(f"unexpected '$' at offset {start}")
        if _is_ident_start(ch):
            j = i + 1
            while j < n and _is_ident_char(sql[j]):
                j += 1
            tokens.append(Token(WORD, start, j, sql[start:j], sql[start:j]))
            i = j
            continue
        if (ch.isascii() and ch.isdigit()) or (
            ch == "." and i + 1 < n and sql[i + 1].isascii() and sql[i + 1].isdigit()
        ):
            j = _scan_number(sql, i)
            if j < n and _is_ident_char(sql[j]):
                raise PostgresSqlError(f"trailing junk after numeric literal at offset {start}")
            tokens.append(Token(NUMBER, start, j, sql[start:j], sql[start:j]))
            i = j
            continue
        if ch == ";":
            tokens.append(Token(SEMICOLON, start, i + 1, ";", ";"))
            i += 1
            continue
        if ch in _SELF_CHARS:
            j = i + 1
            if sql[i : i + 2] in {"::", ":=", ".."}:
                j = i + 2
            tokens.append(Token(PUNCT, start, j, sql[start:j], sql[start:j]))
            i = j
            continue
        if ch in _OPERATOR_CHARS:
            j = i
            # An operator never contains the start of a comment.
            while j < n and sql[j] in _OPERATOR_CHARS and sql[j : j + 2] not in {"--", "/*"}:
                j += 1
            if j == i:
                # The run starts with a comment opener; handled above.
                raise PostgresSqlError(f"unreadable operator at offset {start}")
            tokens.append(Token(OPERATOR, start, j, sql[start:j], sql[start:j]))
            i = j
            continue
        raise PostgresSqlError(f"unexpected character {ch!r} at offset {start}")
    _refuse_string_continuation(sql, tokens)
    for index, token in enumerate(tokens):
        if token.kind in {USTRING, UIDENT}:
            following = _next_significant(tokens, index)
            if following is not None and following.kind == WORD and (
                following.value.translate(_ASCII_UPPER) == "uescape"
            ):
                raise PostgresSqlError("UESCAPE is not supported")
    return tokens


_STRING_KINDS = frozenset({STRING, ESTRING, USTRING, BSTRING})


def _refuse_string_continuation(sql: str, tokens: list[Token]) -> None:
    """Refuse two string constants separated only by whitespace and comments.

    PostgreSQL joins ``'a'`` newline ``'b'`` into one constant, reading the
    second part with the first part's escape rules (``E'...'`` then
    ``'...'``). Its scanner (``quotecontinue`` in scan.l) also lets ``--``
    comments stand in that gap: ``'a' -- c`` newline ``'b'`` is the one
    string ``'ab'`` (checked on PostgreSQL 17.10). Neither sqlglot nor this
    lexer models the join, so the text is refused rather than guessed. With
    no newline in the gap, or a ``/* */`` comment in it, PostgreSQL does not
    join the two and reports a syntax error, while sqlglot may read the pair
    as something else; that is refused too, which costs nothing.
    """
    for index, first in enumerate(tokens):
        if first.kind not in _STRING_KINDS:
            continue
        second = _next_significant(tokens, index)
        if second is None or second.kind not in _STRING_KINDS:
            continue
        raise PostgresSqlError(
            f"string continuation at offset {second.start} is not supported: "
            "two string constants separated only by whitespace or comments"
        )


_DECIMAL_RE = re.compile(
    r"(?:\d+(?:_\d+)*)?(?:\.(?:\d+(?:_\d+)*)?)?(?:[eE][+-]?\d+(?:_\d+)*)?\Z"
)
_RADIX_RE = {
    "x": re.compile(r"0[xX]_?[0-9a-fA-F]+(?:_[0-9a-fA-F]+)*\Z"),
    "o": re.compile(r"0[oO]_?[0-7]+(?:_[0-7]+)*\Z"),
    "b": re.compile(r"0[bB]_?[01]+(?:_[01]+)*\Z"),
}


def _scan_number(sql: str, i: int) -> int:
    j = _scan_number_span(sql, i)
    text = sql[i:j]
    radix = text[1:2].lower() if text[:1] == "0" and len(text) > 1 else ""
    pattern = _RADIX_RE.get(radix, _DECIMAL_RE)
    if not pattern.match(text) or not any(c.isdigit() for c in text):
        raise PostgresSqlError(f"invalid numeric literal {text!r} at offset {i}")
    return j


def _scan_number_span(sql: str, i: int) -> int:
    n = len(sql)
    if sql[i] == "0" and sql[i + 1 : i + 2].lower() in {"x", "o", "b"}:
        j = i + 2
        while j < n and (sql[j].isalnum() or sql[j] == "_") and sql[j].isascii():
            j += 1
        return j
    j = i
    while j < n and ((sql[j].isascii() and sql[j].isdigit()) or sql[j] == "_"):
        j += 1
    if j < n and sql[j] == "." and sql[j + 1 : j + 2] != ".":
        j += 1
        while j < n and ((sql[j].isascii() and sql[j].isdigit()) or sql[j] == "_"):
            j += 1
    if j < n and sql[j] in "eE":
        k = j + 1
        if k < n and sql[k] in "+-":
            k += 1
        if k < n and sql[k].isascii() and sql[k].isdigit():
            while k < n and sql[k].isascii() and sql[k].isdigit():
                k += 1
            j = k
    return j


def _next_significant(tokens: list[Token], index: int) -> Token | None:
    for token in tokens[index + 1 :]:
        if token.kind not in COMMENT_KINDS:
            return token
    return None


def normalize(sql: str, tokens: list[Token] | None = None) -> str:
    """Rewrite ``U&`` identifiers and strings as ordinary quoted text.

    Every other byte of ``sql`` is kept as written.

    A ``U&`` token written directly against a quote of its own kind is
    refused: ``okg.U&"v_nodes"U&"x"`` is a name and an alias to PostgreSQL,
    but decoded it would read ``"v_nodes""x"``, one identifier holding a
    doubled quote. The same goes for ``"a"U&"b"`` and the ``'...'`` forms.
    """
    tokens = lex(sql) if tokens is None else tokens
    pieces: list[str] = []
    cursor = 0
    for token in tokens:
        if token.kind == UIDENT:
            replacement = '"' + token.value.replace('"', '""') + '"'
        elif token.kind == USTRING:
            replacement = "'" + token.value.replace("'", "''") + "'"
        else:
            continue
        quote = replacement[0]
        if sql[token.start - 1 : token.start] == quote or sql[
            token.end : token.end + 1
        ] == quote:
            raise PostgresSqlError(
                f"a U& literal at offset {token.start} touches another quoted "
                "token; separate them with a space"
            )
        pieces.append(sql[cursor : token.start])
        pieces.append(replacement)
        cursor = token.end
    pieces.append(sql[cursor:])
    return "".join(pieces)


def statement_count(tokens: Iterable[Token]) -> int:
    count = 0
    open_statement = False
    for token in tokens:
        if token.kind in COMMENT_KINDS:
            continue
        if token.kind == SEMICOLON:
            if open_statement:
                count += 1
            open_statement = False
        else:
            open_statement = True
    return count + (1 if open_statement else 0)


_SQLGLOT_STRING_TYPES: frozenset[str] = frozenset({
    "STRING",
    "BYTE_STRING",
    "BIT_STRING",
    "HEX_STRING",
    "NATIONAL_STRING",
    "HEREDOC_STRING",
    "RAW_STRING",
    "UNICODE_STRING",
})


def _cross_check(text: str, tokens: list[Token], glot_tokens: list[Any]) -> None:
    """Refuse unless both lexers agree on literal, comment and code regions."""
    by_start = {tok.start: tok for tok in glot_tokens}
    literal_spans: list[tuple[int, int]] = []
    for token in tokens:
        if token.kind not in LITERAL_KINDS:
            continue
        literal_spans.append((token.start, token.end))
        glot = by_start.get(token.start)
        if glot is None or glot.end + 1 != token.end:
            raise PostgresSqlError(
                f"parsers disagree on the literal at offset {token.start}"
            )
        glot_type = glot.token_type.name
        if token.kind in {QIDENT, UIDENT}:
            if glot_type != "IDENTIFIER":
                raise PostgresSqlError(
                    f"parsers disagree on the identifier at offset {token.start}"
                )
        elif glot_type not in _SQLGLOT_STRING_TYPES:
            raise PostgresSqlError(
                f"parsers disagree on the string at offset {token.start}"
            )
    comment_spans = [(t.start, t.end) for t in tokens if t.kind in COMMENT_KINDS]
    covered = bytearray(len(text) + 1)
    literal_starts = {start for start, _end in literal_spans}
    for glot in glot_tokens:
        g_start, g_end = glot.start, glot.end + 1
        glot_type = glot.token_type.name
        if glot_type == "UNKNOWN":
            raise PostgresSqlError(
                f"sqlglot could not classify the text at offset {g_start}: {glot.text!r}"
            )
        if (
            glot_type in _SQLGLOT_STRING_TYPES or glot_type == "IDENTIFIER"
        ) and g_start not in literal_starts:
            raise PostgresSqlError(
                f"parsers disagree on the literal at offset {g_start}"
            )
        for c_start, c_end in comment_spans:
            if g_start < c_end and c_start < g_end:
                raise PostgresSqlError(
                    f"parsers disagree on the comment at offset {c_start}"
                )
        for l_start, l_end in literal_spans:
            if g_start < l_end and l_start < g_end and (g_start, g_end) != (l_start, l_end):
                raise PostgresSqlError(
                    f"parsers disagree on the literal at offset {l_start}"
                )
        for offset in range(max(g_start, 0), min(g_end, len(text))):
            covered[offset] = 1
    for token in tokens:
        if token.kind in COMMENT_KINDS or token.kind in LITERAL_KINDS:
            continue
        if not all(covered[offset] for offset in range(token.start, token.end)):
            raise PostgresSqlError(
                f"parsers disagree on the code at offset {token.start}: {token.text!r}"
            )


@dataclass(frozen=True)
class ParsedSql:
    text: str  # the normalized text every offset below refers to
    tokens: tuple[Token, ...]  # our lexer's tokens over ``text``
    statements: tuple[Any, ...]  # sqlglot expressions, empty statements dropped


# PostgreSQL keywords sqlglot tokenizes as plain words but consumes as syntax
# (``NULLS LAST``, ``EXTRACT(YEAR FROM ...)``, ``AT TIME ZONE``, window frames,
# ``WITHIN GROUP``, ``TRIM(BOTH ...)``, lock strengths) without keeping an
# ``Identifier`` for them. They are the only words the strict check lets go
# unaccounted. The exemption does not look at position: a relation named by
# one of these words that sqlglot dropped from its tree would not be caught
# by this check (a relation sqlglot keeps is still judged by the caller's
# relation walk).
_SYNTAX_WORDS = frozenset({
    "at", "both", "breadth", "century", "current", "cycle", "day", "decade",
    "depth", "dow", "doy", "epoch", "exclude", "first", "following", "groups",
    "hour", "isodow", "isoyear", "julian", "key", "last", "leading", "locked",
    "materialized", "microseconds", "millennium", "milliseconds", "minute",
    "month", "no", "nowait", "nulls", "ordinality", "others", "preceding",
    "quarter", "range", "row", "rows", "search", "second", "share", "skip",
    "ties", "time", "timezone", "timezone_hour", "timezone_minute",
    "trailing", "unbounded", "week", "within", "year", "zone",
})


def _check_identifiers_accounted(parsed: ParsedSql, glot_tokens: list[Any]) -> None:
    """Strict mode: every identifier sqlglot read must be in the tree.

    sqlglot could consume an identifier while parsing an option it does not
    model and drop it; a relation named there would never reach the relation
    check. Each VAR / IDENTIFIER token must be an ``Identifier`` in the tree at
    the same offset, or a function name (the next token is ``(``).
    """
    from sqlglot import exp

    starts: set[int] = set()
    for statement in parsed.statements:
        for node in statement.walk():
            if isinstance(node, exp.Identifier):
                start = (node.meta or {}).get("start")
                if isinstance(start, int):
                    starts.add(start)
    for index, glot in enumerate(glot_tokens):
        if glot.token_type.name not in {"VAR", "IDENTIFIER"}:
            continue
        if glot.start in starts:
            continue
        if (
            glot.token_type.name == "VAR"
            and glot.text.translate(_ASCII_UPPER) in _SYNTAX_WORDS
        ):
            continue
        following = glot_tokens[index + 1] if index + 1 < len(glot_tokens) else None
        if following is not None and following.token_type.name == "L_PAREN":
            continue
        if (
            index >= 2
            and glot_tokens[index - 1].token_type.name == "L_PAREN"
            and glot_tokens[index - 2].text.upper() == "OPERATOR"
        ):
            # The schema of OPERATOR(schema.op); the Operator node keeps it
            # as text and callers check that text.
            continue
        raise PostgresSqlError(
            f"identifier {glot.text!r} at offset {glot.start} is missing from the parse tree"
        )


def parse(sql: str, *, strict: bool = False, standard_strings: bool = False) -> ParsedSql:
    """Parse PostgreSQL ``sql`` with sqlglot, cross-checked by our lexer.

    ``strict`` (safety checks) additionally refuses a standard string that
    contains a backslash -- its meaning depends on the server setting
    ``standard_conforming_strings`` -- and any identifier sqlglot consumed
    without putting it in the tree.

    ``standard_strings`` lifts only the backslash refusal, for a caller that
    itself sets ``standard_conforming_strings = on`` in the transaction that
    runs the text (run_sql does, issue #3666). Under that setting PostgreSQL
    reads a backslash in ``'...'`` as an ordinary character, exactly as this
    lexer and sqlglot do, so the string ends where all three say it ends.
    """
    import sqlglot
    from sqlglot.dialects.postgres import Postgres

    original_tokens = lex(sql)
    text = normalize(sql, original_tokens)
    tokens = lex(text)
    if strict and not standard_strings:
        for token in tokens:
            if token.kind == STRING and "\\" in token.value:
                raise PostgresSqlError(
                    "a backslash in a plain '...' string is ambiguous; use E'...'"
                )
    try:
        glot_tokens = Postgres().tokenize(text)
    except Exception as exc:  # noqa: BLE001 - any tokenizer failure refuses
        raise PostgresSqlError(f"sqlglot could not tokenize the SQL: {exc}") from exc
    _cross_check(text, tokens, glot_tokens)
    try:
        statements = sqlglot.parse(text, read="postgres")
    except Exception as exc:  # noqa: BLE001 - any parser failure refuses
        raise PostgresSqlError(f"sqlglot could not parse the SQL: {exc}") from exc
    # sqlglot carries a comment after the last `;` as a Semicolon node; it is
    # not a statement.
    kept = tuple(
        statement
        for statement in statements
        if statement is not None and type(statement).__name__ != "Semicolon"
    )
    if len(kept) != statement_count(tokens):
        raise PostgresSqlError(
            "parsers disagree on the number of statements "
            f"({statement_count(tokens)} by PostgreSQL rules, {len(kept)} by sqlglot)"
        )
    parsed = ParsedSql(text=text, tokens=tuple(tokens), statements=kept)
    if strict:
        _check_identifiers_accounted(parsed, glot_tokens)
    return parsed


def statement_text(parsed: ParsedSql) -> str:
    """The checked text of a single statement, ready to embed in other SQL.

    This is the text that was validated (after ``U&`` decoding), not a
    re-rendering: sqlglot's renderer does not preserve meaning in every case
    (it rewrites types and string continuations, for example). Leading and
    trailing comments and semicolons are dropped so the statement can be
    wrapped in parentheses; comments inside it are kept as written.
    run_sql's graph path no longer re-renders downstream either: it splices
    relation names into the checked text at parse positions (#2899, fixed by
    #2906; see ``okg.substrate.mcp.graph_sql_splice``).
    """
    if len(parsed.statements) != 1:
        raise PostgresSqlError("expected exactly one statement")
    code = [t for t in parsed.tokens if t.kind not in COMMENT_KINDS and t.kind != SEMICOLON]
    if not code:
        raise PostgresSqlError("the statement is empty")
    return parsed.text[code[0].start : code[-1].end]


# ---- tree helpers ----


def identifier_name(node: Any) -> str:
    """Canonical name of a sqlglot ``Identifier`` (or plain string)."""
    from sqlglot import exp

    if isinstance(node, exp.Identifier):
        return canonical_identifier(str(node.this), quoted=bool(node.args.get("quoted")))
    if isinstance(node, str):
        return canonical_identifier(node, quoted=False)
    return ""


def identifier_is_canonical_lower(node: Any) -> bool:
    """True when a quoted identifier is already in folded (lower) form."""
    from sqlglot import exp

    if isinstance(node, exp.Identifier) and node.args.get("quoted"):
        return str(node.this) == str(node.this).translate(_ASCII_UPPER)
    return True


def table_name_parts(table: Any) -> tuple[str | None, str | None, str] | None:
    """``(catalog, schema, relation)`` for a relation reference, else None.

    None means the FROM item is not a named relation (a table function).
    """
    from sqlglot import exp

    this = table.this
    if not isinstance(this, exp.Identifier):
        return None
    catalog = table.args.get("catalog")
    schema = table.args.get("db")
    return (
        identifier_name(catalog) if catalog is not None else None,
        identifier_name(schema) if schema is not None else None,
        identifier_name(this),
    )


def _cte_name(cte: Any) -> str:
    alias = cte.args.get("alias")
    return identifier_name(alias.this) if alias is not None else ""


def cte_names_visible_at(node: Any) -> set[str]:
    """Names of the CTEs PostgreSQL would resolve a bare reference at ``node`` to.

    A ``WITH`` list is visible to the body it is attached to. Inside the list,
    a CTE sees only the siblings declared before it, unless the list is
    ``RECURSIVE``, in which case it sees them all (itself included).
    """
    from sqlglot import exp

    visible: set[str] = set()
    child = node
    parent = node.parent
    while parent is not None:
        if isinstance(parent, exp.With) and isinstance(child, exp.CTE):
            ctes = list(parent.expressions)
            if parent.args.get("recursive"):
                visible.update(_cte_name(cte) for cte in ctes)
            else:
                for cte in ctes:
                    if cte is child:
                        break
                    visible.add(_cte_name(cte))
        elif not isinstance(parent, exp.With):
            with_ = parent.args.get("with_") or parent.args.get("with")
            if isinstance(with_, exp.With) and with_ is not child:
                visible.update(_cte_name(cte) for cte in with_.expressions)
        child = parent
        parent = parent.parent
    visible.discard("")
    return visible


def function_names(node: Any) -> set[str]:
    """Every lower-case name a sqlglot function node could have been written as."""
    from sqlglot import exp

    if isinstance(node, exp.Anonymous):
        name = identifier_name(node.this)
        return {name} if name else set()
    if isinstance(node, exp.Func):
        try:
            return {name.lower() for name in type(node).sql_names()}
        except Exception:  # noqa: BLE001 - a class without sql names
            return {type(node).__name__.lower()}
    return set()


def iter_nodes(statement: Any) -> Iterator[Any]:
    """Every node of a sqlglot tree, depth first."""
    yield from statement.walk(bfs=False)


def splice(text: str, replacements: list[tuple[int, int, str]]) -> str:
    """Replace ``[start, end)`` spans of ``text``; spans must not overlap."""
    out: list[str] = []
    cursor = 0
    for start, end, value in sorted(replacements):
        if start < cursor:
            raise PostgresSqlError("overlapping rewrite spans")
        out.append(text[cursor:start])
        out.append(value)
        cursor = end
    out.append(text[cursor:])
    return "".join(out)
