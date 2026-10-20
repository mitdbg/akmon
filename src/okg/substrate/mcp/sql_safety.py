"""SQL-safety guard for the MCP `run_sql` tool -- parser-based.

The guard parses with sqlglot (dialect ``postgres``) through
`okg.substrate.postgres_sql`, which cross-checks sqlglot against a lexer
written from PostgreSQL's own scanner rules: the two must agree on every
comment, string and quoted identifier, or the SQL is refused. (It replaced
the GPL-licensed ``pglast`` binding; see PACT ``sql-safety-without-pglast``.)
Anything the front end cannot read with certainty is refused, never allowed.

Layers, in order:

  1. **Statement type.** Exactly one statement, and it is a query:
     ``SELECT`` (with or without ``WITH``), a set operation, or ``VALUES``.
     A non-query first statement is ``run_sql_non_select``; a second
     statement is ``run_sql_multi_statement``. Anywhere in the tree,
     data-modifying or DDL nodes (a writable CTE), ``SELECT ... INTO``, row
     locks (``FOR UPDATE``), ``COPY``, ``SET``, transaction control and
     sqlglot's catch-all ``Command`` node are refused as
     ``run_sql_non_select``.

  2. **Forbidden functions and name-resolving types** (PR #2886's rules
     4-6, issues #2406 / #2585). The deny-list is checked on every function
     node of the tree (bare or schema-qualified) AND on every identifier
     token in the code, so a spelling the tree hides still refuses. It
     covers functions that execute SQL text (``ts_stat``, ``ts_rewrite``,
     the ``*_to_xml`` families, ``dblink*``) and the server-state name
     classes. Every type named ``reg*`` or ``pg_*`` is refused wherever a
     type appears (cast, typed literal, array, column definition list), as
     are the same casts written as a call (``regclass('x')``, ``reg*in``,
     ``reg*recv``) or a field (``x.regclass``), every ``to_reg*`` call,
     and ``pg_input_is_valid`` / ``pg_input_error_info`` /
     ``pg_get_object_address``. When the text names a deny-listed function
     the refusal names that function, argument list and schema stripped.

  3. **FROM items.** A FROM item is a relation, a subquery, VALUES, or an
     allow-listed row generator; ``ROWS FROM`` passes only when every
     function inside it is allow-listed.

  4. **Relation allow-list.** Every named relation must be ``okg.v_*``
     (explicit schema, no database part; ASCII letters fold, nothing else
     does) or a bare reference to a CTE that PostgreSQL's scoping rules make
     visible at that point. A write target never takes CTE scope. Quoted,
     unquoted and ``U&"..."`` spellings reduce to the same canonical name
     first.

The MCP ``run_sql`` tool ALSO runs the query under ``SET LOCAL ROLE
app_ro`` and a per-query ``statement_timeout`` -- defence in depth, not
relied on: the role has SELECT on the ``okg.v_*`` views only.
"""

from __future__ import annotations

from typing import Any, Optional

from okg.substrate.errors import SubstrateError


__all__ = [
    "RunSqlInvalid",
    "RunSqlNonSelect",
    "RunSqlMultiStatement",
    "RunSqlDisallowedRelation",
    "RunSqlForbiddenFunction",
    "checked_run_sql",
    "ProbeQueryRefused",
    "PROBE_QUERY_REFUSED",
    "validate_run_sql",
    "validate_probe_query",
]


# ---- error classes ----


class RunSqlInvalid(SubstrateError):
    """Base for every run_sql validation failure. Subclasses
    refine the ``code``."""
    code = "run_sql_invalid"


class RunSqlNonSelect(RunSqlInvalid):
    code = "run_sql_non_select"


class RunSqlMultiStatement(RunSqlInvalid):
    code = "run_sql_multi_statement"


class RunSqlDisallowedRelation(RunSqlInvalid):
    code = "run_sql_disallowed_relation"


class RunSqlForbiddenFunction(RunSqlInvalid):
    code = "run_sql_forbidden_function"


#: Named refusal for an external-backed ``sql`` probe whose query is not
#: exactly one plain, read-only SELECT (review of #2401). The probe adapter
#: runs the handle's query on the MCP read pool; a second statement could end
#: the read-only transaction and write as that pool's role.
PROBE_QUERY_REFUSED = "external_probe_query_refused"


class ProbeQueryRefused(SubstrateError):
    """An external probe query that is not one plain read-only SELECT.

    ``data["reason"]`` names the rule: ``empty``, ``parse_error``,
    ``non_select``, ``multi_statement``, ``data_modifying``,
    ``select_into``, ``locking_clause`` or ``forbidden_function``.
    """

    code = PROBE_QUERY_REFUSED


# ---- forbidden-function deny-list ----


# PostgreSQL built-in functions the substrate refuses however they are
# spelled (bare, schema-qualified, quoted, Unicode-escaped). The first block
# is the list the pglast guard carried; the second adds functions that run a
# SQL string (and so escape the relation allow-list), write, signal, or read
# the server's files.
_FORBIDDEN_FUNCTIONS: frozenset[str] = frozenset({
    "pg_sleep",
    "pg_sleep_for",
    "pg_sleep_until",
    "pg_read_file",
    "pg_read_binary_file",
    "pg_ls_dir",
    "pg_advisory_lock",
    "pg_advisory_xact_lock",
    "pg_terminate_backend",
    "pg_cancel_backend",
    "lo_import",
    "lo_export",
    "dblink",
    # `current_setting` / `set_config` are how an attacker would
    # rewrite the pin GUC mid-query. The substrate's `okg.current_pin`
    # helper is the safe accessor exposed to the agent.
    "current_setting",
    "set_config",
    # Added with the sqlglot guard.
    "nextval",
    "setval",
    "pg_notify",
    "pg_stat_file",
    "pg_reload_conf",
    "pg_rotate_logfile",
    "pg_switch_wal",
    "pg_promote",
    "pg_export_snapshot",
    "pg_import_system_collations",
    "pg_log_backend_memory_contexts",
    "pg_backup_start",
    "pg_backup_stop",
    "ts_stat",
    "ts_rewrite",
    "txid_current",
    "pg_current_xact_id",
    # Large-object reads and writes whose names lack the `lo_` prefix.
    "loread",
    "lowrite",
    # Catalog, settings and maintenance functions (third review).
    "pg_show_all_settings",
    "pg_get_viewdef",
    "pg_get_functiondef",
    "pg_relation_filepath",
    "gin_clean_pending_list",
})

# Every function whose name starts with one of these is refused too.
_FORBIDDEN_FUNCTION_PREFIXES: tuple[str, ...] = (
    "pg_advisory_",
    "pg_try_advisory_",
    "pg_read_",
    "pg_ls_",
    "pg_file_",
    "pg_stat_reset",
    "pg_create_",
    "pg_drop_replication",
    "pg_replication_",
    "pg_logical_",
    "lo_",
    "dblink",
    "query_to_xml",
    "cursor_to_xml",
    "table_to_xml",
    "schema_to_xml",
    "database_to_xml",
    "pg_stat_get_",
    "pg_copy_",
    "brin_summarize_",
    "brin_desummarize_",
    # Server-state name classes (PR #2886, rule 6): other sessions' activity
    # and settings, auth files, server configuration and control data,
    # replication, backup and WAL control, logging, index maintenance and
    # transaction-id assignment. Broader spellings of some prefixes above.
    "pg_stat_get",
    "pg_show",
    "pg_hba",
    "pg_ident",
    "pg_config",
    "pg_control_",
    "pg_replication",
    "pg_logical",
    "pg_backup",
    "pg_promote",
    "pg_log_",
    "pg_import",
    "pg_export_snapshot",
    "brin_summarize",
    "brin_desummarize",
    "gin_clean",
    "txid_",
    "pg_current_xact_id",
)

# Functions that execute SQL text (PR #2886, rule 5): each runs a query, or
# reads a table, named by a string argument, so the relation allow-list never
# sees what it reads. Core PostgreSQL's callers of the query executor that
# take caller text are ts_stat, ts_rewrite (its query form) and the *_to_xml /
# *_to_xmlschema families; dblink is the extension one.
_SQL_TEXT_EXECUTORS: frozenset[str] = frozenset({"ts_stat", "ts_rewrite"})
_SQL_TEXT_EXECUTOR_PREFIXES: tuple[str, ...] = (
    "query_to_xml",
    "cursor_to_xml",
    "table_to_xml",
    "schema_to_xml",
    "database_to_xml",
    "dblink",
)

# Text turned into a catalog reference (PR #2886, rule 4): the reg* OID-alias
# types, called as functions (``regclass('x')``) or through their input
# functions (``regprocin``, ``regclassin``, the binary ``*recv``), plus the
# functions that resolve a type or object named by text.
_REG_TYPES: frozenset[str] = frozenset({
    "regproc", "regprocedure", "regoper", "regoperator", "regclass",
    "regtype", "regrole", "regnamespace", "regconfig", "regdictionary",
    "regcollation",
})
_NAME_RESOLVER_FUNCTIONS: frozenset[str] = (
    _REG_TYPES
    | frozenset(t + "in" for t in _REG_TYPES)
    | frozenset(t + "recv" for t in _REG_TYPES)
    | frozenset({
        "text_regclass",
        "pg_input_is_valid",
        "pg_input_error_info",
        "pg_get_object_address",
    })
)
_NAME_RESOLVER_PREFIXES: tuple[str, ...] = ("to_reg",)

#: Resolvers whose string argument names a function; the refusal names that
#: function when it is deny-listed.
_PROC_NAME_RESOLVERS: frozenset[str] = frozenset({
    "to_regproc", "to_regprocedure", "regproc", "regprocedure",
    "regprocin", "regprocedurein",
})

#: Type-name prefixes whose input function resolves a string to a catalog
#: object: the ``reg*`` OID-alias types, and the ``pg_*`` catalog row types
#: (``'(...)'::pg_proc`` and ``json_populate_record(NULL::pg_type, ...)``
#: parse ``reg*`` fields from text). Every type so named is refused, in any
#: position: a cast, a typed literal, an array element type or a column
#: definition list.
_NAME_RESOLVING_TYPE_PREFIXES: tuple[str, ...] = ("reg", "pg_")

_REGPROC_TYPES = frozenset({"regproc", "regprocedure"})

# Words a query may open with (``TABLE x`` is a query too, but sqlglot cannot
# parse it, so it is refused later as a parse error).
_QUERY_KEYWORDS = frozenset({"select", "with", "values", "table"})

# sqlglot node classes a read-only query can never contain.
_REFUSED_NODE_NAMES: tuple[str, ...] = (
    "DML",
    "DDL",
    "Insert",
    "Update",
    "Delete",
    "Merge",
    "Copy",
    "Create",
    "Drop",
    "Alter",
    "Command",
    "Into",
    "Lock",
    "Set",
    "Transaction",
    "Commit",
    "Rollback",
    "Grant",
    "Revoke",
    "Use",
    "Pragma",
    "Describe",
    "Show",
    "Kill",
    "Cache",
    "Uncache",
    "Refresh",
    "LoadData",
    "Analyze",
    "TruncateTable",
    "Comment",
    "Execute",
    "Declare",
    "Returning",
    "OnConflict",
)


_ASCII_UPPER = str.maketrans(
    "ABCDEFGHIJKLMNOPQRSTUVWXYZ", "abcdefghijklmnopqrstuvwxyz"
)


def _ascii_fold(name: str) -> str:
    """Fold ASCII letters only, so no non-ASCII spelling folds onto `okg`."""
    return name.translate(_ASCII_UPPER)


def _function_rule(name: str) -> Optional[str]:
    """The rule that refuses function ``name``, or ``None`` when none does.

    ``sql_text_executor`` and ``name_resolver`` are PR #2886's named rules;
    ``call`` is the deny-list and its name prefixes.
    """
    if name in _SQL_TEXT_EXECUTORS or name.startswith(_SQL_TEXT_EXECUTOR_PREFIXES):
        return "sql_text_executor"
    if name in _NAME_RESOLVER_FUNCTIONS or name.startswith(_NAME_RESOLVER_PREFIXES):
        return "name_resolver"
    if (
        name in _FORBIDDEN_FUNCTIONS
        or name.startswith(_FORBIDDEN_FUNCTION_PREFIXES)
        or name.endswith("_replication_slot")
    ):
        return "call"
    return None


def _is_forbidden_function(name: str) -> bool:
    return _function_rule(name) is not None


def _refused_node_types() -> tuple[type, ...]:
    """The sqlglot classes named in ``_REFUSED_NODE_NAMES``.

    Fails loudly if sqlglot no longer defines one of them: a silent skip
    would let a sqlglot rename shrink the refused set without anyone
    noticing. The guard then refuses every query until the list is fixed.
    """
    from sqlglot import exp

    missing = [
        name for name in _REFUSED_NODE_NAMES
        if not isinstance(getattr(exp, name, None), type)
    ]
    if missing:
        raise RuntimeError(
            "sqlglot no longer defines refused node classes "
            f"{missing}; update _REFUSED_NODE_NAMES in sql_safety.py"
        )
    return tuple(getattr(exp, name) for name in _REFUSED_NODE_NAMES)


def _top_level_is_query(stmt: Any) -> bool:
    """SELECT (with or without WITH), a set operation, or VALUES."""
    from sqlglot import exp

    node = stmt
    while isinstance(node, (exp.Subquery, exp.Paren)):
        node = node.this
    return isinstance(node, (exp.Select, exp.SetOperation, exp.Values))


def _function_name_in_text(text: str) -> Optional[str]:
    """Reduce a ``regproc`` / ``regprocedure`` input string to the bare,
    lower-cased function name: ``' pg_catalog."pg_sleep" (int) '`` ->
    ``"pg_sleep"``. The argument list, schema and quotes are stripped.
    Lower-casing a quoted name only adds refusals, never removes one."""
    head = text.split("(", 1)[0].strip()
    if not head:
        return None
    name = head.rsplit(".", 1)[-1].strip().strip('"').strip()
    return _ascii_fold(name) or None


def _type_name(data_type: Any) -> str:
    """The lower-cased last name of a sqlglot type: schema, quotes, array
    brackets and modifiers stripped (``pg_catalog."pg_type"[]`` ->
    ``pg_type``). A type sqlglot cannot render comes back as
    ``"<unreadable type>"``, which the caller refuses."""
    try:
        text = data_type.sql(dialect="postgres")
    except Exception:  # noqa: BLE001 - an unreadable type refuses
        return "<unreadable type>"
    head = text.split("(", 1)[0].split("[", 1)[0].strip()
    name = head.rsplit(".", 1)[-1].strip().strip('"').strip()
    return _ascii_fold(name)


def _string_argument(node: Any) -> Optional[str]:
    """The text of a function call's first argument when it is a string
    literal, else ``None``."""
    from sqlglot import exp

    args = list(node.expressions or ())
    if not args and node.this is not None and not isinstance(node, exp.Anonymous):
        args = [node.this]
    first = args[0] if args else None
    if isinstance(first, exp.Literal) and first.is_string:
        return str(first.this)
    return None


def _reference_refusal(node: Any) -> Optional[tuple[str, str]]:
    """Classify one tree node against the text-to-reference rule (PR #2886,
    rule 4). Returns ``(function, cast_kind)`` when the node is refused,
    else ``None``.

    * A cast to ``regproc`` / ``regprocedure``: ``function`` is the bare
      function name its string names (``"<dynamic>"`` for a non-literal).
    * A resolver call (``to_regprocedure('pg_sleep(int)')``) whose string
      names a deny-listed function: ``function`` is that function.
    * Any type named ``reg*`` or ``pg_*``, wherever a type appears (cast,
      typed literal, array element type, column definition list).
    * A field whose last name is a ``reg*`` type (``('x'::text).regclass``,
      ``t.regclass``), which PostgreSQL reads as a cast.

    Resolver calls that name no deny-listed function are refused by the
    function-name rule (``name_resolver``) instead.
    """
    from sqlglot import exp

    from okg.substrate.postgres_sql import function_names, identifier_name

    if isinstance(node, (exp.Cast, exp.TryCast)):
        to = node.args.get("to")
        if to is not None and _type_name(to) in _REGPROC_TYPES:
            arg = node.this
            if isinstance(arg, exp.Literal) and arg.is_string:
                target = _function_name_in_text(str(arg.this))
                return (target or "<empty>", "regproc")
            return ("<dynamic>", "regproc")
        return None
    if isinstance(node, exp.Func):
        names = {_ascii_fold(n) for n in function_names(node)}
        if names & _PROC_NAME_RESOLVERS:
            text = _string_argument(node)
            target = _function_name_in_text(text) if text else None
            if target and _is_forbidden_function(target):
                return (target, sorted(names & _PROC_NAME_RESOLVERS)[0])
        return None
    if isinstance(node, exp.DataType):
        type_name = _type_name(node)
        if (
            type_name.startswith(_NAME_RESOLVING_TYPE_PREFIXES)
            or type_name == "<unreadable type>"
        ):
            return (type_name, "name_resolving_type")
        return None
    field = None
    if isinstance(node, exp.Dot):
        field = node.expression
    elif isinstance(node, exp.Column):
        field = node.this
    if isinstance(field, exp.Identifier):
        field_name = _ascii_fold(identifier_name(field))
        if field_name in _REG_TYPES:
            return (field_name, "field_cast")
    return None


def _next_code_token(tokens: tuple[Any, ...], index: int) -> Any:
    from okg.substrate.postgres_sql import COMMENT_KINDS

    for token in tokens[index + 1:]:
        if token.kind not in COMMENT_KINDS:
            return token
    return None


def _operator_schema_refusal(tokens: tuple[Any, ...]) -> Optional[str]:
    """Check every ``OPERATOR(...)`` by PostgreSQL's lexical rules.

    Returns the text inside the parentheses of the first one that is not
    exactly ``pg_catalog.<operator>``, else ``None``. The schema comes from
    the lexer token, so its quoting counts: a quoted schema must be exactly
    ``pg_catalog``, and an unquoted one folds ASCII letters. No schema, a
    multi-part name, or anything else inside the parentheses is refused.
    """
    from okg.substrate.postgres_sql import (
        COMMENT_KINDS,
        OPERATOR,
        QIDENT,
        UIDENT,
        WORD,
    )

    code = [token for token in tokens if token.kind not in COMMENT_KINDS]
    for index, token in enumerate(code):
        if not (token.kind == WORD and _ascii_fold(token.value) == "operator"):
            continue
        if index + 1 >= len(code) or code[index + 1].text != "(":
            continue
        inner: list[Any] = []
        closed = False
        for inside in code[index + 2:]:
            if inside.text == ")":
                closed = True
                break
            inner.append(inside)
        text = " ".join(t.text for t in inner)
        if not closed or len(inner) != 3:
            return text or "<empty>"
        schema_token, dot, operator = inner
        if dot.text != "." or operator.kind != OPERATOR:
            return text
        if schema_token.kind == WORD:
            schema = _ascii_fold(schema_token.value)
        elif schema_token.kind in (QIDENT, UIDENT):
            schema = schema_token.value
        else:
            return text
        if schema != "pg_catalog":
            return text
    return None


_RELATION_FIX_HINT = (
    "the agent-facing surface is okg.v_* views — "
    "use okg.v_nodes / okg.v_edges / "
    "okg.v_documents / okg.v_doc_chunks / "
    "okg.v_chunk_sequence / okg.v_table_cells / "
    "okg.v_subtypes / okg.v_aliases / "
    "okg.v_source_runs / "
    "okg.v_source_run_diagnostics / "
    "okg.v_admission_envelopes / "
    "okg.v_admission_events / "
    "okg.v_admission_record_refs / okg.v_runtime_queue_health / "
    "okg.v_mcp_calls; for schema introspection use "
    "inspect(target={'kind':'view','name':'v_nodes'}) "
    "for columns and "
    "inspect(target={'kind':'subtype','subtype':<name>}) "
    "for observed attr keys"
)


# ---- public entry point ----


def checked_run_sql(sql: Optional[str]) -> str:
    """Check ``sql`` against the ``run_sql`` safety contract and return the
    exact statement text that was checked.

    Raises a ``RunSqlInvalid`` subclass on any violation. The returned text
    is what the caller must send to the database: ``U&`` spellings already
    decoded by the same rules the checks used, outer comments and ``;``
    trimmed. Executing anything else (the caller's raw text) would reopen
    every difference between this module's decoding and PostgreSQL's.

    Validation order:
      1. Type / emptiness check.
      2. Parse (`okg.substrate.postgres_sql.parse`, strict); any error or
         parser disagreement becomes ``RunSqlInvalid``. A backslash in a
         plain ``'...'`` string is accepted (issue #3666): run_sql sets
         ``standard_conforming_strings = on`` in the transaction that runs
         the text, where PostgreSQL reads the backslash as an ordinary
         character, as the checks here do.
      3. Statement-type check on the first statement.
      4. Single-statement check.
      5. Refused-node walk (writable CTE, INTO, locks, Command, ...).
      6. Forbidden-function walk (tree, identifier tokens, reg* / pg_* types).
      7. FROM-item allow-list (relations, subqueries, VALUES, a few
         set-returning functions).
      8. Relation allow-list walk.
    """
    if sql is None:
        raise RunSqlInvalid(
            "run_sql received None for `sql`",
            data={"sql": None},
            fix_hint="pass a SELECT / WITH SQL string",
        )
    if not isinstance(sql, str):
        raise RunSqlInvalid(
            f"run_sql expected str, got {type(sql).__name__}",
            data={"sql_type": type(sql).__name__},
        )
    if not sql.strip():
        raise RunSqlInvalid(
            "run_sql received empty SQL",
            data={"sql": sql},
            fix_hint="submit a SELECT / WITH query",
        )

    from okg.substrate.postgres_sql import (
        COMMENT_KINDS,
        IDENTIFIER_KINDS,
        QIDENT,
        SEMICOLON,
        UIDENT,
        WORD,
        lex,
        PostgresSqlError,
        canonical_identifier,
        function_names,
        iter_nodes,
        parse,
        statement_text,
    )

    # ``first_token`` is the agent-friendly tag -- the leading SQL keyword
    # that triggered the rejection -- kept for the regex-era error envelope.
    first_token = (sql.split(None, 1) or [""])[0].upper()

    def _non_select(reason: str, node_type: str) -> RunSqlNonSelect:
        return RunSqlNonSelect(
            f"run_sql refuses statements other than SELECT / WITH ({reason})",
            data={
                "first_token": first_token,
                "stmt_type": node_type,
                "sql_prefix": sql[:120],
            },
            fix_hint=(
                "rewrite as a SELECT against okg.v_* views; writes "
                "/ DDL go through the substrate CLI, not run_sql"
            ),
        )

    # ---- (2a) leading keyword, by PostgreSQL's lexer rules ----
    # A statement that does not open with SELECT / WITH / VALUES / "(" is not
    # a query whatever follows; refuse it before sqlglot, which parses many
    # utility statements only as an opaque Command.
    try:
        leading = next(
            (token for token in lex(sql) if token.kind not in COMMENT_KINDS),
            None,
        )
    except PostgresSqlError as e:
        raise RunSqlInvalid(
            f"run_sql parse error: {e}",
            data={"sql_prefix": sql[:120], "parse_error": str(e)},
            fix_hint="fix the SQL syntax and retry",
        ) from e
    except Exception as e:  # noqa: BLE001 - an internal failure refuses too
        raise RunSqlInvalid(
            f"run_sql parse error: {type(e).__name__}",
            data={"sql_prefix": sql[:120], "parse_error": type(e).__name__},
            fix_hint="fix the SQL syntax and retry",
        ) from e
    if leading is not None and not (
        (leading.kind == WORD and leading.value.lower() in _QUERY_KEYWORDS)
        or leading.text == "("
        or leading.kind == SEMICOLON
    ):
        raise _non_select("leading keyword", leading.text.upper())

    # ---- (2b) parse ----
    try:
        parsed = parse(sql, strict=True, standard_strings=True)
    except PostgresSqlError as e:
        raise RunSqlInvalid(
            f"run_sql parse error: {e}",
            data={
                "sql_prefix": sql[:120],
                "parse_error": str(e),
            },
            fix_hint="fix the SQL syntax and retry",
        ) from e
    except RecursionError as e:
        raise RunSqlInvalid(
            "run_sql parse error: the query is nested too deeply",
            data={"sql_prefix": sql[:120]},
            fix_hint="simplify the query",
        ) from e
    except Exception as e:  # noqa: BLE001 - an internal failure refuses too
        raise RunSqlInvalid(
            f"run_sql parse error: {type(e).__name__}",
            data={"sql_prefix": sql[:120], "parse_error": type(e).__name__},
            fix_hint="fix the SQL syntax and retry",
        ) from e

    statements = parsed.statements
    if not statements:
        raise RunSqlInvalid(
            "run_sql received only-comments SQL",
            data={"sql": sql},
            fix_hint="submit a SELECT / WITH query",
        )

    stmt = statements[0]

    # ---- (3) statement-type check ----
    # When the *first* statement is not a query, this refusal fires before
    # the multi-statement count (``DELETE ...; SELECT 1`` is
    # run_sql_non_select, not run_sql_multi_statement).
    if not _top_level_is_query(stmt):
        raise _non_select("top-level statement", type(stmt).__name__)

    # ---- (4) single-statement check ----
    if len(statements) > 1:
        raise RunSqlMultiStatement(
            "run_sql refuses multi-statement queries",
            data={
                "n_statements": len(statements),
                "sql_prefix": sql[:120],
            },
            fix_hint="submit one SELECT / WITH per call",
        )

    from sqlglot import exp

    nodes = list(iter_nodes(stmt))

    # ---- (5) refused-node walk ----
    refused = _refused_node_types()
    for node in nodes:
        if isinstance(node, refused):
            raise _non_select(
                f"{type(node).__name__} inside the query", type(node).__name__
            )

    # ---- (6) forbidden-function walk ----
    def _forbidden(name: str, **extra: Any) -> RunSqlForbiddenFunction:
        return RunSqlForbiddenFunction(
            f"run_sql refuses function {name!r}",
            data={"function": name, **extra},
            fix_hint=(
                "this function is on the substrate's "
                "deny-list for safety / determinism reasons"
            ),
        )

    for node in nodes:
        reference = _reference_refusal(node)
        if reference is not None:
            target, cast_kind = reference
            raise RunSqlForbiddenFunction(
                f"run_sql refuses {cast_kind} naming {target!r}",
                data={"function": target, "cast_kind": cast_kind},
                fix_hint=(
                    "reg* and pg_* types, to_reg* and the other resolvers "
                    "turn a string into a catalog reference and are refused"
                ),
            )
        if isinstance(node, exp.Operator):
            # OPERATOR(schema.op) runs whatever function that operator is
            # bound to; only the built-in pg_catalog operators are admitted.
            operator_text = str(node.args.get("operator") or "")
            schema_text = operator_text.rpartition(".")[0].strip()
            if schema_text.startswith('"') and schema_text.endswith('"') and len(schema_text) > 1:
                schema = schema_text[1:-1].replace('""', '"')
            else:
                schema = _ascii_fold(schema_text)
            if schema != "pg_catalog":
                raise _forbidden(f"OPERATOR({operator_text})")
        for name in sorted({_ascii_fold(n) for n in function_names(node)}):
            rule = _function_rule(name)
            if rule is not None:
                raise _forbidden(name, cast_kind=rule)

    # Token pass: a deny-listed name anywhere in the code refuses, so a
    # spelling the tree does not model still cannot reach the database. A
    # word spelled as a reg* type refuses wherever it stands (a cast, a
    # typed literal, a field, a column definition list).
    for index, token in enumerate(parsed.tokens):
        if token.kind not in IDENTIFIER_KINDS:
            continue
        name = canonical_identifier(
            token.value, quoted=token.kind in {QIDENT, UIDENT}
        )
        # Compare ASCII-folded, quoted or not: `"PG_SLEEP"(1)` names no real
        # function, but the pglast guard refused it and so does this one.
        name = _ascii_fold(name)
        following = _next_code_token(parsed.tokens, index)
        called = following is not None and following.text == "("
        if (
            name in _FORBIDDEN_FUNCTIONS
            or name in _REG_TYPES
            or (called and _is_forbidden_function(name))
        ):
            raise _forbidden(name, detected_by="token")

    # OPERATOR(schema.op), read from the lexer's tokens: sqlglot's operator
    # text drops the quotes, and PostgreSQL resolves `"PG_CATALOG"` to a
    # schema literally named PG_CATALOG, not pg_catalog.
    bad_operator = _operator_schema_refusal(parsed.tokens)
    if bad_operator is not None:
        raise _forbidden(f"OPERATOR({bad_operator})", detected_by="token")

    # ---- (7) FROM-item allow-list ----
    # A FROM item is a named relation, a subquery, VALUES, or one of a few
    # set-returning functions (ROWS FROM of those too). Anything else --
    # another function, XMLTABLE, JSON_TABLE, a construct sqlglot adds
    # later -- is refused.
    for node in nodes:
        if isinstance(node, (exp.From, exp.Join)):
            _check_from_item(node.this, _forbidden)
    # TABLESAMPLE is refused whatever its method: the method is a function
    # PostgreSQL looks up by name (tsm_handler), and the pglast guard refused
    # every RangeTableSample. Read from the tree anywhere it appears and from
    # the tokens, so a sample sqlglot left out of the tree still refuses.
    for node in nodes:
        if isinstance(node, exp.TableSample):
            raise _forbidden("tablesample", cast_kind="from_item", position="from")
    for token in parsed.tokens:
        if token.kind == WORD and _ascii_fold(token.value) == "tablesample":
            raise _forbidden(
                "tablesample", cast_kind="from_item", position="from",
                detected_by="token",
            )

    # ---- (8) relation allow-list walk ----
    _check_relation_allow_list(nodes)

    return statement_text(parsed)


def _is_write_target(table: Any) -> bool:
    """True when ``table`` is the target of an INSERT / UPDATE / DELETE /
    MERGE (possibly under an INSERT column list). PostgreSQL resolves a
    write target through the search path, never to a CTE."""
    from sqlglot import exp

    child, parent = table, table.parent
    if isinstance(parent, exp.Schema) and child.arg_key == "this":
        child, parent = parent, parent.parent
    return (
        isinstance(parent, (exp.Insert, exp.Update, exp.Delete, exp.Merge))
        and child.arg_key == "this"
    )


def _check_relation_allow_list(nodes: Any) -> None:
    """Refuse every named relation that is neither ``okg.v_*`` nor a bare
    name resolving to a CTE visible at that point (PR #2886, rules 2-3).

    Runs on its own too: a write target never takes CTE scope here, so the
    allow-list stays sound even if the refused-node walk before it did not
    refuse the write.
    """
    from sqlglot import exp

    from okg.substrate.postgres_sql import cte_names_visible_at, table_name_parts

    for node in nodes:
        if not isinstance(node, exp.Table):
            continue
        parts = table_name_parts(node)
        if parts is None:
            # A table function; step (7) admitted it by name.
            continue
        catalog, schemaname, relname = parts

        # CTE shadowing: a bare reference to a CTE PostgreSQL can see from
        # here is allowed even if the name matches a physical table.
        if (
            catalog is None
            and schemaname is None
            and not _is_write_target(node)
            and relname in cte_names_visible_at(node)
        ):
            continue

        # Names compare case-folded, as the pglast guard did: `"OKG"."V_NODES"`
        # passes here. Only okg.v_nodes / okg.v_edges can route anywhere under
        # such a spelling -- the graph rewrite matches them the same way and
        # replaces them -- and on the Postgres path PostgreSQL resolves the
        # exact spelling, which app_ro cannot create.
        allowed = (
            catalog is None
            and _ascii_fold(schemaname) == "okg"
            and _ascii_fold(relname).startswith("v_")
        ) if schemaname is not None else False
        if not allowed:
            canonical = ".".join(
                part for part in (catalog, schemaname, relname) if part
            ).lower()
            raise RunSqlDisallowedRelation(
                f"run_sql refuses reference to {canonical!r}",
                data={"relation": canonical},
                fix_hint=_RELATION_FIX_HINT,
            )


def validate_run_sql(sql: Optional[str]) -> None:
    """Raise a ``RunSqlInvalid`` subclass if ``sql`` violates the
    ``run_sql`` safety contract; return ``None`` when it passes.

    Callers that go on to execute the SQL use `checked_run_sql`, which
    returns the exact text that was checked.
    """
    checked_run_sql(sql)
    return None


# Set-returning functions a FROM item may call: the row generators PR #2886
# admits (rule 7), none of which reads the catalog or runs caller text. Any
# other FROM function, XMLTABLE, JSON_TABLE or an unknown FROM item is
# refused. Add a name here only with a test.
_ALLOWED_FROM_FUNCTIONS = frozenset({
    "generate_series",
    "generate_subscripts",
    "unnest",
    "jsonb_array_elements",
    "jsonb_array_elements_text",
    "jsonb_each",
    "jsonb_each_text",
    "jsonb_object_keys",
    "jsonb_to_record",
    "jsonb_to_recordset",
    "jsonb_path_query",
    "json_array_elements",
    "json_array_elements_text",
    "json_each",
    "json_each_text",
    "json_object_keys",
    "json_to_record",
    "json_to_recordset",
    "regexp_matches",
    "regexp_split_to_table",
    "string_to_table",
})

# sqlglot's own class names for some of those functions.
_SQLGLOT_FROM_FUNCTION_NAMES = {
    "exploding_generate_series": "generate_series",
    "generate_series": "generate_series",
    "unnest": "unnest",
}


def _check_from_item(item: Any, forbidden: Any) -> None:
    from sqlglot import exp

    from okg.substrate.postgres_sql import function_names, identifier_name

    if isinstance(item, exp.Lateral):
        item = item.this
    if isinstance(item, (exp.Subquery, exp.Values, exp.Unnest)):
        return
    if isinstance(item, exp.Table) and item.args.get("rows_from"):
        # ROWS FROM (f(...), g(...)): admitted when every function inside is
        # an allow-listed row generator (PR #2886's rule). Each entry goes
        # through this same check; anything else in the item refuses.
        if item.this is not None or item.args.get("db") is not None or item.args.get(
            "catalog"
        ) is not None:
            raise forbidden("<rows from>", position="from")
        for entry in item.args["rows_from"]:
            _check_from_item(entry, forbidden)
        return
    if isinstance(item, exp.Table):
        this = item.this
        if isinstance(this, exp.Identifier):
            return
        if item.args.get("catalog") is not None:
            raise forbidden("<qualified table function>")
        schema = item.args.get("db")
        if schema is not None and identifier_name(schema) != "pg_catalog":
            raise forbidden(f"{identifier_name(schema)}.<table function>")
        item = this
    if isinstance(item, exp.Func):
        names = function_names(item)
        mapped = {
            _SQLGLOT_FROM_FUNCTION_NAMES.get(name, name) for name in names
        }
        if len(mapped) == 1 and mapped <= _ALLOWED_FROM_FUNCTIONS:
            return
        raise forbidden(
            sorted(mapped)[0] if mapped else type(item).__name__.lower(),
            position="from",
        )
    raise forbidden(
        type(item).__name__.lower() if item is not None else "<from item>",
        position="from",
    )


# ---- external probe queries ----
#
# Carried over from dev commit cbce86fc16 (PACT probe-cache-mcp-grant, review
# of #2401), re-implemented on the sqlglot front end: same function name,
# refusal code, reason names and order of checks; the parse is
# `okg.substrate.postgres_sql.parse(strict=True)` instead of pglast.


#: The ``run_sql`` deny-list less the sleep family. ``run_sql`` refuses sleeps
#: for determinism; a probe runs under its own declared ``statement_timeout``,
#: which is the bound that cuts a slow probe (and is tested with a sleep), so a
#: sleep there is not an escape. Everything that reads files, rewrites
#: settings, takes advisory locks, signals backends or leaves the database
#: stays refused.
_PROBE_ALLOWED_SLEEPS: frozenset[str] = frozenset({
    "pg_sleep", "pg_sleep_for", "pg_sleep_until",
})
_PROBE_FORBIDDEN_FUNCTIONS: frozenset[str] = (
    _FORBIDDEN_FUNCTIONS - _PROBE_ALLOWED_SLEEPS
)


def _is_probe_forbidden_function(name: str) -> bool:
    return name not in _PROBE_ALLOWED_SLEEPS and _is_forbidden_function(name)


# Words that open a PostgreSQL statement other than a query. A probe that
# sqlglot cannot read but that opens with one of these is reported as
# ``non_select`` (what it is), not ``parse_error``.
_NON_QUERY_KEYWORDS = frozenset({
    "abort", "alter", "analyze", "begin", "call", "checkpoint", "close",
    "cluster", "comment", "commit", "copy", "create", "deallocate", "declare",
    "delete", "discard", "do", "drop", "end", "execute", "explain", "fetch",
    "grant", "import", "insert", "listen", "load", "lock", "merge", "move",
    "notify", "prepare", "reassign", "refresh", "reindex", "release", "reset",
    "revoke", "rollback", "savepoint", "security", "set", "show", "start",
    "truncate", "unlisten", "update", "vacuum",
})


def _probe_refusal(reason: str, message: str, **data: object) -> ProbeQueryRefused:
    return ProbeQueryRefused(
        message,
        data={"reason": reason, **data},
        fix_hint=(
            "write the probe query as one plain SELECT: no second statement, "
            "no data-modifying WITH, no SELECT INTO, no FOR UPDATE, no "
            "deny-listed function"
        ),
    )


def validate_probe_query(sql: Optional[str]) -> None:
    """Raise :class:`ProbeQueryRefused` unless ``sql`` is one plain SELECT.

    The external-backed ``sql`` probe adapter runs a query that comes from a
    node's handle, on the MCP read pool, inside ``SET TRANSACTION READ ONLY``.
    That transaction mode is not a boundary on its own: ``COMMIT; INSERT ...``
    ends it. So before anything runs, the query must be exactly one query
    statement (by PostgreSQL's lexical rules and sqlglot's parse, which must
    agree) that

    * contains no INSERT / UPDATE / DELETE / MERGE anywhere, including in a
      ``WITH`` clause;
    * has no ``INTO`` target (``SELECT ... INTO`` creates a table);
    * takes no row locks (``FOR UPDATE`` / ``FOR SHARE``);
    * calls no function on the ``run_sql`` deny-list (less the sleep family,
      which the probe's own statement timeout bounds), however spelled, and
      no ``regproc`` / ``regprocedure`` cast.

    Unlike :func:`checked_run_sql` it does not restrict which relations the
    query reads: a probe reads whatever its deployment's handle names, under
    the pool role's own privileges. The adapter also runs the query as a
    prepared statement, so PostgreSQL refuses a second statement on its own.
    """
    from sqlglot import exp

    from okg.substrate.postgres_sql import (
        COMMENT_KINDS,
        IDENTIFIER_KINDS,
        QIDENT,
        UIDENT,
        WORD,
        canonical_identifier,
        function_names,
        iter_nodes,
        lex,
        parse,
        statement_count,
    )

    if not isinstance(sql, str) or not sql.strip():
        raise _probe_refusal("empty", "the probe query is empty")
    # Any failure while reading the query -- a lexer refusal, a parser
    # disagreement, or an internal error in either -- refuses as parse_error,
    # as dev's pglast version did. Only the exception's type is reported and
    # nothing is chained, so the refusal never quotes the handle's query.
    try:
        tokens = lex(sql)
    except Exception as exc:  # noqa: BLE001 - every failure refuses
        raise _probe_refusal(
            "parse_error",
            f"the probe query does not parse ({type(exc).__name__})",
        ) from None
    n_statements = statement_count(tokens)
    if n_statements == 0:
        raise _probe_refusal("empty", "the probe query has no statement")
    if n_statements > 1:
        raise _probe_refusal(
            "multi_statement",
            f"the probe query holds {n_statements} statements; one is allowed",
            n_statements=n_statements,
        )
    leading = next(t for t in tokens if t.kind not in COMMENT_KINDS)
    leading_word = leading.value.lower() if leading.kind == WORD else ""
    if leading_word in _NON_QUERY_KEYWORDS:
        raise _probe_refusal(
            "non_select",
            f"the probe query is a {leading_word.upper()} statement, not a SELECT",
            stmt_type=leading_word.upper(),
        )
    try:
        parsed = parse(sql, strict=True)
    except Exception as exc:  # noqa: BLE001 - every failure refuses
        raise _probe_refusal(
            "parse_error",
            f"the probe query does not parse ({type(exc).__name__})",
        ) from None
    if not parsed.statements:
        raise _probe_refusal("empty", "the probe query has no statement")
    if len(parsed.statements) > 1:
        raise _probe_refusal(
            "multi_statement",
            f"the probe query holds {len(parsed.statements)} statements; one is allowed",
            n_statements=len(parsed.statements),
        )
    stmt = parsed.statements[0]
    if not _top_level_is_query(stmt):
        raise _probe_refusal(
            "non_select",
            f"the probe query is a {type(stmt).__name__}, not a SELECT",
            stmt_type=type(stmt).__name__,
        )
    writes = (exp.Insert, exp.Update, exp.Delete, exp.Merge)
    refused = _refused_node_types()
    for node in iter_nodes(stmt):
        if isinstance(node, writes):
            raise _probe_refusal(
                "data_modifying",
                f"the probe query contains a {type(node).__name__}",
                stmt_type=type(node).__name__,
            )
        if isinstance(node, exp.Into):
            raise _probe_refusal(
                "select_into", "the probe query selects INTO a table",
            )
        if isinstance(node, exp.Lock):
            raise _probe_refusal(
                "locking_clause", "the probe query takes row locks",
            )
        if isinstance(node, refused):
            raise _probe_refusal(
                "non_select",
                f"the probe query contains a {type(node).__name__}",
                stmt_type=type(node).__name__,
            )
        reference = _reference_refusal(node)
        if reference is not None:
            target, cast_kind = reference
            raise _probe_refusal(
                "forbidden_function",
                f"the probe query turns text into a catalog reference "
                f"({cast_kind} naming {target!r})",
                function=target,
                cast_kind=cast_kind,
            )
        for name in sorted({_ascii_fold(n) for n in function_names(node)}):
            if _is_probe_forbidden_function(name):
                raise _probe_refusal(
                    "forbidden_function",
                    f"the probe query calls deny-listed function {name!r}",
                    function=name,
                )
    # Token pass, as in checked_run_sql: a deny-listed name anywhere in the
    # code refuses, whatever the tree made of it.
    for index, token in enumerate(parsed.tokens):
        if token.kind not in IDENTIFIER_KINDS:
            continue
        name = _ascii_fold(
            canonical_identifier(token.value, quoted=token.kind in {QIDENT, UIDENT})
        )
        following = _next_code_token(parsed.tokens, index)
        called = following is not None and following.text == "("
        if (
            name in _PROBE_FORBIDDEN_FUNCTIONS
            or name in _REG_TYPES
            or (called and _is_probe_forbidden_function(name))
        ):
            raise _probe_refusal(
                "forbidden_function",
                f"the probe query calls deny-listed function {name!r}",
                function=name,
            )
    # OPERATOR(schema.op), by the lexer's tokens, as in checked_run_sql: an
    # operator outside pg_catalog runs whatever function it is bound to.
    bad_operator = _operator_schema_refusal(parsed.tokens)
    if bad_operator is not None:
        raise _probe_refusal(
            "forbidden_function",
            f"the probe query uses OPERATOR({bad_operator}); only pg_catalog "
            "operators are allowed",
            function=f"OPERATOR({bad_operator})",
        )
