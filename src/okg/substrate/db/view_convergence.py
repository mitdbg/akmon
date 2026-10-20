"""Converge stored views that ``CREATE OR REPLACE VIEW`` cannot rewrite.

``views.sql`` is re-executed wholesale on every migrate — that is the
idempotency contract. The contract carries an unstated assumption that
PostgreSQL does not honour: ``CREATE OR REPLACE VIEW`` may only APPEND
output columns. It cannot rename, reorder, retype, or drop a column the
stored view already has, and it raises ``InvalidTableDefinition``
("cannot change name of view column ...") instead. That aborts the whole
multi-statement script, and with it the caller's migrate.

Editing a view's column list mid-list is therefore an upgrade-path
break for every database that stored the older shape, even though a
fresh install is fine. It has happened at least five times in this
repo; four of those were patched one view at a time with a hand-written
``DROP VIEW IF EXISTS`` line above the ``CREATE``, which is a per-view
workaround rather than a mechanism, so the fifth shipped unguarded.

This module is that mechanism, and it is deliberately reactive:

1. Run the view scripts inside a savepoint. A database that installs
   cleanly — every converged deployment, and every fresh install — is
   done here and pays nothing: no probe, no extra lock, no extra DDL.
2. Only when PostgreSQL refuses with ``InvalidTableDefinition``, roll
   back to the savepoint and drop the views the scripts themselves
   declare, then re-run the scripts so every one is recreated.
3. Re-run once. A second failure is raised, never retried in a loop.

PostgreSQL is the oracle for step 2, not a hand-derived column list.
That is the point: a parser over the scripts' SELECT lists would have
to re-implement PostgreSQL's own naming rules, and every place it
guessed wrong would be a silent divergence. Asking the server "does
this replace work?" cannot guess wrong, and it covers renames,
reorders, type changes, and dropped columns alike.

The drops are restrictive — never ``CASCADE``. Views the scripts own
are peeled in dependency order (each pass drops whatever no longer has
a dependant). Anything still blocked after the peel has a dependant the
scripts do NOT own — an operator's view, a materialized view, a
deployment-local object — and the run refuses loudly, naming the view
and its dependants, rather than removing something nobody asked to
remove.

Safety property worth stating plainly: this path is entered only after
the install has ALREADY failed. It can turn a failing migrate into a
succeeding one, or into a failing one with a better message. It cannot
turn a succeeding migrate into a failing one.

Dropping a view drops its grants, column-level privileges, and comments.
The migration caller reapplies the shared relation-grant registry after the
complete substrate-plus-deployment view install and refuses the transaction if
any registered grant remains missing. Operator-issued column privileges and
comments are outside that registry. The recovery logs the recreated relations
at info level and returns their names, which the caller surfaces in its result.
"""

from __future__ import annotations

import logging
import re
from collections.abc import Callable, Iterable, Sequence

import psycopg
from psycopg import sql as pgsql

from okg.substrate.errors import SubstrateError

_LOG = logging.getLogger(__name__)

#: A view declaration at the start of a line. The line anchor is what
#: keeps commented-out and quoted occurrences out of the set: every real
#: declaration in ``views.sql`` starts its own line, while prose about
#: the hazard lives behind a leading ``--``. ``CREATE MATERIALIZED
#: VIEW`` does not match (MATERIALIZED sits between the two keywords),
#: which is intended — a materialized view holds rows and is never
#: dropped for a column-shape mismatch.
_VIEW_DECLARATION = re.compile(
    r"^[ \t]*CREATE[ \t]+(?:OR[ \t]+REPLACE[ \t]+)?VIEW[ \t]+"
    r"(?P<schema>[A-Za-z_][A-Za-z0-9_$]*)\.(?P<name>[A-Za-z_][A-Za-z0-9_$]*)",
    re.IGNORECASE | re.MULTILINE,
)

#: Views declared without a schema qualifier. They are not droppable
#: without resolving ``search_path``, so they are reported rather than
#: guessed at.
_UNQUALIFIED_VIEW_DECLARATION = re.compile(
    r"^[ \t]*CREATE[ \t]+(?:OR[ \t]+REPLACE[ \t]+)?VIEW[ \t]+"
    r"(?![A-Za-z_][A-Za-z0-9_$]*\.)"
    r"(?P<name>[A-Za-z_][A-Za-z0-9_$]*)",
    re.IGNORECASE | re.MULTILINE,
)

#: Dependants of a relation, via the rewrite rules that implement views.
#: Used only to explain a refusal; the authoritative "you may not drop
#: this" comes from PostgreSQL itself.
_DEPENDANTS_SQL = """
SELECT DISTINCT
       dependent_ns.nspname AS schema_name,
       dependent.relname    AS relation_name,
       dependent.relkind    AS relation_kind
  FROM pg_depend
  JOIN pg_rewrite    ON pg_depend.objid = pg_rewrite.oid
  JOIN pg_class    dependent    ON pg_rewrite.ev_class = dependent.oid
  JOIN pg_class    source       ON pg_depend.refobjid = source.oid
  JOIN pg_namespace dependent_ns ON dependent.relnamespace = dependent_ns.oid
  JOIN pg_namespace source_ns    ON source.relnamespace = source_ns.oid
 WHERE source_ns.nspname = %s
   AND source.relname = %s
   AND dependent.oid <> source.oid
 ORDER BY 1, 2
"""

_RELATION_KIND_LABELS = {
    "v": "view",
    "m": "materialized view",
    "r": "table",
    "p": "partitioned table",
    "f": "foreign table",
}


def declared_views(scripts: Iterable[str]) -> list[tuple[str, str]]:
    """The schema-qualified views the given SQL scripts create.

    Order follows the scripts, de-duplicated on first sight. A view
    declared without a schema qualifier is skipped here and surfaced by
    :func:`unqualified_view_declarations` so a refusal can say so
    instead of dropping the wrong relation.
    """
    seen: dict[tuple[str, str], None] = {}
    for script in scripts:
        for match in _VIEW_DECLARATION.finditer(script):
            seen.setdefault(
                (match.group("schema").lower(), match.group("name").lower())
            )
    return list(seen)


def unqualified_view_declarations(scripts: Iterable[str]) -> list[str]:
    """View names the scripts create without naming a schema."""
    names: dict[str, None] = {}
    for script in scripts:
        for match in _UNQUALIFIED_VIEW_DECLARATION.finditer(script):
            names.setdefault(match.group("name").lower())
    return list(names)


#: PostgreSQL's wording for every "CREATE OR REPLACE VIEW may only
#: append" variant: renamed column, changed type, changed collation
#: ("... view column ..."), and dropped column ("cannot drop columns
#: from view").
_FROZEN_VIEW_SHAPE_PHRASES = ("view column", "columns from view")


def is_unreplaceable_view_error(exc: BaseException) -> bool:
    """Is this the failure a stored view's frozen column shape causes?

    SQLSTATE 42P16 alone is too broad to key on. PostgreSQL also uses it
    for unrelated definition errors ("multiple primary keys for table
    ..."), and callers route on this predicate to decide whether an error
    is theirs to diagnose — `migrate` hands anything true here to the
    convergence pass INSTEAD of wrapping it with the deployment file that
    caused it. Keying on the message family keeps a deployment author's
    broken `CREATE TABLE` pointing at their file.

    Erring narrow is the safe direction: a 42P16 this predicate rejects
    is reported loudly with its own diagnosis, which is exactly the
    behavior before the convergence pass existed. A future PostgreSQL
    wording outside this family would cost the automatic recovery, not
    correctness.
    """
    if not isinstance(exc, psycopg.errors.InvalidTableDefinition):
        return False
    message = str(exc).lower()
    return any(phrase in message for phrase in _FROZEN_VIEW_SHAPE_PHRASES)


def install_with_view_convergence(
    conn: psycopg.Connection,
    *,
    run: Callable[[], None],
    scripts: Sequence[str],
) -> list[str]:
    """Run ``run()``; on a frozen-view-shape failure, converge and retry.

    ``run`` executes the view scripts (it owns its own cursor and any
    caller-specific error wrapping). ``scripts`` is the SQL text of
    exactly those scripts, and defines which views this function is
    allowed to drop.

    Returns the views it dropped — empty on the ordinary path, which is
    every fresh install and every already-converged database.

    Raises ``SubstrateError`` when a blocking view has a dependant the
    scripts do not own, or when the retry fails.
    """
    _ensure_open_transaction(conn)
    try:
        with conn.transaction():
            run()
        return []
    except Exception as exc:  # noqa: BLE001 — re-raised unless it is the frozen-shape failure.
        if not is_unreplaceable_view_error(exc):
            raise
        install_failure = exc

    declared = declared_views(scripts)
    if not declared:
        raise SubstrateError(
            "view installation failed because a stored view's column shape "
            "cannot be reached by CREATE OR REPLACE VIEW, and no view "
            f"declarations were found in the scripts to converge: {install_failure}",
            data={"postgres_error": str(install_failure)},
            fix_hint=(
                "the SQL scripts must declare their views as "
                "`CREATE OR REPLACE VIEW <schema>.<name>` at the start of a "
                "line so the convergence pass can find them"
            ),
        ) from install_failure

    dropped, blocked, postgres_detail = _drop_declared_views(conn, declared)
    if blocked:
        raise SubstrateError(
            _refusal_message(blocked, postgres_detail, install_failure),
            data={
                "postgres_error": str(install_failure),
                "dropped_views": dropped,
                "blocked_views": {
                    view: [dependant for dependant, _ in dependants]
                    for view, dependants in blocked.items()
                },
                "postgres_dependency_detail": postgres_detail,
                "unqualified_view_declarations": unqualified_view_declarations(
                    scripts
                ),
            },
            fix_hint=(
                "drop or re-point the named dependants yourself, then re-run "
                "migrate. They are not dropped automatically because they are "
                "not declared by the SQL scripts being installed, and a "
                "CASCADE would remove objects nobody asked to remove."
            ),
        ) from install_failure

    try:
        with conn.transaction():
            run()
    except Exception as retry_failure:
        # The blind spot, named rather than denied: this pass can only
        # drop what `declared_views` recognizes. A declaration it cannot
        # see -- unqualified, split across lines, quoted identifiers --
        # stays stored in its old shape, so the retry fails for the SAME
        # reason as the first attempt while every OTHER view was dropped
        # for nothing. A hint that said "the convergence is not the
        # cause" would be wrong in precisely that case.
        unqualified = unqualified_view_declarations(scripts)
        still_frozen = is_unreplaceable_view_error(retry_failure)
        if still_frozen:
            retry_hint = (
                "the retry hit the SAME frozen-view-shape error, so a view "
                "this pass cannot see is still stored in its old shape. It "
                "only recognizes `CREATE [OR REPLACE] VIEW <schema>.<name>` "
                "at the start of a line; rewrite the declaration that way, "
                "or drop the view by hand and re-run"
            )
            if unqualified:
                retry_hint += (
                    f" (view declarations seen without a schema "
                    f"qualifier, which this pass skipped: {unqualified})"
                )
        else:
            retry_hint = (
                "the retry error is a different failure from the frozen view "
                "shape that triggered convergence; diagnose the retry error "
                "itself"
            )
        raise SubstrateError(
            "view installation still failed after converging "
            f"{len(dropped)} stored view(s) whose column shape CREATE OR "
            f"REPLACE VIEW could not reach: {retry_failure}",
            data={
                "postgres_error": str(install_failure),
                "retry_error": str(retry_failure),
                "retry_error_is_frozen_view_shape": still_frozen,
                "dropped_views": dropped,
                "unqualified_view_declarations": unqualified,
            },
            fix_hint=retry_hint,
        ) from retry_failure

    _LOG.info(
        "migrate: dropped and recreated %d stored view(s) whose column shape "
        "CREATE OR REPLACE VIEW cannot reach (%s). Registered relation grants "
        "are reconciled after the complete view install. Views recreated: %s",
        len(dropped),
        install_failure,
        ", ".join(dropped),
    )
    return dropped


def _ensure_open_transaction(conn: psycopg.Connection) -> None:
    """Make ``conn.transaction()`` issue a SAVEPOINT, not BEGIN/COMMIT.

    psycopg picks between the two by looking at the connection's
    transaction status: idle means "outermost", which ends in a real
    ``COMMIT``. ``migrate()`` runs schema, views, and its ledger row in
    ONE transaction, so an outermost-looking savepoint here would commit
    the views before the ledger row exists and break that contract on
    any caller that happens to arrive idle. Opening the implicit
    transaction first removes the ambiguity.

    An autocommit connection is left alone: there BEGIN/COMMIT around
    the scripts is the correct and intended behavior.

    Caveat, stated because it is the one place the module's "cannot make
    a passing install worse" claim is weaker. Under autocommit each
    ``conn.transaction()`` block commits on its own, so a drop pass that
    then hits the refusal branch leaves the already-dropped views gone
    and durable, and the caller must re-run to restore them. Under a
    caller-owned transaction (both callers in this package) the same
    failure rolls the drops back with everything else. No autocommit
    caller exists today; the tests exercise the transactional path.
    """
    if conn.autocommit:
        return
    if conn.pgconn.transaction_status == psycopg.pq.TransactionStatus.IDLE:
        with conn.cursor() as cur:
            cur.execute("SELECT 1")


def _drop_declared_views(
    conn: psycopg.Connection,
    declared: Sequence[tuple[str, str]],
) -> tuple[list[str], dict[str, list[tuple[str, str]]]]:
    """Restrictively drop the declared views, peeling dependants first.

    Each pass tries every remaining view; a view whose dependants are
    gone drops, and one that is still depended upon is retried next
    pass. Because every dropped view removes at least one dependency
    edge, repeating until a pass makes no progress drops exactly the
    views whose entire dependant closure is script-owned.

    Returns ``(dropped, blocked, postgres_detail)`` where ``blocked``
    maps a still-undroppable view to its enumerated dependants and
    ``postgres_detail`` carries PostgreSQL's own DETAIL line for the
    refusal. The catalog query and the server's own explanation can
    disagree — the server is authoritative about what blocked the drop,
    so a refusal reports both rather than only the query we wrote.
    """
    remaining = [
        (schema, name)
        for schema, name in declared
        if _view_exists(conn, schema, name)
    ]
    dropped: list[str] = []
    postgres_detail: dict[str, str] = {}
    progress = True
    while remaining and progress:
        progress = False
        still_blocked: list[tuple[str, str]] = []
        for schema, name in remaining:
            detail = _try_drop_view(conn, schema, name)
            if detail is None:
                dropped.append(f"{schema}.{name}")
                progress = True
            else:
                postgres_detail[f"{schema}.{name}"] = detail
                still_blocked.append((schema, name))
        remaining = still_blocked

    blocked = {
        f"{schema}.{name}": _dependants(conn, schema, name)
        for schema, name in remaining
    }
    return dropped, blocked, postgres_detail


def _view_exists(conn: psycopg.Connection, schema: str, name: str) -> bool:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT 1 FROM pg_class c "
            "JOIN pg_namespace n ON n.oid = c.relnamespace "
            "WHERE n.nspname = %s AND c.relname = %s AND c.relkind = 'v'",
            (schema, name),
        )
        return cur.fetchone() is not None


def _try_drop_view(
    conn: psycopg.Connection, schema: str, name: str
) -> str | None:
    """Restrictive ``DROP VIEW``. ``None`` on success.

    On a dependant-blocked drop, returns PostgreSQL's own DETAIL line
    ("view X depends on view Y") rather than discarding it — that text
    names the blocker authoritatively, and a refusal that threw it away
    would be asking an operator to re-derive what the server just said.

    The savepoint matters: a failed DROP poisons the surrounding
    transaction, and the surrounding transaction is the caller's whole
    migrate.
    """
    statement = pgsql.SQL("DROP VIEW {}").format(pgsql.Identifier(schema, name))
    try:
        with conn.transaction(), conn.cursor() as cur:
            cur.execute(statement)
    except psycopg.errors.DependentObjectsStillExist as exc:
        return (exc.diag.message_detail or str(exc)).strip()
    return None


def _dependants(
    conn: psycopg.Connection, schema: str, name: str
) -> list[tuple[str, str]]:
    """``(qualified_name, kind_label)`` for every relation reading this view."""
    with conn.cursor() as cur:
        cur.execute(_DEPENDANTS_SQL, (schema, name))
        return [
            (
                f"{row[0]}.{row[1]}",
                _RELATION_KIND_LABELS.get(row[2], f"relkind={row[2]}"),
            )
            for row in cur.fetchall()
        ]


def _refusal_message(
    blocked: dict[str, list[tuple[str, str]]],
    postgres_detail: dict[str, str],
    install_failure: BaseException,
) -> str:
    lines = [
        "view installation failed because a stored view's column shape "
        "cannot be reached by CREATE OR REPLACE VIEW, and the blocking "
        "view(s) cannot be dropped without removing objects the SQL "
        f"scripts do not own: {install_failure}",
    ]
    for view, dependants in sorted(blocked.items()):
        if dependants:
            described = ", ".join(
                f"{dependant} ({kind})" for dependant, kind in dependants
            )
        else:
            described = (
                "no dependants could be enumerated from pg_depend — inspect "
                "the relation manually"
            )
        lines.append(f"  {view} is depended on by: {described}")
        detail = postgres_detail.get(view)
        if detail:
            lines.append(f"    postgres said: {detail}")
    return "\n".join(lines)
