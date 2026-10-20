"""`run_sql` MCP tool — agent escape hatch over the substrate's
view layer.

The agent submits a `SELECT` (or `WITH ... SELECT`) statement
referencing only the `okg.v_*` views; this tool validates,
pin-injects, and executes it under the read-only `app_ro` role.
The result envelope conforms to `add-mcp-search-design-doc`:
`pinned_generation_id`, `returned_count`, `truncated`, plus the
SQL-specific fields `rows`, `columns`, `query_plan_summary`,
`elapsed_ms`. `returned_count` counts the rows in THIS payload;
this tool does not count the caller's matching set, so it emits
no `total_count` (issue #1440).

The four lines of defense (see `sql_safety.py` for layers 1-3
and the role/timeout for layer 4):

  1. Statement-prefix check (SELECT / WITH only).
  2. Single-statement check (one query per call).
  3. Forbidden-function + relation allow-list (`okg.v_*` only).
  4. Run under `app_ro` (no SELECT on physical tables) with a
     bounded `statement_timeout`.

Graph-backed SQL (`okg.v_nodes` / `okg.v_edges`) is answered from a Chronos
checkout instead, as the MCP login role `okg_mcp`, whose reach is wider than
`app_ro`'s. That path runs the user query read-only with the same timeout
(`_enter_graph_read_boundary`); the role that would also limit its reads is
a pending decision (issues #2406, #2585).

Per `add-mcp-run-sql-tool` (2026-04-27).
"""

from __future__ import annotations

import json
import time
from typing import Any, Callable

import psycopg
from psycopg.pq import TransactionStatus

from okg.substrate.errors import SubstrateError
from okg.substrate.mcp import graph_sql_splice
from okg.substrate.mcp.session import Session, assert_session_matches
from okg.substrate.mcp.sql_safety import (
    RunSqlInvalid,
    checked_run_sql,
)


__all__ = ["RunSqlTimeout", "run_sql"]


# Hard ceiling on row_cap to keep response sizes bounded
# regardless of agent-supplied values.
_ROW_CAP_CEILING = 10_000

# Hard ceiling on timeout_ms to keep the substrate's
# connection pool from accumulating long-running queries.
_TIMEOUT_MS_CEILING = 60_000

_PAYLOAD_BYTES_CEILING = 262_144
_CELL_STRING_CEILING = 8_192
_HEAVY_COLUMN_MARKERS = ("embedding", "vector", "blob")
_GRAPH_QUERY_SAVEPOINT = "okg_run_sql_graph_query"
_OPERATIONAL_QUERY_SAVEPOINT = "okg_run_sql_operational_query"


class RunSqlTimeout(SubstrateError):
    """Postgres reported `query_canceled` after exceeding the
    per-query `statement_timeout`. The agent gets a structured
    error so it can react (back off, retry with a tighter
    filter, etc.) rather than a transport-level failure.
    """

    code = "run_sql_timeout"


def _payload_bytes(payload: dict[str, Any]) -> int:
    return len(json.dumps(payload, sort_keys=True, default=str).encode())


def _is_heavy_column(column: str) -> bool:
    lowered = column.lower()
    return any(marker in lowered for marker in _HEAVY_COLUMN_MARKERS)


def _shape_query_rows(
    columns: list[str],
    rows: list[list[Any]],
) -> tuple[list[list[Any]], list[str], int]:
    omitted_columns = [column for column in columns if _is_heavy_column(column)]
    omitted = set(omitted_columns)
    truncated_cells = 0
    shaped: list[list[Any]] = []
    for row in rows:
        out: list[Any] = []
        for column, value in zip(columns, row, strict=False):
            if column in omitted and value is not None:
                out.append("[omitted:heavy_column]")
                continue
            if isinstance(value, str) and len(value) > _CELL_STRING_CEILING:
                out.append(value[: _CELL_STRING_CEILING - 3] + "...")
                truncated_cells += 1
                continue
            out.append(value)
        shaped.append(out)
    return shaped, omitted_columns, truncated_cells


def _bound_query_payload(payload: dict[str, Any]) -> dict[str, Any]:
    columns = [str(column) for column in payload.get("columns") or []]
    rows = [list(row) for row in payload.get("rows") or []]
    rows, omitted_columns, truncated_cells = _shape_query_rows(columns, rows)
    payload["rows"] = rows
    if omitted_columns:
        payload["omitted_columns"] = omitted_columns
        payload["projection_warning"] = (
            "embedding/vector/blob-like columns are omitted from query results; "
            "use structured operators or a narrower projection for inspectable fields"
        )
    if truncated_cells:
        payload["truncated_cells"] = truncated_cells
        payload["truncation_warning"] = (
            f"string cells over {_CELL_STRING_CEILING} characters are shortened"
        )

    payload["payload_byte_cap"] = _PAYLOAD_BYTES_CEILING
    omitted_rows = 0
    size = _payload_bytes(payload)
    while rows and size > _PAYLOAD_BYTES_CEILING:
        rows.pop()
        omitted_rows += 1
        payload["rows"] = rows
        payload["truncated"] = True
        payload["payload_truncated"] = True
        payload["payload_rows_omitted"] = omitted_rows
        size = _payload_bytes(payload)
    payload["payload_bytes"] = size
    return payload


def _execute_one_statement(
    conn: psycopg.Connection, cur: psycopg.Cursor, text: str
) -> None:
    """Execute ``text`` so PostgreSQL itself refuses a second statement.

    A plain ``cur.execute(text)`` with no parameters uses the simple query
    protocol, which runs every statement in the string. In pipeline mode
    psycopg always uses the extended protocol with an unnamed statement, and
    PostgreSQL rejects more than one command there ("cannot insert multiple
    commands into a prepared statement"). That holds even if the parser front
    end ever miscounted statements; the statement-count check stays too.
    Unnamed statements keep this safe behind transaction-pooling proxies.
    Results stay on ``cur`` after the pipeline block syncs.
    """
    with conn.pipeline():
        cur.execute(text)


def _is_query_canceled(exc: BaseException) -> bool:
    stack: list[BaseException] = [exc]
    seen: set[int] = set()
    while stack:
        current = stack.pop()
        if id(current) in seen:
            continue
        seen.add(id(current))
        if isinstance(current, psycopg.errors.QueryCanceled):
            return True
        if current.__cause__ is not None:
            stack.append(current.__cause__)
        if current.__context__ is not None:
            stack.append(current.__context__)
    return False


def _set_local_statement_timeout(cur: psycopg.Cursor, timeout_ms: int) -> None:
    cur.execute(
        "SELECT set_config('statement_timeout', %s, true)",
        (f"{int(timeout_ms)}ms",),
    )


def _graph_reader_for_read_only_query(graph: Any) -> Any:
    """Renew the history read lease now, and return what runs the query.

    A leased checkout (`LeasedGenerationSession`) renews its lease -- a WRITE
    to `okg.history_generation_pins` -- at the start of `query()` once the
    lease is old enough. The user query below runs in a read-only
    transaction, where that write would fail, so the renewal happens here,
    before read-only is set, and the read relations spliced into the query
    (issue #2899) come from the inner session, which does not renew again.
    A plain checkout (no lease) has nothing to renew.

    Where the renewal lands depends on the path: the caller-owned path keeps
    it in the caller's transaction, while the owned path discards it when
    its transaction rolls back (as it did before read-only was added), so
    there only the in-process lease clock resets. run_sql checks out a fresh
    lease per call, so a renewal is not reached from run_sql today.
    """
    prepare = getattr(graph, "session_for_read_only_query", None)
    if prepare is None:
        return graph
    return prepare()


def _enter_graph_read_boundary(cur: psycopg.Cursor) -> None:
    """Constrain the transaction (or savepoint) that runs caller SQL.

    This is the ONE place the graph path of run_sql limits what the user's
    spliced query may do. Today it makes the query read-only:
    `SET LOCAL transaction_read_only = on` is what `SET TRANSACTION READ
    ONLY` sets, and inside a savepoint `ROLLBACK TO SAVEPOINT` undoes it.

    It does NOT limit what the query can READ: the graph path still runs as
    the MCP pool's login role `okg_mcp`, which can read tables such as
    `okg.mcp_call_log` and the Nomos identity and decision tables, so the
    SQL checker is the only guard on reads here. Which role closes that gap
    -- a new narrow graph-read role, or shrinking `okg_mcp` itself -- is a
    pending decision for the operator (issues #2406, #2585). When it is
    made, its `SET LOCAL ROLE <role>` goes here, after read-only.

    It also sets `standard_conforming_strings = on` for the transaction
    (or savepoint) that runs the caller's text: the checker, the splice and
    PostgreSQL then all read a backslash in `'...'` as an ordinary
    character, so a string literal ends where the checks say it ends
    (issue #3666; `_set_standard_strings`).
    """
    cur.execute("SET LOCAL transaction_read_only = on")
    _set_standard_strings(cur)


def _set_standard_strings(cur: psycopg.Cursor) -> None:
    """Make PostgreSQL read `'...'` the way the SQL checks read it.

    `checked_run_sql` accepts a backslash in a plain `'...'` string (a regex
    such as `'\\d+'`), reading it as an ordinary character. PostgreSQL does
    the same only while `standard_conforming_strings` is on; with it off, a
    backslash escapes the next character and a string could end later than
    the checks saw, hiding code inside what they judged a literal. Setting
    it here, LOCAL to the transaction that runs the caller's text, makes the
    two readings the same whatever the connection's own setting is (a
    pooled connection may carry another session's setting). The caller's
    statement cannot change it: `set_config` is refused by the checks, and
    a statement's text is read before it runs.
    """
    cur.execute("SET LOCAL standard_conforming_strings = on")


def _run_graph_query_in_owned_transaction(
    conn: psycopg.Connection,
    graph: Any,
    read: Callable[[Any], list[dict[str, Any]]],
    *,
    timeout_ms: int,
    prior_autocommit: bool,
) -> list[dict[str, Any]]:
    """Renew the lease, go read-only, then run ``read(reader)``; always roll back.

    ``read`` receives what `_graph_reader_for_read_only_query` returned and
    must do all its database work through it or on ``conn`` (run_sql's
    ``read`` reads the checkout's relations and runs the spliced text).
    """
    try:
        if prior_autocommit:
            conn.autocommit = False
        reader = _graph_reader_for_read_only_query(graph)
        with conn.cursor() as cur:
            _enter_graph_read_boundary(cur)
            _set_local_statement_timeout(cur, timeout_ms)
        return read(reader)
    finally:
        if not conn.closed:
            try:
                conn.rollback()
            except Exception:  # noqa: BLE001 - best-effort cleanup
                pass
        if prior_autocommit and not conn.closed:
            conn.autocommit = prior_autocommit


def _run_graph_query_in_caller_transaction(
    conn: psycopg.Connection,
    graph: Any,
    read: Callable[[Any], list[dict[str, Any]]],
    *,
    timeout_ms: int,
) -> list[dict[str, Any]]:
    """Renew the lease, then run ``read(reader)`` read-only inside a savepoint."""
    # The lease renewal is a write the caller's transaction keeps, so it runs
    # before the savepoint; everything inside the savepoint is rolled back.
    reader = _graph_reader_for_read_only_query(graph)
    with conn.cursor() as cur:
        cur.execute(f"SAVEPOINT {_GRAPH_QUERY_SAVEPOINT}")
    try:
        with conn.cursor() as cur:
            _enter_graph_read_boundary(cur)
            _set_local_statement_timeout(cur, timeout_ms)
        return read(reader)
    finally:
        # ALWAYS roll back to the savepoint, on success too. A RELEASE would
        # keep the savepoint's SET LOCAL settings (read-only, the timeout,
        # and later a role) in force for the rest of the caller's
        # transaction. The query only reads, so there is nothing to keep.
        # The RELEASE after the rollback only drops the now-empty savepoint.
        if not conn.closed:
            with conn.cursor() as cur:
                cur.execute(f"ROLLBACK TO SAVEPOINT {_GRAPH_QUERY_SAVEPOINT}")
                cur.execute(f"RELEASE SAVEPOINT {_GRAPH_QUERY_SAVEPOINT}")


def _read_spliced_graph_sql(
    conn: psycopg.Connection,
    reader: Any,
    plan: graph_sql_splice.GraphSqlPlan,
    *,
    row_cap: int,
    executed: dict[str, str],
) -> list[dict[str, Any]]:
    """Splice the checkout's read subqueries into the caller's text and run it.

    ``reader`` is the checkout AFTER any lease renewal (the inner session of a
    leased checkout), so reading its relations writes nothing and is safe
    under read-only. The subqueries and branch point are the same ones
    ``graph.query`` would use; they are spliced into the caller's checked text
    at parse positions, never re-rendered (issue #2899).
    """
    relations, params = reader.read_relations()
    spliced = graph_sql_splice.render_graph_sql(
        plan, relations, params, row_cap=row_cap
    )
    graph_sql_splice.verify_splice(plan, spliced)
    executed["sql"] = spliced.sql
    return graph_sql_splice.execute_spliced_sql(conn, spliced.sql)


def run_sql(
    conn: psycopg.Connection,
    session: Session,
    sql: str,
    *,
    row_cap: int = 1_000,
    timeout_ms: int = 5_000,
) -> dict[str, Any]:
    """Execute `sql` against the agent-facing view layer and
    return a decision-shaped envelope.

    `conn` is the substrate's pooled connection (logged in as
    the substrate's privileged role); this function opens a
    transaction inside it, sets `LOCAL ROLE app_ro` so the
    SQL can't escape the view layer, sets the pin GUC from
    `session.generation_id`, and runs the query.

    On success returns:

      {
        "rows": [[...], ...],      # row_cap-bounded
        "columns": ["col", ...],
        "returned_count": int,     # rows in THIS payload, after cap + budget
        "truncated": bool,         # more rows exist past the cap/budget
        "pinned_generation_id": int | str,
        "query_plan_summary": str, # one-line EXPLAIN
        "elapsed_ms": int,
      }

    There is no `total_count`: `sql` carries its own OFFSET and this function
    never counts the matching set, so any total it reported would be a
    page-local number wearing a set-sized name (issue #1440). Callers that
    need a real total run their own bounded COUNT over the same predicate --
    see `set_operators._bounded_total`.

    Raises:
      - `RunSqlInvalid` (or subclass) if the SQL fails the
        safety guard. The agent gets `code` + `message` +
        optional `fix_hint`.
      - `RunSqlTimeout` if the query exceeds `timeout_ms`.

    Cooperates with the logical MCP session boundary in `server.py`:
    callers pass an open substrate Session whose exact pin has already been
    resolved (from an explicit override, the transport-session default, or an
    honest request-scoped fallback), and we honor it without negotiation.
    """
    entry_transaction_status = conn.info.transaction_status
    if entry_transaction_status == TransactionStatus.INERROR:
        raise RunSqlInvalid(
            "run_sql cannot run inside a failed transaction",
            data={"transaction_status": "INERROR"},
            fix_hint="rollback the failed transaction before calling run_sql",
        )

    assert_session_matches(conn, session)

    # ---- defense layers 1-3 ----
    # From here on `sql` is the exact text the checks judged (U& spellings
    # decoded, outer comments and `;` trimmed). Every later step -- graph
    # splice, EXPLAIN, execution, error envelopes -- uses it, never the
    # caller's raw string. The operational path executes this text as-is; the
    # graph path executes it with only relation names spliced and a row-limit
    # wrapper, never re-rendered (#2899, fixed by #2906; see graph_sql_splice).
    raw_sql = sql
    sql = checked_run_sql(sql)

    from okg.substrate.graph_store import ChronosGraphStore

    # Graph-backed SQL (okg.v_nodes/okg.v_edges, directly or through a
    # composite okg.v_* deployment view) is answered from a Chronos checkout.
    # The executed text is built by splicing into the checked text at parse
    # positions, never by re-rendering it (issue #2899, fixed by #2906); see
    # graph_sql_splice. Operational okg.v_* views over source_progress/jobs/etc.
    # stay on the app_ro/Postgres path below with the checked text as-is.
    graph_plan = graph_sql_splice.plan_graph_sql(sql, conn)
    if graph_plan.routes_to_graph:
        # The checked text has U&'...' strings already decoded to plain
        # '...', so the caller's raw text is checked too: #2906 refuses the
        # U&'...' form on this path, and a refusal check on the raw text only
        # adds refusals. Nothing below executes or splices the raw text.
        try:
            graph_sql_splice.refuse_escape_dependent_literals(raw_sql)
        except graph_sql_splice.RunSqlGraphLiteralRefused as refused:
            # Issue #3666: an E'...' string whose only escapes are a doubled
            # backslash or a backslash-quote gets the exact retry, the same
            # query with the string written as $$...$$.
            rewritten = graph_sql_splice.dollar_quoted_rewrite(raw_sql)
            if rewritten is not None:
                retry_arguments: dict[str, Any] = {
                    "sql": rewritten,
                    "generation_id": session.generation_id,
                }
                if row_cap != 1_000:
                    retry_arguments["row_cap"] = row_cap
                if timeout_ms != 5_000:
                    retry_arguments["timeout_ms"] = timeout_ms
                refused.retry_call = {
                    "tool": "query", "mode": "corrected",
                    "arguments": retry_arguments,
                }
            raise
        graph_sql_splice.refuse_escape_dependent_literals(sql)
        graph_sql_splice.refuse_unsupported_forms(graph_plan)
        graph_sql_splice.assert_standard_conforming_strings(conn)
        from okg.substrate.nomos import deployment_nomos_enforcement_mode

        # Tri-state: enforce REFUSES graph-backed SQL (arbitrary SQL cannot be
        # source-lineage filtered after joins/aggregates); audit records the
        # bypass observation and ALLOWS (log-and-allow); off skips entirely.
        enforcement_mode = deployment_nomos_enforcement_mode(
            session.deployment_name,
        )
        if enforcement_mode == "enforce":
            raise RunSqlInvalid(
                "run_sql graph-backed SQL is refused while Nomos runtime "
                "enforcement is active because arbitrary SQL cannot be "
                "source-lineage filtered after joins or aggregates",
                data={
                    "deployment_name": session.deployment_name,
                    "generation_id": session.generation_id,
                    "sql": sql,
                },
                fix_hint=(
                    "use inspect/search/expand/filter/map/aggregate operators "
                    "that apply Nomos row visibility, or run this query from an "
                    "admin context with a separate declassification path"
                ),
            )
        if enforcement_mode == "audit":
            _record_run_sql_audit_bypass(conn, session, sql=sql)
        row_cap = max(1, min(int(row_cap), _ROW_CAP_CEILING))
        timeout_ms = max(100, min(int(timeout_ms), _TIMEOUT_MS_CEILING))
        caller_owned_transaction = entry_transaction_status != TransactionStatus.IDLE
        if (
            not caller_owned_transaction
            and not conn.autocommit
            and conn.info.transaction_status != TransactionStatus.IDLE
        ):
            conn.rollback()
        graph = ChronosGraphStore.from_connection(
            conn,
            ensure_schema=False,
            autocommit=False,
        ).checkout_generation(
            session.generation_id,
            branch=session.branch_name,
        )
        prior_autocommit = conn.autocommit
        executed: dict[str, str] = {}

        def _read_checkout(reader: Any) -> list[dict[str, Any]]:
            return _read_spliced_graph_sql(
                conn, reader, graph_plan, row_cap=row_cap, executed=executed
            )

        t0 = time.monotonic()
        try:
            if caller_owned_transaction:
                rows_dict = _run_graph_query_in_caller_transaction(
                    conn,
                    graph,
                    _read_checkout,
                    timeout_ms=timeout_ms,
                )
            else:
                rows_dict = _run_graph_query_in_owned_transaction(
                    conn,
                    graph,
                    _read_checkout,
                    timeout_ms=timeout_ms,
                    prior_autocommit=prior_autocommit,
                )
        except RunSqlInvalid:
            raise
        except Exception as exc:
            if _is_query_canceled(exc):
                raise RunSqlTimeout(
                    f"run_sql exceeded statement_timeout ({timeout_ms}ms)",
                    data={
                        "timeout_ms": timeout_ms,
                        "sql_prefix": sql[:80],
                        "rewritten_sql_prefix": executed.get("sql", "")[:120],
                    },
                    fix_hint=(
                        "tighten the WHERE clause or increase timeout_ms (max 60000)"
                    ),
                ) from exc
            raise RunSqlInvalid(
                f"run_sql plan-time error: {exc}",
                data={"sql": sql, "rewritten_sql": executed.get("sql")},
                fix_hint="check selected columns and allowed okg.v_* view names",
            ) from exc
        elapsed_ms = int((time.monotonic() - t0) * 1000)
        truncated = len(rows_dict) > row_cap
        rows_dict = rows_dict[:row_cap]
        columns = list(rows_dict[0].keys()) if rows_dict else []
        out_rows = [[row.get(column) for column in columns] for row in rows_dict]
        payload = {
            "rows": out_rows,
            "columns": columns,
            "truncated": truncated,
            "pinned_generation_id": session.generation_id,
            "query_plan_summary": "Chronos checkout",
            "elapsed_ms": elapsed_ms,
        }
        payload = _bound_query_payload(payload)
        # `returned_count` replaces the former `total_count` (issue #1440).
        # The submitted SQL carries its own OFFSET, and the row list is capped
        # at `row_cap` and then trimmed again to fit the byte budget, so the
        # only number this function can state truthfully is how many rows it
        # is handing back. Calling that a TOTAL made a page-local, cap-
        # saturated value read as the size of the matching set. Counted after
        # the budget trim so it describes the rows actually in the payload.
        payload["returned_count"] = len(payload["rows"])
        from okg.substrate.mcp.freshness import null_ordering_warning

        ordering = null_ordering_warning(sql, columns, out_rows)
        if ordering:
            payload["ordering_warning"] = ordering
        return payload

    # ---- defense layer 4 ----
    row_cap = max(1, min(int(row_cap), _ROW_CAP_CEILING))
    timeout_ms = max(100, min(int(timeout_ms), _TIMEOUT_MS_CEILING))
    pin = _legacy_pin_for_operational_sql(session)

    # The connection from the pool may be in autocommit mode; turn it
    # off so SET LOCAL is scoped to this query only. psycopg then opens
    # the transaction with its own BEGIN before the first statement below;
    # an explicit "BEGIN" here was a second one inside it, which
    # PostgreSQL ignores with a warning (lane C item 238a). We always roll
    # back at the end (read-only path).
    #
    # When the CALLER already holds a transaction, that rollback must not
    # be the caller's: the old ignored BEGIN made the final ROLLBACK end
    # the caller's whole transaction, discarding its writes. The query
    # then runs in a savepoint instead, like the graph path above, and
    # only the savepoint is rolled back. The SET LOCAL settings below are
    # undone with it.
    prior_autocommit = conn.autocommit
    caller_owned_transaction = (
        entry_transaction_status != TransactionStatus.IDLE
    )

    plan_summary = ""
    elapsed_ms = 0
    columns: list[str] = []
    rows_fetched: list[tuple] = []
    truncated = False

    try:
        if prior_autocommit:
            conn.autocommit = False
        with conn.cursor() as cur:
            if caller_owned_transaction:
                cur.execute(f"SAVEPOINT {_OPERATIONAL_QUERY_SAVEPOINT}")
            try:
                # Defense layer 4 — three Postgres-side
                # constraints, all scoped to this transaction:
                #
                #   (a) `SET TRANSACTION READ ONLY` — the
                #       Postgres engine refuses any write
                #       (INSERT/UPDATE/DELETE/CREATE/DROP)
                #       with SQLSTATE 25006 ('read_only_sql_
                #       transaction'), independent of role
                #       privileges. Must come before any
                #       statement inside the transaction.
                #   (b) `SET LOCAL ROLE app_ro` — even if the
                #       READ ONLY guard is somehow bypassed,
                #       the role lacks SELECT on physical
                #       tables, so the agent can only see
                #       the view layer.
                #   (c) `SET LOCAL statement_timeout` —
                #       runaway queries are killed.
                #
                # SET LOCAL scopes to the transaction; after
                # ROLLBACK, the connection returns to the pool
                # in its prior state.
                cur.execute("SET TRANSACTION READ ONLY")
                cur.execute("SET LOCAL ROLE app_ro")
                # (d) the checks read `'...'` with standard strings; so
                #     does PostgreSQL from here on (issue #3666).
                _set_standard_strings(cur)
                # `SET LOCAL` is a Postgres utility statement
                # and doesn't accept parameter binding for the
                # value. The values come from the substrate
                # (timeout_ms is range-clamped above; pin is derived from
                # session authority), not from agent input -- so inline
                # interpolation is safe.
                cur.execute(f"SET LOCAL statement_timeout = '{timeout_ms}ms'")
                cur.execute(f"SET LOCAL okg.pin_generation = '{pin}'")

                # ---- query plan summary (EXPLAIN) ----
                # Run EXPLAIN before the real query so a
                # plan-time error (e.g. relation not found
                # under app_ro) surfaces here rather than
                # mid-fetch. The summary is the top-level
                # plan node's `Node Type` + total cost.
                try:
                    _execute_one_statement(
                        conn, cur, "EXPLAIN (FORMAT JSON) " + sql,
                    )
                    plan_row = cur.fetchone()
                    plan_summary = _summarize_plan(plan_row)
                except psycopg.errors.QueryCanceled as e:
                    # EXPLAIN itself shouldn't time out, but
                    # be safe.
                    raise RunSqlTimeout(
                        f"run_sql EXPLAIN exceeded {timeout_ms}ms",
                        data={"timeout_ms": timeout_ms},
                    ) from e
                except psycopg.Error as e:
                    # Plan-time error — surface with a
                    # structured envelope rather than a raw
                    # psycopg exception.
                    raise RunSqlInvalid(
                        f"run_sql plan-time error: {type(e).__name__}: {e}",
                        data={
                            "pgcode": getattr(e, "sqlstate", None),
                        },
                        fix_hint=(
                            "check column names against the "
                            "view schema; "
                            "EXPLAIN refused this query"
                        ),
                    ) from e

                # ---- real execution ----
                t0 = time.monotonic()
                try:
                    _execute_one_statement(conn, cur, sql)
                except psycopg.errors.QueryCanceled as e:
                    raise RunSqlTimeout(
                        f"run_sql exceeded statement_timeout ({timeout_ms}ms)",
                        data={
                            "timeout_ms": timeout_ms,
                            "sql_prefix": sql[:80],
                        },
                        fix_hint=(
                            "tighten the WHERE clause or "
                            "increase timeout_ms (max 60000)"
                        ),
                    ) from e
                except psycopg.Error as e:
                    # Run-time error (constraint violation,
                    # type mismatch in operator, etc.). The
                    # plan-time check should have caught most;
                    # this is the catch-all.
                    raise RunSqlInvalid(
                        f"run_sql run-time error: {type(e).__name__}: {e}",
                        data={
                            "pgcode": getattr(e, "sqlstate", None),
                        },
                    ) from e

                # Description gives us column names.
                desc = cur.description or []
                columns = [c.name for c in desc]

                # Fetch row_cap + 1 to detect truncation in
                # one trip. fetchmany returns a list; iterate
                # until we have row_cap+1 or run out.
                rows_fetched = list(cur.fetchmany(row_cap + 1))
                truncated = len(rows_fetched) > row_cap
                if truncated:
                    rows_fetched = rows_fetched[:row_cap]

                elapsed_ms = int(
                    (time.monotonic() - t0) * 1000,
                )
            finally:
                # Always rollback (read-only) — keeps the
                # session GUCs reset and releases any locks.
                if caller_owned_transaction:
                    cur.execute(
                        f"ROLLBACK TO SAVEPOINT {_OPERATIONAL_QUERY_SAVEPOINT}"
                    )
                    cur.execute(
                        f"RELEASE SAVEPOINT {_OPERATIONAL_QUERY_SAVEPOINT}"
                    )
                else:
                    cur.execute("ROLLBACK")
    finally:
        if prior_autocommit:
            conn.autocommit = prior_autocommit

    # No `total_count` here either (issue #1440). The count BEFORE truncation
    # needs a second COUNT(*) over the caller's own SQL, which this tool does
    # not run; the old convention reported `row_cap` when truncated, which is
    # a saturated page size dressed as a set total. `returned_count` below is
    # the one number this function can state truthfully.
    payload = _bound_query_payload(
        {
            "rows": [list(_serialize_row(r)) for r in rows_fetched],
            "columns": columns,
            "truncated": truncated,
            "pinned_generation_id": pin,
            "query_plan_summary": plan_summary,
            "elapsed_ms": elapsed_ms,
        }
    )
    payload["returned_count"] = len(payload["rows"])
    return payload


def _parse_single_statement(sql: str) -> Any:
    """Parse one statement with the shared PostgreSQL front end.

    Raises ``PostgresSqlError`` (or ``ValueError``) when the text is not
    exactly one statement both parsers read the same way.
    """
    from okg.substrate.postgres_sql import parse

    parsed = parse(sql)
    if len(parsed.statements) != 1:
        raise ValueError("expected one SELECT statement")
    return parsed.statements[0]


def _named_relations(stmt: Any):
    """Yield ``(table_node, "schema.relation")`` for every named relation."""
    from sqlglot import exp

    from okg.substrate.postgres_sql import iter_nodes, table_name_parts

    for node in iter_nodes(stmt):
        if not isinstance(node, exp.Table):
            continue
        parts = table_name_parts(node)
        if parts is None or parts[0] is not None:
            continue
        _catalog, schema, relname = parts
        yield node, f"{(schema or '').lower()}.{relname.lower()}"


_GRAPH_VIEW_REWRITES = {
    "okg.v_nodes": "graph_nodes",
    "okg.v_edges": "graph_edges",
}


def _rewrite_graph_views(sql: str) -> str:
    """Rewrite base graph views through parsed relation nodes.

    `validate_run_sql` accepts quoted identifiers after canonicalizing them
    to lower-case relation names, so this rewrite must do the same. A string
    replace misses `"okg"."v_nodes"` and can also rewrite string literals.

    The rewrite splices the new names over the identifier offsets the parser
    reports, so every other byte of the statement is kept as written (after
    `U&` identifiers are decoded).
    """
    from okg.substrate.postgres_sql import parse, splice

    try:
        parsed = parse(sql)
        if len(parsed.statements) != 1:
            raise ValueError("expected one SELECT statement")
        replacements: list[tuple[int, int, str]] = []
        for node, key in _named_relations(parsed.statements[0]):
            physical = _GRAPH_VIEW_REWRITES.get(key)
            if physical is None:
                continue
            for identifier, value in (
                (node.args.get("db"), "okg"),
                (node.this, physical),
            ):
                meta = identifier.meta or {}
                start, end = meta.get("start"), meta.get("end")
                if not isinstance(start, int) or not isinstance(end, int):
                    raise ValueError(f"no source offset for relation {key}")
                replacements.append((start, end + 1, value))
    except Exception as exc:
        raise RunSqlInvalid(
            f"run_sql could not parse graph-view references: {exc}",
            data={"sql": sql},
            fix_hint="simplify the query or reference okg.v_nodes/okg.v_edges directly",
        ) from exc

    if not replacements:
        return sql
    return splice(parsed.text, replacements)


def _record_run_sql_audit_bypass(
    conn: Any,
    session: Session,
    *,
    sql: str,
) -> None:
    """Record (but allow) a graph-backed run_sql under audit enforcement.

    In audit (log-and-allow) mode the structural source-lineage bypass that
    enforce mode refuses is still allowed, but the observation is recorded
    through the active-policy boundary so the audit trail captures that an
    unfilterable graph-backed query proceeded.

    Recording is record-or-fail, identical to the primary read path
    (``read_filter.filter_node_rows_for_session`` →
    ``evaluate_with_active_policy``), which does NOT swallow a persist failure:
    a recording error propagates and aborts the read rather than letting an
    unaudited bypass proceed silently. ``require_active=True`` forces a
    fail-closed deny to be persisted even when no policy bundle is active (with
    ``require_active=False`` ``evaluate_runtime_boundary_with_active_policy``
    returns ``None`` WITHOUT persisting any decision), so the audit trail always
    captures the bypass.

    The Nomos import stays lazy to avoid the circular import, but evaluation and
    persistence errors are no longer swallowed.
    """
    from okg.substrate.nomos import (
        NomosRuntimeRequest,
        evaluate_runtime_boundary_with_active_policy,
    )
    from okg.substrate.nomos.mcp_subject import require_mcp_subject
    from okg.substrate.nomos.read_filter import _assert_multi_user_subject

    # The subject the MCP boundary already resolved for this session (the
    # request principal in multi-user mode). A multi-user session without one
    # is refused rather than answered as the process; otherwise the one
    # resolver every MCP path shares (issue #2343), refused by name when it
    # is unresolved.
    subject = getattr(session, "nomos_subject", None)
    if subject is None:
        _assert_multi_user_subject(session, None)
        subject = require_mcp_subject(
            conn,
            deployment_name=session.deployment_name,
            surface="graph.read.sql_admin",
        )
    evaluate_runtime_boundary_with_active_policy(
        conn,
        deployment_name=session.deployment_name,
        scope=f"deployment:{session.deployment_name}",
        request=NomosRuntimeRequest(
            surface="graph.read.sql_admin",
            subject=subject,
            # Volatile publish coordinates stay OUT of the identity-bearing
            # resource (issue #635: they re-mint the bypass decision once per
            # publish); the audit-only target records them.
            resource={
                "resource_kind": "mcp_graph_read_boundary",
                "use": "run_sql.graph_backed_audit_bypass",
            },
        ),
        target={
            "kind": "run_sql_graph_backed",
            "generation_id": session.generation_id,
            "branch_name": getattr(session, "branch_name", "default"),
            "catalog_version_id": getattr(session, "catalog_version_id", None),
        },
        performed_obligations=("audit",),
        require_active=True,
        enforcement_mode="audit",
    )
    conn.commit()


def _legacy_pin_for_operational_sql(session: Session) -> int:
    """Return the integer pin required by legacy operational SQL views.

    Graph-backed reads never use this path: they execute against a Chronos
    checkout pinned by the session's string generation id. Operational views
    that still consult ``okg.current_pin()`` require a compatibility integer.
    Falling back to ``0`` would silently change the generation authority, so a
    Chronos session without a compat id is rejected.
    """
    try:
        return int(session.generation_id)
    except (TypeError, ValueError):
        pass
    if session.compat_generation_id is not None:
        return int(session.compat_generation_id)
    raise RunSqlInvalid(
        "run_sql operational path cannot derive a legacy integer pin from "
        f"Chronos generation {session.generation_id!r}",
        data={
            "generation_id": session.generation_id,
            "compat_generation_id": session.compat_generation_id,
            "deployment_name": session.deployment_name,
        },
        fix_hint=(
            "query graph-backed okg.v_nodes/okg.v_edges through the Chronos "
            "checkout path, or publish checkpoint metadata with target_generation_id"
        ),
    )


def _references_physical_graph_view(sql: str) -> bool:
    """True when expanded SQL must be answered from a Chronos checkout."""
    try:
        stmt = _parse_single_statement(sql)
    except Exception:
        # validate_run_sql reports parser errors before this helper's result
        # matters. Fail closed for graph routing here rather than routing an
        # operational query because a string literal mentions a graph table.
        return False
    return any(
        key in {"okg.graph_nodes", "okg.graph_edges"}
        for _node, key in _named_relations(stmt)
    )


# Base graph views and their physical Chronos-backed tables. On the str-based
# fast path these are handled by _rewrite_graph_views; inside the composite-view
# expansion they are rewritten through sqlglot WITH alias preservation, because
# pg_get_viewdef emits implicit-alias column references (e.g. `v_nodes.attrs`
# over `FROM okg.v_nodes`) that a bare name swap would strand.
_BASE_VIEW_PHYSICAL = {"okg.v_nodes": "graph_nodes", "okg.v_edges": "graph_edges"}
_BASE_GRAPH_VIEWS = frozenset(_BASE_VIEW_PHYSICAL)

# Bound on recursive expansion of nested composite views (a deployment view
# built on another deployment view). Far above any real view-nesting depth;
# a backstop against a pathological/cyclic view graph.
_MAX_VIEW_EXPANSION_DEPTH = 8


def _references_composite_view(sql: str) -> bool:
    """True if the SQL names an okg.v_* view other than the base v_nodes/v_edges.

    `validate_run_sql` accepts quoted and Unicode-escaped identifiers, so this
    selector uses the same parser view instead of a string pre-check.
    """
    try:
        stmt = _parse_single_statement(sql)
    except Exception:
        # validate_run_sql reports parser errors before this helper matters.
        return False
    return any(
        key.startswith("okg.v_") and key not in _BASE_GRAPH_VIEWS
        for _node, key in _named_relations(stmt)
    )


def _normalize_viewdef_key_casts(definition: str) -> str:
    """Drop redundant ::text casts on literal json-arrow keys.

    pg_get_viewdef renders `payload -> 'timings_ms'` as
    `payload -> 'timings_ms'::text` (the cast binds to the KEY under
    Postgres precedence and is a no-op on a quoted literal). sqlglot
    (<= 30.10.0) parses that as `(payload -> 'timings_ms')::text` and
    re-renders `CAST(payload -> 'timings_ms' AS TEXT)`, which turns a
    jsonb expression into text — v_source_runs then fails at plan time
    with `COALESCE types text and jsonb cannot be matched`
    (fix-source-run-telemetry-reads
    req.telemetry.v-source-runs-readable). Removing the no-op cast
    before the parse keeps the precedence question out of sqlglot.
    """
    import re

    return re.sub(
        r"(->>?\s*'(?:[^']|'')*')::text",
        r"\1",
        definition,
    )


def _parse_composite_view_query(sql: str, sqlglot: Any) -> Any:
    """Parse an already-validated query for sqlglot-based view expansion.

    sqlglot cannot read Unicode-escaped identifiers such as U&"okg", so the
    text is first normalized by `okg.substrate.postgres_sql.normalize`, which
    decodes them by PostgreSQL's rules into ordinary quoted identifiers.
    """
    from okg.substrate.postgres_sql import normalize

    try:
        return sqlglot.parse_one(normalize(sql), read="postgres")
    except Exception as exc:
        raise RunSqlInvalid(
            f"run_sql could not parse the query to inline a deployment "
            f"view: {exc}",
            data={"sql": sql},
            fix_hint=(
                "simplify the query or reference the base node/edge views directly"
            ),
        ) from exc


def _expand_composite_views(sql: str, conn: Any) -> str:
    """Inline composite okg.v_* deployment views as aliased subqueries.

    The Chronos checkout rewrites only literal registered-table references in
    the submitted SQL text and cannot see through a Postgres view, so a
    deployment view over v_nodes/v_edges must be expanded into the query before
    the checkout runs. Each composite-view table reference is replaced by its
    `pg_get_viewdef` wrapped as a subquery that preserves the original alias
    (mirroring Chronos's own table->subquery rewrite). The base v_nodes/v_edges
    and physical graph_nodes/graph_edges are left for _rewrite_graph_views.

    Returns the SQL unchanged when it references no composite view, so the
    str-based fast path is never perturbed.
    """
    if not _references_composite_view(sql):
        return sql

    import sqlglot
    from sqlglot import exp

    viewdef_cache: dict[str, str | None] = {}

    def _table_key(table: "exp.Table") -> str:
        key = f"{table.db}.{table.name}" if table.db else table.name
        return key.lower()

    def _viewdef(key: str) -> str | None:
        if not key.startswith("okg.v_") or key in _BASE_GRAPH_VIEWS:
            return None
        if key in viewdef_cache:
            return viewdef_cache[key]
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
        if definition is not None:
            definition = _normalize_viewdef_key_casts(definition)
        viewdef_cache[key] = definition
        return definition

    for _ in range(_MAX_VIEW_EXPANSION_DEPTH):
        tree = _parse_composite_view_query(sql, sqlglot)

        expanded = False

        def _replace(node: "exp.Expression") -> "exp.Expression":
            nonlocal expanded
            if isinstance(node, exp.Table):
                key = _table_key(node)
                alias = node.args.get("alias") or exp.TableAlias(
                    this=exp.to_identifier(node.name)
                )
                physical = _BASE_VIEW_PHYSICAL.get(key)
                if physical is not None:
                    # okg.v_nodes -> okg.graph_nodes, keeping the original
                    # (possibly implicit) alias so qualified column refs in the
                    # expanded view body still resolve.
                    new_table = exp.Table(
                        this=exp.to_identifier(physical),
                        db=exp.to_identifier("okg"),
                    )
                    new_table.set("alias", alias)
                    expanded = True
                    return new_table
                definition = _viewdef(key)
                if definition is not None:
                    expanded = True
                    return exp.Subquery(
                        this=sqlglot.parse_one(definition, read="postgres"),
                        alias=alias,
                    )
            return node

        tree = tree.transform(_replace)
        if not expanded:
            break
        sql = tree.sql(dialect="postgres")
    else:
        raise RunSqlInvalid(
            "run_sql exceeded the deployment-view expansion depth "
            f"({_MAX_VIEW_EXPANSION_DEPTH}); the view graph may be cyclic",
            data={"sql": sql},
        )

    return sql


def _summarize_plan(plan_row: Any) -> str:
    """Reduce an `EXPLAIN (FORMAT JSON)` row to a one-liner.

    Postgres returns a list-of-dicts in the first column
    (with `FORMAT JSON`). The top-level dict carries `Plan`
    with `Node Type`, `Total Cost`, `Plan Rows`. Format as
    `<Node Type> (cost=<low>..<high> rows=<n>)`.
    """
    if not plan_row:
        return ""
    plan_blob = plan_row[0]
    # psycopg may return list[dict] (json) or str (jsonb text)
    # depending on the Postgres / driver combo. Handle both.
    if isinstance(plan_blob, str):
        plan_blob = json.loads(plan_blob)
    if not isinstance(plan_blob, list) or not plan_blob:
        return ""
    top = plan_blob[0].get("Plan", {})
    node_type = top.get("Node Type", "")
    startup = top.get("Startup Cost", 0)
    total = top.get("Total Cost", 0)
    plan_rows = top.get("Plan Rows", 0)
    return f"{node_type} (cost={startup:.2f}..{total:.2f} rows={plan_rows})"


def _serialize_row(row: Any) -> tuple:
    """Best-effort coercion to JSON-friendly types.

    psycopg returns native Python types for most columns
    (str, int, float, dict, list, datetime, etc.). The MCP
    server's tool wrapper will JSON-serialize the response;
    we coerce datetimes to ISO strings here to avoid the
    server-layer needing a custom encoder.
    """
    from datetime import date, datetime

    out = []
    for v in row:
        if isinstance(v, (datetime, date)):
            out.append(v.isoformat())
        elif isinstance(v, memoryview):
            out.append(bytes(v).hex())
        else:
            out.append(v)
    return tuple(out)
