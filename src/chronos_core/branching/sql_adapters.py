from __future__ import annotations

import sqlite3
from abc import ABC, abstractmethod
from pathlib import Path
import re
from typing import Any, Iterable, Mapping, Sequence
from urllib.parse import unquote, urlparse

import psycopg
from psycopg.conninfo import conninfo_to_dict
from psycopg.pq import TransactionStatus
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from chronos_core.pg_session import (
    idle_in_transaction_option,
    session_options,
)


SQLRow = Mapping[str, Any]


class SQLAdapterError(Exception):
    """Base error raised by SQL database adapters."""


class SQLIntegrityError(SQLAdapterError):
    """Raised for logical integrity violations independent of a DB driver."""


class SQLDatabaseAdapter(ABC):
    """Small portability boundary between branching logic and a SQL database.

    The branching layer intentionally depends on this interface rather than a
    concrete DB-API driver. New database engines should add an adapter here and
    keep branch metadata, interval maintenance, and log replay unchanged.
    """

    dialect: str

    @abstractmethod
    def execute(
        self, sql: str, params: Sequence[Any] | Mapping[str, Any] = ()
    ) -> Any:
        raise NotImplementedError

    @abstractmethod
    def executemany(self, sql: str, params: Iterable[Sequence[Any]]) -> Any:
        raise NotImplementedError

    @abstractmethod
    def commit(self) -> None:
        raise NotImplementedError

    @abstractmethod
    def rollback(self) -> None:
        raise NotImplementedError

    @abstractmethod
    def begin(self) -> None:
        raise NotImplementedError

    @abstractmethod
    def close(self) -> None:
        raise NotImplementedError

    @property
    @abstractmethod
    def in_transaction(self) -> bool:
        raise NotImplementedError

    @property
    @abstractmethod
    def raw_connection(self) -> Any:
        raise NotImplementedError

    @abstractmethod
    def table_defs(self, table: str) -> tuple[tuple[str, ...], tuple[str, ...]]:
        raise NotImplementedError

    @abstractmethod
    def last_insert_id(self) -> int:
        raise NotImplementedError

    @property
    @abstractmethod
    def auto_increment_primary_key(self) -> str:
        raise NotImplementedError

    def quote_identifier(self, identifier: str) -> str:
        return '"' + identifier.replace('"', '""') + '"'

    def placeholders(self, count: int) -> str:
        return ", ".join("?" for _ in range(count))

    def create_temp_table(self, name: str, column_defs: Sequence[str]) -> None:
        self.execute(
            f"CREATE TEMP TABLE {self.quote_identifier(name)} "
            f"({', '.join(column_defs)})"
        )

    def drop_table(self, name: str) -> None:
        self.execute(f"DROP TABLE IF EXISTS {self.quote_identifier(name)}")


class SQLiteDatabaseAdapter(SQLDatabaseAdapter):
    dialect = "sqlite"

    def __init__(self, conn: sqlite3.Connection, database_path: str):
        self._conn = conn
        self.database_path = database_path

    @classmethod
    def connect(cls, database_url: str) -> SQLiteDatabaseAdapter:
        parsed = urlparse(database_url)
        if parsed.scheme not in ("", "sqlite", "file"):
            raise ValueError(f"not a SQLite URL: {_describe_dsn(database_url)}")
        if database_url in (":memory:", "sqlite:///:memory:"):
            path = ":memory:"
        elif parsed.scheme == "file":
            path = database_url
        elif parsed.scheme == "":
            path = database_url
        elif parsed.netloc and parsed.netloc not in ("", "localhost"):
            raise ValueError(
                "Only local SQLite URLs are supported: "
                f"{_describe_dsn(database_url)}"
            )
        else:
            path = unquote(parsed.path)
            if path.startswith("/") and database_url.startswith("sqlite:///"):
                pass
            elif path:
                path = path.lstrip("/")
            if not path:
                raise ValueError(
                    f"SQLite URL is missing a path: {_describe_dsn(database_url)}"
                )
        uri = False
        if parsed.scheme == "sqlite" and parsed.query:
            path = f"file:{path}?{parsed.query}"
            uri = True
        elif parsed.scheme in ("", "file") and database_url.startswith("file:"):
            uri = True
        if path != ":memory:" and not path.startswith("file:"):
            Path(path).parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(path, check_same_thread=False, uri=uri)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys=OFF")
        conn.execute("PRAGMA journal_mode=WAL")
        database_path = parsed.path if parsed.scheme == "file" else path
        return cls(conn, database_path)

    def execute(
        self, sql: str, params: Sequence[Any] | Mapping[str, Any] = ()
    ) -> sqlite3.Cursor:
        return self._conn.execute(sql, params)

    def executemany(self, sql: str, params: Iterable[Sequence[Any]]) -> sqlite3.Cursor:
        return self._conn.executemany(sql, params)

    def commit(self) -> None:
        self._conn.commit()

    def rollback(self) -> None:
        self._conn.rollback()

    def begin(self) -> None:
        self._conn.execute("BEGIN")

    def close(self) -> None:
        self._conn.close()

    @property
    def in_transaction(self) -> bool:
        return self._conn.in_transaction

    @property
    def raw_connection(self) -> sqlite3.Connection:
        return self._conn

    @property
    def auto_increment_primary_key(self) -> str:
        return "INTEGER PRIMARY KEY AUTOINCREMENT"

    def table_defs(self, table: str) -> tuple[tuple[str, ...], tuple[str, ...]]:
        rows = self.execute(
            f"PRAGMA table_info({self.quote_identifier(table)})"
        ).fetchall()
        if not rows:
            return (), ()
        columns: list[str] = []
        defs: list[str] = []
        for row in rows:
            name = row["name"]
            col_type = row["type"] or "TEXT"
            columns.append(name)
            defs.append(f"{self.quote_identifier(name)} {col_type}")
        return tuple(columns), tuple(defs)

    def last_insert_id(self) -> int:
        return int(self.execute("SELECT last_insert_rowid()").fetchone()[0])


#: The session name Chronos gives a PostgreSQL session whose DSN names none.
CHRONOS_APPLICATION_NAME = "chronos-branching"


def _parse_postgres_dsn(database_url: str) -> dict[str, Any]:
    """Parse a PostgreSQL DSN, refusing an unparseable one without its content.

    libpq's parse errors quote the text around the fault, which is often the
    password: ``missing "=" after "secret"`` for a keyword DSN whose password
    holds a space, ``invalid percent-encoded token: "%zzS3CRET"`` for a URL
    (okg issue #2285, item 5). A parse failure is re-raised as a
    ``psycopg.ProgrammingError`` -- the class psycopg itself raises -- whose
    message names only what ``_describe_dsn`` names. The raise sits outside
    the ``except`` block so the original error is neither the new error's
    ``__cause__`` nor its ``__context__``. A CONNECTION failure (network,
    authentication) happens later, in ``psycopg.connect``, and keeps its own
    message.
    """
    try:
        fields: dict[str, Any] | None = conninfo_to_dict(database_url)
    except psycopg.ProgrammingError:
        fields = None
    if fields is None:
        raise psycopg.ProgrammingError(
            "Chronos could not parse the PostgreSQL connection string "
            f"({_describe_dsn(database_url)}); check its quoting and "
            "percent-encoding"
        )
    return fields


def _dsn_application_name(fields: Mapping[str, Any]) -> str:
    """Return the ``application_name`` a parsed DSN already carries, or ``""``.

    ``fields`` comes from ``_parse_postgres_dsn``, which reads both spellings
    libpq accepts -- a URL query parameter and a keyword/value pair. An empty
    value names nothing and counts as absent.
    """
    return str(fields.get("application_name") or "")


class PostgresDatabaseAdapter(SQLDatabaseAdapter):
    dialect = "postgres"

    _named_param = re.compile(r"(?<!:):([A-Za-z_][A-Za-z0-9_]*)")
    _dollar_quote = re.compile(r"\$[A-Za-z_][A-Za-z0-9_]*\$|\$\$")

    def __init__(self, conn: psycopg.Connection[Any], database_url: str | None = None):
        self._conn = conn
        self.database_url = database_url

    @classmethod
    def connect(
        cls, database_url: str, *, options: str | None = None,
    ) -> PostgresDatabaseAdapter:
        """Open a Chronos-owned PostgreSQL session.

        `options` adds libpq ``options`` settings on top of the ones this
        factory and the DSN already carry. It is appended LAST and libpq takes
        the last `-c` for a setting, so a caller that needs a different
        idle-in-transaction bound -- `-c idle_in_transaction_session_timeout=0`
        for a deliberately long external transaction -- still wins.
        """
        # The DSN's own application_name wins (okg issue #2027). A keyword
        # argument overrides the value carried in the connection string, so
        # passing ours unconditionally erased the `okg-chronos-store` stamp
        # okg puts on every ChronosGraphStore DSN. Name the session only when
        # the DSN names none.
        # Parse first: an unparseable DSN is refused here with a redacted
        # message, before psycopg.connect can quote libpq's parse error.
        dsn_fields = _parse_postgres_dsn(database_url)
        name_kwargs: dict[str, str] = (
            {}
            if _dsn_application_name(dsn_fields)
            else {"application_name": CHRONOS_APPLICATION_NAME}
        )
        conn = psycopg.connect(
            database_url, row_factory=dict_row,
            **name_kwargs,
            # Pin the session search_path so a login role whose NAME matches a
            # schema (a role named `okg`) cannot put that schema ahead of
            # `public` and send unqualified `_chronos_*` DDL to the wrong
            # place. See chronos_core.pg_session (issue #1968).
            #
            # Bound how long this session may sit `idle in transaction`
            # (issue #1997). okg reaches this factory from ~18 production call
            # sites through `ChronosGraphStore.connect`, and those sessions
            # carried NO bound while okg's own pools have had one since
            # #1310 -- so the two factories disagreed, and the path with no
            # bound is the one that opens Chronos's metadata transactions.
            # Both now apply the same RESOLVED value: the embedding
            # application's resolver (okg's env override, then manifest key,
            # then default), not the bare default -- issue #2920, where the
            # default killed a cms-kb re-ingest configured for two hours.
            # The ordering matches
            # `okg.substrate.db.connection.connection_kwargs` exactly: pin,
            # then the DSN's own options, then the bound, then the caller's.
            options=session_options(
                database_url,
                extra=" ".join(
                    part for part in (idle_in_transaction_option(), options or "")
                    if part
                ),
            ),
        )
        return cls(conn, database_url)

    def execute(
        self, sql: str, params: Sequence[Any] | Mapping[str, Any] = ()
    ) -> psycopg.Cursor[Any]:
        execute = self._execute_with_dict_rows
        if not params:
            return execute(sql)
        translated_sql, translated_params = self._translate_params(sql, params)
        translated_sql = self._escape_pyformat_percents(
            translated_sql,
            named_params=isinstance(translated_params, Mapping),
        )
        translated_params = self._adapt_params(translated_params)
        return execute(translated_sql, translated_params)

    def _execute_with_dict_rows(
        self,
        sql: str,
        params: Sequence[Any] | Mapping[str, Any] = (),
    ) -> psycopg.Cursor[Any]:
        if getattr(self._conn, "row_factory", None) is dict_row:
            return self._conn.execute(sql, params)
        cursor = self._conn.cursor(row_factory=dict_row)
        return cursor.execute(sql, params)

    def executemany(
        self, sql: str, params: Iterable[Sequence[Any]]
    ) -> psycopg.Cursor[Any]:
        translated_sql = self._translate_qmark(sql)
        translated_sql = self._escape_pyformat_percents(translated_sql)
        cursor = self._conn.cursor()
        cursor.executemany(
            translated_sql,
            (self._adapt_params(row) for row in params),
        )
        return cursor

    def commit(self) -> None:
        self._conn.commit()

    def rollback(self) -> None:
        self._conn.rollback()

    def begin(self) -> None:
        """Open a transaction on an idle session, with exactly one BEGIN.

        On a non-autocommit connection -- every session `connect()` opens --
        psycopg sends its own BEGIN before the first statement of a
        transaction. Executing "BEGIN" as that statement therefore sent a
        second BEGIN inside psycopg's: PostgreSQL ignored it and logged
        "there is already a transaction in progress" once per Chronos
        transaction (lane C item 238a; 92 warnings in one CI bootstrap).

        The transaction must be open when this returns, not at the next
        statement: `in_transaction` decides whether
        `_bootstrap_transaction_boundary` commits, and psycopg's
        `Connection.transaction()` nests as a SAVEPOINT only inside an open
        transaction -- either would otherwise COMMIT the caller's transaction
        early. So on a non-autocommit connection a trivial statement lets
        psycopg's own BEGIN open it now; on an autocommit connection only an
        explicit BEGIN opens one.

        A session that is already in a transaction is refused: a BEGIN there
        starts nothing, so whoever called this would believe it owns a
        transaction boundary that does not exist.
        """
        status = self._conn.info.transaction_status
        if status != TransactionStatus.IDLE:
            raise SQLAdapterError(
                "begin() needs an idle session, but the session is "
                f"{status.name}: PostgreSQL ignores a BEGIN inside a "
                "transaction, so no new transaction would start"
            )
        if self._conn.autocommit:
            self._conn.execute("BEGIN")
        else:
            self._conn.execute("SELECT 1")

    def close(self) -> None:
        self._conn.close()

    @property
    def in_transaction(self) -> bool:
        return self._conn.info.transaction_status != TransactionStatus.IDLE

    @property
    def raw_connection(self) -> psycopg.Connection[Any]:
        return self._conn

    @property
    def auto_increment_primary_key(self) -> str:
        return "BIGSERIAL PRIMARY KEY"

    def create_temp_table(self, name: str, column_defs: Sequence[str]) -> None:
        self.execute(
            f"CREATE TEMP TABLE {self.quote_identifier(name)} "
            f"({', '.join(column_defs)}) ON COMMIT DROP"
        )

    def _resolve_schema(self, table_name: str) -> str | None:
        """Return the schema `table_name` resolves to under `search_path`.

        `to_regclass` applies the session's own `search_path`, so this is
        the same table the surrounding DDL and DML reach. Returns `None`
        when no table of that name is visible.
        """
        row = self.execute(
            """
            SELECT namespace.nspname AS schema_name
            FROM pg_catalog.pg_class AS relation
            JOIN pg_catalog.pg_namespace AS namespace
              ON namespace.oid = relation.relnamespace
            WHERE relation.oid = pg_catalog.to_regclass(
                pg_catalog.quote_ident(:table)
            )
            """,
            {"table": table_name},
        ).fetchone()
        return None if row is None else str(row["schema_name"])

    def table_defs(self, table: str) -> tuple[tuple[str, ...], tuple[str, ...]]:
        schema, table_name = self._split_table_name(table)
        if schema is None:
            schema = self._resolve_schema(table_name)
            if schema is None:
                return (), ()
        rows = self.execute(
            """
            SELECT column_name, data_type, udt_name, character_maximum_length,
                   numeric_precision, numeric_scale, is_nullable
            FROM information_schema.columns
            WHERE table_schema = :schema
              AND table_name = :table
            ORDER BY ordinal_position
            """,
            {"schema": schema, "table": table_name},
        ).fetchall()
        if not rows:
            return (), ()
        columns: list[str] = []
        defs: list[str] = []
        for row in rows:
            name = row["column_name"]
            columns.append(name)
            # Branched physical tables must be able to represent tombstones and
            # partial log records, so source-table NOT NULL constraints are not
            # copied into the branch storage table definitions.
            defs.append(f"{self.quote_identifier(name)} {self._column_type(row)}")
        return tuple(columns), tuple(defs)

    def last_insert_id(self) -> int:
        row = self.execute("SELECT LASTVAL() AS id").fetchone()
        return int(row["id"])

    def _translate_params(
        self, sql: str, params: Sequence[Any] | Mapping[str, Any]
    ) -> tuple[str, Sequence[Any] | Mapping[str, Any]]:
        if isinstance(params, Mapping):
            return self._translate_named(sql), params
        return self._translate_qmark(sql), params

    @classmethod
    def _translate_named(cls, sql: str) -> str:
        if ":" not in sql:
            return sql
        if not any(marker in sql for marker in ("'", "--", "/*", "$", "::")):
            return cls._named_param.sub(r"%(\1)s", sql)

        def replace(segment: str, index: int) -> tuple[str, int] | None:
            if segment[index] != ":":
                return None
            if index > 0 and segment[index - 1] == ":":
                return None
            if index + 1 >= len(segment):
                return None
            first = segment[index + 1]
            if not (first.isalpha() or first == "_"):
                return None
            end = index + 2
            while end < len(segment):
                char = segment[end]
                if not (char.isalnum() or char == "_"):
                    break
                end += 1
            return f"%({segment[index + 1:end]})s", end

        return cls._rewrite_outside_sql_literals(sql, replace)

    @classmethod
    def _translate_qmark(cls, sql: str) -> str:
        if "?" not in sql:
            return sql
        if not any(marker in sql for marker in ("'", "--", "/*", "$")):
            return sql.replace("?", "%s")

        def replace(segment: str, index: int) -> tuple[str, int] | None:
            if segment[index] == "?":
                return "%s", index + 1
            return None

        return cls._rewrite_outside_sql_literals(sql, replace)

    @staticmethod
    def _escape_pyformat_percents(sql: str, *, named_params: bool = False) -> str:
        """Escape literal percent signs for psycopg's pyformat parser.

        psycopg scans the full SQL string for ``%`` placeholders even inside
        quoted SQL literals. Chronos injects internal bound parameters into
        checked-out queries, so user SQL like ``LIKE 'doc:%'`` must become
        ``LIKE 'doc:%%'`` while generated placeholders remain intact.

        ``named_params`` selects which bare placeholders can be real: with a
        params mapping the translate step only generates ``%(name)s``, so a
        bare ``%s`` is always a literal. With params sequences, bare
        ``%s``/``%b``/``%t`` are placeholders and must be preserved.
        """
        if "%" not in sql:
            return sql
        output: list[str] = []
        index = 0
        length = len(sql)
        while index < length:
            char = sql[index]
            if char != "%":
                output.append(char)
                index += 1
                continue
            if (
                not named_params
                and index + 1 < length
                and sql[index + 1] in {"s", "b", "t"}
            ):
                output.append(sql[index:index + 2])
                index += 2
                continue
            if index + 1 < length and sql[index + 1] == "(":
                end = sql.find(")s", index + 2)
                name = sql[index + 2:end] if end != -1 else ""
                if end != -1 and name and all(
                    char.isalnum() or char == "_" for char in name
                ):
                    output.append(sql[index:end + 2])
                    index = end + 2
                    continue
            output.append("%%")
            index += 1
        return "".join(output)

    @classmethod
    def _rewrite_outside_sql_literals(
        cls,
        sql: str,
        replace,
    ) -> str:
        output: list[str] = []
        index = 0
        length = len(sql)
        while index < length:
            char = sql[index]
            if char == "'":
                index = cls._copy_single_quoted(sql, index, output)
                continue
            if char == '"':
                index = cls._copy_double_quoted(sql, index, output)
                continue
            if sql.startswith("--", index):
                newline = sql.find("\n", index + 2)
                end = length if newline == -1 else newline + 1
                output.append(sql[index:end])
                index = end
                continue
            if sql.startswith("/*", index):
                end = sql.find("*/", index + 2)
                end = length if end == -1 else end + 2
                output.append(sql[index:end])
                index = end
                continue
            if char == "$":
                match = cls._dollar_quote.match(sql, index)
                if match:
                    tag = match.group(0)
                    end = sql.find(tag, match.end())
                    end = length if end == -1 else end + len(tag)
                    output.append(sql[index:end])
                    index = end
                    continue
            replacement = replace(sql, index)
            if replacement is not None:
                text, index = replacement
                output.append(text)
                continue
            output.append(char)
            index += 1
        return "".join(output)

    @staticmethod
    def _copy_single_quoted(sql: str, index: int, output: list[str]) -> int:
        output.append(sql[index])
        index += 1
        length = len(sql)
        while index < length:
            output.append(sql[index])
            if sql[index] == "'":
                if index + 1 < length and sql[index + 1] == "'":
                    output.append(sql[index + 1])
                    index += 2
                    continue
                index += 1
                break
            index += 1
        return index

    @staticmethod
    def _copy_double_quoted(sql: str, index: int, output: list[str]) -> int:
        output.append(sql[index])
        index += 1
        length = len(sql)
        while index < length:
            output.append(sql[index])
            if sql[index] == '"':
                if index + 1 < length and sql[index + 1] == '"':
                    output.append(sql[index + 1])
                    index += 2
                    continue
                index += 1
                break
            index += 1
        return index

    @staticmethod
    def _split_table_name(table: str) -> tuple[str | None, str]:
        """Split `schema.table`; `None` schema means "resolve it live".

        An unqualified name must NOT be assumed to live in `public`. The
        connecting role decides: with the default `search_path` of
        `"$user", public`, a role named `okg` resolves an unqualified name
        in schema `okg` when that schema exists, which is exactly what the
        okg migration creates. Reading `public` in that case reports the
        wrong (usually empty) column set, so a caller guarding
        "does this column already exist?" re-adds it and Postgres raises
        DuplicateColumn (issue #1922).
        """
        if "." in table:
            schema, table_name = table.split(".", 1)
            return schema, table_name
        return None, table

    @staticmethod
    def _column_type(row: Mapping[str, Any]) -> str:
        data_type = row["data_type"]
        udt_name = row["udt_name"]
        if data_type == "USER-DEFINED":
            return str(udt_name)
        if data_type == "ARRAY":
            return str(udt_name)
        if data_type == "character varying":
            length = row["character_maximum_length"]
            return f"VARCHAR({length})" if length else "VARCHAR"
        if data_type == "character":
            length = row["character_maximum_length"]
            return f"CHAR({length})" if length else "CHAR"
        if data_type == "numeric":
            precision = row["numeric_precision"]
            scale = row["numeric_scale"]
            if precision is not None and scale is not None:
                return f"NUMERIC({precision}, {scale})"
            return "NUMERIC"
        if data_type == "timestamp without time zone":
            return "TIMESTAMP"
        if data_type == "timestamp with time zone":
            return "TIMESTAMPTZ"
        return str(data_type).upper()

    @staticmethod
    def _adapt_params(
        params: Sequence[Any] | Mapping[str, Any]
    ) -> Sequence[Any] | Mapping[str, Any]:
        if isinstance(params, Mapping):
            adapted_mapping: dict[str, Any] | None = None
            for key, value in params.items():
                if isinstance(value, dict):
                    if adapted_mapping is None:
                        adapted_mapping = dict(params)
                    adapted_mapping[key] = Jsonb(value)
            return params if adapted_mapping is None else adapted_mapping

        adapted_sequence: list[Any] | None = None
        for index, value in enumerate(params):
            if isinstance(value, dict):
                if adapted_sequence is None:
                    adapted_sequence = list(params)
                adapted_sequence[index] = Jsonb(value)
        return params if adapted_sequence is None else adapted_sequence


#: libpq keyword/value connection parameters. A scheme-less string carrying
#: any of them is a Postgres DSN in the OTHER spelling libpq accepts, never a
#: SQLite file path -- nobody names a database file `dbname=okg_test`.
_LIBPQ_KEYWORD_DSN = re.compile(
    r"(?:^|\s)(?:host|hostaddr|port|dbname|user|password|sslmode|service"
    r"|passfile|connect_timeout|application_name|options)\s*=",
)


def _is_libpq_keyword_dsn(database_url: str) -> bool:
    return bool(_LIBPQ_KEYWORD_DSN.search(database_url))


#: The only connection fields an error message may name. A password, a
#: passfile, an sslpassword or an `options` string never reaches a message.
_SAFE_DSN_FIELDS = ("host", "port", "dbname", "user")


def _describe_dsn(database_url: str) -> str:
    """Name a connection string's host, port, database and user -- nothing else.

    Error messages used to quote the whole string, password included (okg
    issue #2285, item 5). This reads either spelling libpq accepts; a string
    that parses as neither is described without any of its content.
    """
    fields: dict[str, str] = {}
    if _is_libpq_keyword_dsn(database_url):
        try:
            parsed_fields = conninfo_to_dict(database_url)
        except Exception:  # noqa: BLE001 -- describing, not connecting
            return "an unparseable keyword/value connection string"
        fields = {
            key: str(parsed_fields[key])
            for key in _SAFE_DSN_FIELDS
            if parsed_fields.get(key)
        }
    else:
        try:
            parsed = urlparse(database_url)
        except ValueError:
            # An unbalanced "[" or "]" -- which a raw password may hold --
            # makes urlparse raise "Invalid IPv6 URL" (okg issue #2285).
            return "a connection string that does not parse as a URL"
        _, _, rest = database_url.partition("://")
        if "@" in rest[len(parsed.netloc):]:
            # A raw "/", "?" or "#" in the password ended the authority
            # early, so the "database" (or host, port) read here would be the
            # password's tail. Name the scheme alone (okg issue #2285).
            if parsed.scheme:
                return f"scheme={parsed.scheme}"
            return "a connection string with no host, port, database or user"
        try:
            port = parsed.port
        except ValueError:
            port = None
        candidates = {
            "scheme": parsed.scheme,
            "host": parsed.hostname,
            "port": port,
            "dbname": parsed.path.lstrip("/"),
            "user": parsed.username,
        }
        fields = {key: str(value) for key, value in candidates.items() if value}
    if not fields:
        return "a connection string with no host, port, database or user"
    return " ".join(f"{key}={value}" for key, value in fields.items())


def connect_sql_database(
    database_url: str, *, options: str | None = None,
) -> SQLDatabaseAdapter:
    """Open the adapter for `database_url`.

    `options` carries extra libpq ``options`` settings for a PostgreSQL URL
    (see `PostgresDatabaseAdapter.connect`); it is meaningless for SQLite and
    is refused there rather than ignored.
    """
    parsed = urlparse(database_url)
    if parsed.scheme in ("", "sqlite", "file"):
        if _is_libpq_keyword_dsn(database_url):
            # Backend selection is by URL scheme, and a keyword-form DSN
            # parses to an empty scheme. Treating it as a path used to open
            # a SQLite database in a FILE named after the whole connection
            # string; the caller then met a sqlite3 UNIQUE-constraint error
            # that never mentioned the DSN (okg issue #1971).
            #
            # This runs BEFORE the `options` refusal below, deliberately. A
            # keyword-form DSN passed WITH `options` is a wrong-spelling
            # problem, and this message names the URL form to write instead.
            # The `options` message would only say the argument does not
            # apply to SQLite, which for a Postgres DSN points at the wrong
            # argument entirely.
            raise ValueError(
                "refusing a libpq keyword/value connection string as a "
                "SQLite path: Chronos selects its backend by URL scheme, "
                "and this string has none. Pass the URL form instead, "
                "postgresql://user:password@host:port/dbname -- got "
                f"{_describe_dsn(database_url)}"
            )
        if options:
            raise ValueError(
                "libpq options are not applicable to a SQLite Chronos URL: "
                f"{_describe_dsn(database_url)}"
            )
        return SQLiteDatabaseAdapter.connect(database_url)
    if parsed.scheme in ("postgres", "postgresql"):
        return PostgresDatabaseAdapter.connect(database_url, options=options)
    raise ValueError(
        "unsupported SQL database URL for Chronos branching: "
        f"{_describe_dsn(database_url)}"
    )
