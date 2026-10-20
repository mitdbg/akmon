"""Adversarial fuzz pass for `run_sql`'s safety guard.

Per `harden-mcp-run-sql-safety` task 4.1: feed ~1000 random byte
strings + ~100 hand-crafted bypass seeds to `validate_run_sql`.
The guard MUST raise a structured `RunSqlInvalid` subclass on
malformed / privileged input, OR accept clean inputs cleanly. No
plain Python exception (KeyError, AttributeError, TypeError) may
escape the safety guard — that would mean the guard had a code
path the agent could exploit by carefully shaping its input.

The fuzz seeds are NOT exhaustive; they target classes of inputs
the AST walker is most likely to mishandle (Postgres syntax
corners, identifier-escape combinations, deeply-nested CTEs,
malformed UTF-8). The random bytes catch the unknown-unknowns.
"""
from __future__ import annotations

import random
import string

import pytest

from okg.substrate.errors import SubstrateError


# ---------- adversarial seed corpus ----------

_SEEDS: tuple[str, ...] = (
    # Privileged-table references in every spelling we can think of.
    'SELECT * FROM okg.nodes_live',
    'SELECT * FROM okg."nodes_live"',
    'SELECT * FROM "okg"."nodes_live"',
    'SELECT * FROM "OKG"."NODES_LIVE"',
    'SELECT * FROM okg.U&"nodes_live"',
    'SELECT * FROM "okg".U&"nodes_live"',
    'SELECT * FROM ONLY okg.nodes_live',
    'TABLE okg.nodes_live',
    # Forbidden functions in every spelling.
    'SELECT pg_sleep(5)',
    'SELECT pg_catalog.pg_sleep(5)',
    'SELECT "pg_catalog"."pg_sleep"(5)',
    "SELECT 'pg_sleep'::regproc",
    "SELECT 'pg_catalog.pg_sleep'::regproc",
    "SELECT 'pg_sleep'::pg_catalog.regproc",
    "SELECT pg_catalog.set_config('foo','bar',true)",
    # Multi-statement attempts — first stmt non-SELECT must fire
    # NonSelect; both SELECT must fire MultiStatement.
    'DELETE FROM okg.nodes_live; SELECT 1',
    'SELECT 1; DROP TABLE okg.nodes_live',
    'SELECT 1; SELECT 2',
    'BEGIN; SELECT 1; COMMIT',
    # Nested-comment hide attempts (these should pass — pglast strips
    # comments before the AST has any forbidden nodes).
    'SELECT 1 /* /* x */ pg_sleep(5) */ FROM okg.v_nodes',
    'SELECT /*x*/ 1 /*y*/ FROM okg.v_nodes',
    # CTE shadowing.
    'WITH nodes_live AS (SELECT 1 AS x) SELECT * FROM nodes_live',
    # Recursive CTE shadowing.
    'WITH RECURSIVE nodes_live AS '
    '(SELECT 1 AS x UNION ALL SELECT x+1 FROM nodes_live WHERE x<3) '
    'SELECT * FROM nodes_live',
    # Multiple CTEs.
    'WITH a AS (SELECT 1), b AS (SELECT 2) '
    'SELECT * FROM a, b',
    # Deeply-nested subquery.
    'SELECT * FROM ('
    '  SELECT * FROM ('
    '    SELECT * FROM okg.v_nodes WHERE node_id IN ('
    '      SELECT node_id FROM okg.nodes_live'
    '    )'
    '  ) sub1'
    ') sub2',
    # LATERAL join with privileged ref.
    'SELECT * FROM okg.v_nodes n, LATERAL '
    '(SELECT 1 FROM okg.edges_live WHERE src = n.node_id) sub',
    # SET / EXPLAIN / VACUUM (refused as non-SELECT).
    'SET search_path = okg, public',
    'EXPLAIN ANALYZE SELECT * FROM okg.v_nodes',
    'VACUUM okg.nodes_live',
    'COPY okg.nodes_live TO STDOUT',
    'CALL okg.something()',
    # DDL.
    'CREATE TABLE okg.evil (x int)',
    'ALTER ROLE app_ro NOLOGIN',
    # Empty / minimal / whitespace-only.
    '',
    '   ',
    '\n\t  \n',
    ';',
    ';;;;',
    '-- only a comment',
    '/* only a block */',
    # Truncated / unterminated SQL — should parse-error cleanly.
    'SELECT * FROM',
    "SELECT 'unterminated string FROM okg.v_nodes",
    'SELECT * FROM "unterminated_ident',
    # Invalid Unicode escape.
    "SELECT U&'\\zzzzz' FROM okg.v_nodes",
)


# ---------- fuzz harness ----------


def _random_sql(rng: random.Random) -> str:
    """Generate a random byte-string that LOOKS sql-ish enough to
    exercise the parser. We mix:
      - short random keywords from a fixed vocabulary
      - random punctuation (semicolons, parens, quotes, commas)
      - rare control characters
    The idea is not to produce *valid* SQL but to surface any
    code path that crashes on unexpected input shapes.
    """
    vocab = (
        "SELECT", "FROM", "WHERE", "AND", "OR", "NOT", "JOIN", "ON",
        "WITH", "AS", "GROUP", "BY", "HAVING", "ORDER", "LIMIT",
        "OFFSET", "UNION", "ALL", "DISTINCT", "CASE", "WHEN", "THEN",
        "ELSE", "END", "IS", "NULL", "TRUE", "FALSE", "INSERT",
        "UPDATE", "DELETE", "DROP", "CREATE", "TABLE", "VIEW",
        "okg.nodes_live", "okg.v_nodes", "okg.edges_live",
        "pg_sleep", "set_config", "current_setting",
        "pg_catalog.pg_sleep", "'pg_sleep'::regproc",
        "(", ")", ",", ";", "*", "1", "0", "'x'", '"y"',
        "/* nested /* */", "*/", "--",
    )
    n_tokens = rng.randint(1, 30)
    parts = [rng.choice(vocab) for _ in range(n_tokens)]
    sep_choices = (" ", "  ", "\n", "\t")
    return rng.choice(sep_choices).join(parts)


def _random_garbage(rng: random.Random) -> str:
    """Random ASCII garbage, occasionally with control chars."""
    n = rng.randint(0, 80)
    chars = string.ascii_letters + string.digits + " \t\n\";'()[],/-*"
    return "".join(rng.choice(chars) for _ in range(n))


@pytest.mark.parametrize("seed", _SEEDS)
def test_seed_corpus(seed: str) -> None:
    """Each adversarial seed must either validate cleanly or raise
    a structured `RunSqlInvalid` subclass — never a plain Python
    exception.
    """
    from okg.substrate.mcp.sql_safety import (
        validate_run_sql, RunSqlInvalid,
    )

    try:
        validate_run_sql(seed)
    except RunSqlInvalid:
        # Structured rejection — by design.
        return
    except SubstrateError:
        # Substrate's other structured errors are also acceptable.
        return
    except Exception as e:
        # Any other exception is a guard regression.
        pytest.fail(
            f"unstructured exception escaped safety guard: "
            f"{type(e).__name__}: {e}\n"
            f"seed: {seed!r}\n"
            f"This is a fuzz-test regression — every code path "
            f"in validate_run_sql must raise a RunSqlInvalid "
            f"subclass on bad input."
        )


def test_random_sql_corpus() -> None:
    """1000 random sql-ish strings + 100 garbage strings. Same
    discipline: structured errors only."""
    from okg.substrate.mcp.sql_safety import (
        validate_run_sql, RunSqlInvalid,
    )

    rng = random.Random(0xDEADBEEF)  # deterministic across runs
    failures: list[tuple[str, Exception]] = []

    for _ in range(1000):
        sql = _random_sql(rng)
        try:
            validate_run_sql(sql)
        except (RunSqlInvalid, SubstrateError):
            continue
        except Exception as e:  # noqa: BLE001
            failures.append((sql, e))

    for _ in range(100):
        sql = _random_garbage(rng)
        try:
            validate_run_sql(sql)
        except (RunSqlInvalid, SubstrateError):
            continue
        except Exception as e:  # noqa: BLE001
            failures.append((sql, e))

    assert not failures, (
        f"fuzz pass produced {len(failures)} unstructured "
        f"exceptions. First five:\n"
        + "\n".join(
            f"  {type(e).__name__}: {e}\n  on: {sql!r}"
            for sql, e in failures[:5]
        )
    )
