"""okg#3112 step 2, slice 1: admission and bounded keys (PACT
lane-h-3112-step2, req.lane-h-3112-step2.admission; design.md sections
2.1-2.3 and section 9 items 1-3). Native PostgreSQL.

* num, text and bool: the index predicate and key admit exactly what
  ``attr_index``'s ``_admission_sql`` admitted, over the E1b corpus. Since
  okg#3112 step 4b deleted that module, its rule is frozen here
  (:data:`_FROZEN_ATTR_INDEX_ADMISSION`) as the expected result.
* The catalog's declarable value types equal the index value types
  (``modules_compose.ATTR_VALUE_TYPES == declared_attr_indexes.VALUE_TYPES``),
  so the catalog cannot declare a type the indexes skip.
* ts: ``okg.attr_ts_v4`` admits exactly E14's 27 values with the recorded
  instants, the same under the four E6 session settings; the 11
  Python-only bound forms are refused (not pushable). Failure cases: the
  regex dropped, or narrowed back to 6 fractional digits.
* The rendered DDL equals ``pg_get_indexdef`` (the lifecycle test checks the
  canonical text); EXPLAIN of the rendered probe names the index at the head
  and at a pinned ordinal. Failure case: the probe's key changed by one
  token.
* E7: a 3,224-character incompressible text and a 10,000-digit number are
  indexed without error.
* ``okg.attr_num_f8`` equals ``v::float8`` where the cast succeeds and
  Python ``float()`` on every E13 value.
"""

from __future__ import annotations

from typing import Any

import pytest

psycopg = pytest.importorskip("psycopg")

from psycopg.rows import dict_row  # noqa: E402

import declared_attr_index_corpora as corpora  # noqa: E402
import declared_attr_index_support as s  # noqa: E402
from okg.substrate import declared_attr_indexes as dai  # noqa: E402
from okg.substrate.catalog.modules_compose import ATTR_VALUE_TYPES  # noqa: E402
from okg.substrate.library.sources.base import NodeFact  # noqa: E402

pytestmark = pytest.mark.postgres

_UTC = (
    "CASE WHEN t IS NULL THEN NULL WHEN isfinite(t) THEN "
    "to_char(t AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS.US\"Z\"') ELSE t::text END"
)


def _fuzz_table(cur: Any) -> None:
    cur.execute("CREATE TEMP TABLE fz (id int, attrs jsonb, deleted boolean DEFAULT false)")
    for i, text in enumerate(corpora.E1B_VALUES):
        cur.execute(
            "INSERT INTO fz (id, attrs) VALUES (%s, jsonb_build_object('k', %s::jsonb))",
            (i, text),
        )


#: ``okg.substrate.attr_index._admission_sql`` for num, text and bool as it
#: stood at okg#3112 step 4a (dev ee1ca86059), frozen when step 4b deleted the
#: module: the typed value the derivation stored, or NULL when the JSON type
#: contradicts the declaration. ``{a}`` is the attribute's jsonb expression.
_FROZEN_ATTR_INDEX_ADMISSION = """
      CASE WHEN d.value_type = 'num'
            AND jsonb_typeof({a}) = 'number'
           THEN ({a} #>> '{{}}')::numeric END AS value_num,
      CASE WHEN d.value_type = 'text'
            AND jsonb_typeof({a}) = 'string'
           THEN {a} #>> '{{}}' END AS value_text,
      CASE WHEN d.value_type = 'bool'
            AND jsonb_typeof({a}) = 'boolean'
           THEN ({a} #>> '{{}}')::boolean END AS value_bool
"""


def test_value_type_vocabulary_matches_the_index_value_types() -> None:
    """The catalog's declarable value types and the declared indexes' value
    types are one vocabulary declared in two modules (composing a catalog does
    not import the index runtime). Drift would let a catalog declare a type
    the reconciler cannot index."""
    assert set(ATTR_VALUE_TYPES) == set(dai.VALUE_TYPES)


@pytest.mark.parametrize("value_type", ["num", "text", "bool"])
def test_num_text_bool_admission_equals_attr_index(clean_runtime: str, value_type: str) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn, conn.cursor() as cur:
        _fuzz_table(cur)
        # attr_index (frozen): the typed value its derivation stored, or NULL.
        typed_column = {"num": "value_num", "text": "value_text", "bool": "value_bool"}[value_type]
        cur.execute(
            f"SELECT id, x.{typed_column}::text FROM ("
            f"SELECT fz.id, {_FROZEN_ATTR_INDEX_ADMISSION.format(a='(fz.attrs -> ' + chr(39) + 'k' + chr(39) + ')')} "
            f"FROM fz CROSS JOIN (SELECT %s::text AS value_type) d) x "
            f"WHERE x.{typed_column} IS NOT NULL ORDER BY id",
            (value_type,),
        )
        old = cur.fetchall()
        # The expression index: its predicate, and its key's typed value.
        cur.execute(
            f"SELECT id, {dai._typed_text(value_type, 'attrs -> ' + chr(39) + 'k' + chr(39))} "
            f"FROM fz WHERE {dai.admission_predicate('k', value_type)} "
            f"AND {dai.key_expression('k', value_type)} IS NOT NULL ORDER BY id"
        )
        new = cur.fetchall()
    assert new == old
    assert len(new) >= 2


def _ts_v4_rows(cur: Any, function: str = "okg.attr_ts_v4") -> dict[str, str]:
    out: dict[str, str] = {}
    for value in corpora.E14_TS:
        cur.execute(f"SELECT {_UTC} FROM (SELECT {function}(%s) AS t) q", (value,))
        result = cur.fetchone()[0]
        if result is not None:
            out[value] = result
    return out


def test_ts_v4_admits_exactly_e14s_values_under_every_session_setting(clean_runtime: str) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn, conn.cursor() as cur:
        seen = []
        for statements in corpora.TS_SESSIONS:
            for statement in statements:
                cur.execute(statement)
            seen.append(_ts_v4_rows(cur))
    assert seen[0] == corpora.E14_V4_ADMITS
    assert len(seen[0]) == 27
    assert all(rows == seen[0] for rows in seen[1:]), "v4 must not depend on the session"


def test_ts_v4_sets_no_abbreviations(clean_runtime: str) -> None:
    """Cost guard, by structure. ``SET timezone_abbreviations`` on the
    function re-reads the abbreviations file on every call (200,000 calls:
    9.7 s against 0.64 s without it, evidence/slice1/review-fixes/
    ts_v4_cost.out). v4 maps Z to +00:00 instead, so no abbreviation is
    looked up and only TimeZone and DateStyle are set. Asserted on the
    function's settings, not on a timing, so a loaded runner cannot flake."""
    with psycopg.connect(clean_runtime, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute("SELECT proconfig FROM pg_proc WHERE oid = 'okg.attr_ts_v4(text)'::regprocedure")
        config = list(cur.fetchone()[0] or [])
    assert not [c for c in config if c.startswith("timezone_abbreviations")], config
    assert sorted(c.split("=")[0] for c in config) == ["DateStyle", "TimeZone"], config


def test_ts_admission_goes_through_the_index_predicate(clean_runtime: str) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute("CREATE TEMP TABLE tz (v text, attrs jsonb, deleted boolean DEFAULT false)")
        for value in corpora.E14_TS + [None]:
            cur.execute(
                "INSERT INTO tz (v, attrs) VALUES (%s, jsonb_build_object('k', to_jsonb(%s::text)))",
                (value, value),
            )
        cur.execute("INSERT INTO tz (v, attrs) VALUES ('number', '{\"k\": 5}')")
        cur.execute(
            f"SELECT v FROM tz WHERE {dai.admission_predicate('k', 'ts')} "
            f"AND {dai.key_expression('k', 'ts')} IS NOT NULL"
        )
        admitted = {r[0] for r in cur.fetchall()}
    assert admitted == set(corpora.E14_V4_ADMITS)


@pytest.mark.parametrize(
    ("variant", "replace"),
    [
        ("regex_dropped", ("v !~ ", "false AND v !~ ")),
        ("six_digits", (r"[0-9]{1,9}", r"[0-9]{1,6}")),
    ],
)
def test_ts_failure_cases_a_changed_regex_is_caught(
    clean_runtime: str, variant: str, replace: tuple[str, str]
) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT pg_get_functiondef('okg.attr_ts_v4(text)'::regprocedure)"
        )
        source = cur.fetchone()[0]
        assert replace[0] in source
        broken = source.replace(replace[0], replace[1], 1).replace(
            "okg.attr_ts_v4(", "pg_temp.attr_ts_broken(", 1
        )
        cur.execute(broken)
        rows = _ts_v4_rows(cur, "pg_temp.attr_ts_broken")
    assert rows != corpora.E14_V4_ADMITS
    if variant == "six_digits":
        assert "2024-01-02T03:04:05.123456789Z" not in rows
    else:
        assert "20240102" in rows or "2024-1-2" in rows or "2024-01-02 " in rows


def test_the_python_only_ts_bound_forms_are_not_pushable(clean_runtime: str) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn, conn.cursor(row_factory=dict_row) as cur:
        accepts = lambda bounds: dai.ts_bounds_accepted(cur, bounds)  # noqa: E731
        for bound in corpora.PYTHON_ONLY_TS_BOUNDS:
            for op, value in (("eq", bound), ("gt", bound), ("between", [bound, "2025-01-01"])):
                with pytest.raises(dai.NotPushable) as refused:
                    dai.render_probe("k", "ts", op, value, ts_accepts=accepts)
                assert refused.value.reason == "bound_not_strict_iso"
        # The regex alone does not decide: a well-shaped invalid date.
        with pytest.raises(dai.NotPushable):
            dai.render_probe("k", "ts", "eq", "2024-13-45", ts_accepts=accepts)
        assert dai.render_probe("k", "ts", "eq", "2024-01-02t03:04:05z", ts_accepts=accepts)


def test_attr_num_f8_equals_the_cast_and_python_float(clean_runtime: str) -> None:
    values, _ = corpora.e13()
    assert len(values) == 698
    with psycopg.connect(clean_runtime, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            """
            CREATE FUNCTION pg_temp.try_f8(v numeric) RETURNS float8 LANGUAGE plpgsql AS $$
            BEGIN RETURN v::float8; EXCEPTION WHEN OTHERS THEN RETURN NULL; END $$
            """
        )
        cur.execute(
            "SELECT raw, okg.attr_num_f8(raw::numeric), pg_temp.try_f8(raw::numeric) "
            "FROM unnest(%s::text[]) AS raw",
            (values,),
        )
        rows = cur.fetchall()
    cast_ok = [(raw, f8, cast) for raw, f8, cast in rows if cast is not None]
    assert len(cast_ok) == 687, len(cast_ok)
    assert all(f8 == cast and str(f8) == str(cast) for _, f8, cast in cast_ok)
    # Equal as numbers (E13's test). PostgreSQL numeric has no negative zero,
    # so "-0" reads 0.0 where Python gives -0.0; the two compare equal.
    mismatched = [(raw, f8) for raw, f8, _ in rows if float(raw) != f8]
    assert mismatched == []


def test_attr_num_key_is_monotone_and_bounded(clean_runtime: str) -> None:
    values, _ = corpora.e13()
    with psycopg.connect(clean_runtime, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT count(*) FILTER (WHERE prev > k), max(octet_length(k::text))
              FROM (SELECT okg.attr_num_key(n) AS k,
                           lag(okg.attr_num_key(n)) OVER (ORDER BY n) AS prev
                      FROM (SELECT raw::numeric AS n FROM unnest(%s::text[]) AS raw) a) b
            """,
            (values,),
        )
        violations, widest = cur.fetchone()
    assert violations == 0
    assert widest <= 400


# --------------------------------------------------------------------------- #
# Real indexes: E7, renderer = index                                           #
# --------------------------------------------------------------------------- #


def test_e7_long_text_and_a_10000_digit_number_index_without_error(clean_runtime: str) -> None:
    """The writes go through the graph store with both indexes in place. No
    publish: today's attr_index text index itself fails on this value inside
    the publish (design section 13), which is what the bounded key removes."""
    dsn = clean_runtime
    s.seed(dsn, {"t_text": "text", "t_num": "num"})
    assert s.reconcile(dsn)["status"] == "converged"
    long_text = corpora.md5_chain("x", 3224)
    s.apply(dsn, [NodeFact(s.nid(i), "thing", {"t_text": f"v{i}", "t_num": i}, {"row": i}, {"rev": 1})
                  for i in range(20)])
    s.apply(dsn, [NodeFact(s.nid(100), "thing", {"t_text": long_text, "t_num": 1}, {"row": 100},
                           {"rev": 1})])
    tables = s.physical(dsn)
    digits = "7" * 10000
    with psycopg.connect(dsn, autocommit=True) as conn:
        # A JSON number Python cannot carry as a float, written in place.
        conn.execute(
            f'UPDATE public."{tables["node"]}" SET attrs = jsonb_set(attrs, %s, %s::jsonb) '
            "WHERE node_id = %s",
            (["t_num"], digits, s.nid(100)),
        )
    with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor(row_factory=dict_row) as cur:
        found = {}
        for attr, value_type in (("t_text", "text"), ("t_num", "num")):
            cur.execute("BEGIN")
            cur.execute("SET LOCAL enable_seqscan = off")
            statement = (
                f'SELECT octet_length(g.attrs ->> %s) AS n FROM public."{tables["node"]}" g '
                f"WHERE {dai.admission_predicate(attr, value_type, alias='g')} "
                f"AND {dai.key_expression(attr, value_type, alias='g')} IS NOT NULL "
                "AND g.node_id = %s AND g.live_hi = (SELECT max(live_hi) FROM "
                f'public."{tables["node"]}")'
            )
            cur.execute("EXPLAIN (COSTS OFF) " + statement, (attr, s.nid(100)))
            plan = "\n".join(str(r["QUERY PLAN"]) for r in cur.fetchall())
            cur.execute(statement, (attr, s.nid(100)))
            found[attr] = [r["n"] for r in cur.fetchall()]
            cur.execute("ROLLBACK")
            assert dai.index_name("node", attr, value_type) in plan, plan
    assert found == {"t_text": [3224], "t_num": [10000]}
    with pytest.raises(psycopg.errors.ProgramLimitExceeded):
        # Failure case: a full-value text key cannot hold the same row.
        with psycopg.connect(dsn, autocommit=True) as conn:
            conn.execute(
                f'CREATE INDEX e7_full_value ON public."{tables["node"]}" '
                "((attrs ->> 't_text'), live_lo, live_hi) "
                "WHERE deleted = false AND jsonb_typeof(attrs -> 't_text') = 'string'"
            )


def _probe_plan(cur: Any, sql_text: str, params: tuple[Any, ...]) -> str:
    cur.execute("BEGIN")
    try:
        cur.execute("SET LOCAL enable_seqscan = off")
        cur.execute("EXPLAIN (COSTS OFF) " + sql_text, params)
        return "\n".join(str(next(iter(r.values()))) for r in cur.fetchall())
    finally:
        cur.execute("ROLLBACK")


#: A fragment of each key as EXPLAIN prints it in an Index Cond.
_KEY_IN_PLAN = {"text": "\"left\"((attrs", "num": "attr_num_key", "bool": "CASE", "ts": "attr_ts_v4"}


def _key_in_index_cond(plan: str, index_name: str, value_type: str) -> bool:
    """True when the plan scans ``index_name`` with its key in the Index Cond."""
    lines = plan.splitlines()
    for i, line in enumerate(lines):
        if index_name in line and "Index" in line:
            for cond in lines[i + 1:i + 3]:
                if "Index Cond" in cond and _KEY_IN_PLAN[value_type] in cond:
                    return True
    return False


def test_the_rendered_probe_reads_through_the_index_at_head_and_pinned(clean_runtime: str) -> None:
    dsn = clean_runtime
    identity = s.seed(dsn)
    assert s.reconcile(dsn)["status"] == "converged"
    nodes, rels = s.generation_1()
    s.apply(dsn, nodes)
    s.apply(dsn, rels)
    s.publish(dsn, identity)
    nodes, rels = s.generation_2()
    s.apply(dsn, nodes)
    s.apply(dsn, rels)
    s.publish(dsn, identity)
    tables = s.physical(dsn)
    pinned, head = [int(o["bp"]) for o in s.ordinals(dsn)]
    probes = {
        "text": ("between", ["alpha", "gamma"]),
        "num": ("gt", 1.5),
        "bool": ("eq", True),
        "ts": ("lt", "2024-06-01T00:00:00Z"),
    }
    with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor(row_factory=dict_row) as cur:
        accepts = lambda bounds: dai.ts_bounds_accepted(cur, bounds)  # noqa: E731
        for kind in dai.INDEXED_KINDS:
            for attr, value_type in s.DECL.items():
                op, value = probes[value_type]
                probe = dai.render_probe(attr, value_type, op, value, alias="g", ts_accepts=accepts)
                name = dai.index_name(kind, attr, value_type)
                for bp in (head, pinned):
                    statement = (
                        f'SELECT g.ctid FROM public."{tables[kind]}" g WHERE {probe.sql} '
                        "AND g.live_lo <= %s AND %s < g.live_hi"
                    )
                    plan = _probe_plan(cur, statement, (*probe.params, bp, bp))
                    assert _key_in_index_cond(plan, name, value_type), plan
                # Failure case: one token of the key changed (512 -> 511 for
                # text, a different function argument elsewhere): the
                # planner can no longer match the index.
                broken = probe.sql.replace(", 512)", ", 511)").replace(
                    "::numeric)", "::numeric + 0)"
                ).replace("::boolean END", "::text::boolean END").replace(
                    "okg.attr_ts_v4(g.attrs ->> ", "okg.attr_ts_v4(btrim(g.attrs ->> "
                )
                if "btrim(" in broken:
                    broken = broken.replace(f"btrim(g.attrs ->> '{attr}')", f"btrim(g.attrs ->> '{attr}'))")
                assert broken != probe.sql
                statement = (
                    f'SELECT g.ctid FROM public."{tables[kind]}" g WHERE {broken} '
                    "AND g.live_lo <= %s AND %s < g.live_hi"
                )
                plan = _probe_plan(cur, statement, (*probe.params, head, head))
                # The partial index may still serve the live_lo/live_hi
                # columns, but its key no longer bounds the scan.
                assert not _key_in_index_cond(plan, name, value_type), plan


_ZERO_DDL_DECLARED = {"pid": "num", "path": "text", "active": "bool", "seen_at": "ts"}


def test_declaring_filterable_executes_zero_ddl(clean_runtime: str) -> None:
    """Declaring an attribute filterable is catalog DATA, never DDL. A
    database-wide event trigger records every DDL command executed while the
    declaration is written; the recorded list must be empty. The declared
    indexes are built later by the reconciler (``okg db attr-index
    reconcile``), never by the declaration.

    Ported from ``test_attr_filter_index.py`` (deleted by okg#3112 step 4b,
    which removed okg.attr_index); migration 0021's comment still cites the
    old path and is not edited because an applied migration is pinned."""
    from tests.pytest.catalog_fixture_builders import SubtypeSpec, seed_catalog

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        with conn.cursor() as cur:
            # The scaffolding lives in the public schema, which the runtime
            # fixture does not truncate, so clear a leftover first.
            cur.execute("DROP EVENT TRIGGER IF EXISTS capture_ddl_trg")
            cur.execute("DROP TABLE IF EXISTS ddl_capture")
            cur.execute("DROP FUNCTION IF EXISTS capture_ddl()")
            cur.execute("CREATE TABLE ddl_capture (command_tag TEXT, object_identity TEXT)")
            cur.execute(
                "CREATE FUNCTION capture_ddl() RETURNS event_trigger "
                "LANGUAGE plpgsql AS $fn$ BEGIN "
                "  INSERT INTO ddl_capture (command_tag, object_identity) "
                "  SELECT command_tag, object_identity "
                "    FROM pg_event_trigger_ddl_commands(); "
                "END $fn$"
            )
            try:
                cur.execute(
                    "CREATE EVENT TRIGGER capture_ddl_trg ON ddl_command_end "
                    "EXECUTE FUNCTION capture_ddl()"
                )
            except psycopg.errors.InsufficientPrivilege:  # pragma: no cover
                pytest.skip("event triggers require a superuser connection")
        try:
            with psycopg.connect(clean_runtime, autocommit=True) as declare_conn:
                seed_catalog(
                    declare_conn,
                    subtypes=[
                        SubtypeSpec(
                            name="doc",
                            metadata={"filterable_attrs": dict(_ZERO_DDL_DECLARED)},
                        )
                    ],
                )
        finally:
            # Database-wide: disarm before anything else runs.
            with conn.cursor() as cur:
                cur.execute("DROP EVENT TRIGGER IF EXISTS capture_ddl_trg")
        with conn.cursor() as cur:
            cur.execute("SELECT command_tag, object_identity FROM ddl_capture")
            captured = cur.fetchall()
            cur.execute("DROP TABLE IF EXISTS ddl_capture")
            cur.execute("DROP FUNCTION IF EXISTS capture_ddl()")
        assert captured == [], (
            "declaring an attribute filterable executed DDL, breaking the "
            f"schema-as-data contract: {captured}"
        )
        # ...and the declaration is readable as a registry.
        with psycopg.connect(clean_runtime, autocommit=True) as read_conn:
            assert dai.declared_filterable_attrs(read_conn) == _ZERO_DDL_DECLARED
