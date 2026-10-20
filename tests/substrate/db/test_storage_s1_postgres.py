"""Storage redesign stage S1 in native PostgreSQL (PACT lane-s-redesign-s1).

* req.lane-s-storage-redesign.gin -- ``okg migrate`` builds the GIN
  (``jsonb_path_ops``) index on node attrs online with its exact definition,
  rebuilds an invalid or wrong one, defers it while the graph-index DDL lock
  is held (and reconciliation does not build it the blocking way), rotation
  leaves it alone, doctor reports it missing; containment selects the same
  rows as the per-key recompute for non-null scalars, and JSON null is the
  recorded exception (``test_gin_*``, ``test_containment_*``).
* req.lane-s-storage-redesign.statistics -- the reconciler creates one
  statistics object per declared attribute on a sized fixture, PostgreSQL's
  estimate is within 10x of the true distinct count, undeclaring drops it,
  a quoted key round-trips, an unowned prefixed object on the node table is
  dropped and a foreign one is reported (``test_statistics_*``).
* req.lane-s-storage-redesign.supporting-indexes -- ``okg migrate`` builds
  the node (subtype, node_id) and edge (edge_type, edge_id) B-trees
  online with their exact definitions, and a page of one type comes back in
  id order through them with a Limit and no Sort (``test_supporting_*``).
* req.lane-s-storage-redesign.plan-cache -- the doctor reads each read
  role's effective setting from ``pg_db_role_setting`` (``test_plan_cache_*``).
* req.lane-s-storage-redesign.order-and-rollback -- the rollback removes
  every S1 object (``test_rollback_*``).
* Review fixes -- deployed without migrate, a real publish cycle's rebuild
  leaves the GIN build to migrate; a failed GIN build does not block
  migrate; a tuned (``WITH (fastupdate = off)``) index is kept; statistics
  of another expression are redefined and a held name is missing; a
  malformed read DSN and an owner read DSN.

Roles are cluster-wide. These tests never change ``okg_mcp`` role-wide:
they override it only in this test's own database, and use a role of their
own (dropped afterwards) for the role-wide cases.
"""

from __future__ import annotations

import uuid
from typing import Any

import pytest

psycopg = pytest.importorskip("psycopg")

from psycopg.conninfo import conninfo_to_dict, make_conninfo  # noqa: E402
from psycopg.rows import dict_row  # noqa: E402
from psycopg.types.json import Jsonb  # noqa: E402

import declared_attr_index_support as s  # noqa: E402
from okg.substrate import declared_attr_indexes as dai  # noqa: E402
from okg.substrate.db import cold_history, management, readiness  # noqa: E402
from okg.substrate.db.migrate import migrate  # noqa: E402
from okg.substrate.library.sources.base import NodeFact  # noqa: E402
from okg.substrate.publisher import Publisher  # noqa: E402

pytestmark = pytest.mark.postgres

_GIN = cold_history.NODE_ATTRS_GIN_SPEC
_GIN_DEFINITION = "USING gin (attrs jsonb_path_ops) WHERE (deleted = false)"


def _node_table(dsn: str) -> str:
    return s.physical(dsn)["node"]


def _gin_name(dsn: str) -> str:
    return cold_history._full_index_name(_node_table(dsn), _GIN.suffix)


def _index(dsn: str, name: str) -> dict[str, Any] | None:
    with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            SELECT c.oid::bigint AS oid, (i.indisvalid AND i.indisready) AS valid,
                   pg_get_indexdef(i.indexrelid) AS definition
              FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid
             WHERE c.relname = %s
            """,
            (name,),
        )
        row = cur.fetchone()
    return dict(row) if row else None


def _seed_small(dsn: str, nodes: int = 200) -> Any:
    identity = s.seed(dsn, {"t_num": "num"})
    s.apply(dsn, [s.node(i, 1) for i in range(nodes)])
    return identity


def _drop(dsn: str, name: str) -> None:
    with psycopg.connect(dsn, autocommit=True) as conn:
        conn.execute(f'DROP INDEX IF EXISTS public."{name}"')


def _doctor_codes(dsn: str) -> dict[str, list[dict[str, Any]]]:
    report = management.database_health_report(dsn)
    out: dict[str, list[dict[str, Any]]] = {}
    for finding in report["findings"]:
        out.setdefault(str(finding.get("code")), []).append(finding)
    return out


# --------------------------------------------------------------------------- #
# GIN index                                                                    #
# --------------------------------------------------------------------------- #


def test_gin_index_exists_after_migrate_and_is_built_online(clean_runtime: str) -> None:
    dsn = clean_runtime
    _seed_small(dsn)
    name = _gin_name(dsn)
    fresh = _index(dsn, name)
    # A fresh database: schema reconciliation made the same definition.
    assert fresh is not None and fresh["valid"], fresh
    assert fresh["definition"].endswith(_GIN_DEFINITION), fresh

    _drop(dsn, name)
    report = cold_history.converge_interval_read_indexes(dsn, lock_timeout_ms=5000)
    assert report["status"] == "converged", report
    assert [item["index_name"] for item in report["created"]] == [name]
    built = _index(dsn, name)
    assert built is not None and built["valid"]
    assert built["definition"].endswith(_GIN_DEFINITION), built
    # Idempotent: neither a second online step nor a full migrate rebuilds it.
    again = cold_history.converge_interval_read_indexes(dsn, lock_timeout_ms=5000)
    assert again["created"] == []
    migrate(dsn)
    assert _index(dsn, name)["oid"] == built["oid"]


def test_gin_invalid_leftover_and_wrong_definition_are_rebuilt(clean_runtime: str) -> None:
    dsn = clean_runtime
    _seed_small(dsn)
    name = _gin_name(dsn)
    table = _node_table(dsn)
    _drop(dsn, name)
    with psycopg.connect(dsn, autocommit=True) as conn:
        # Every node has the same subtype: a unique build fails after the
        # catalog entry exists and leaves an invalid index under the name.
        with pytest.raises(psycopg.errors.UniqueViolation):
            conn.execute(f'CREATE UNIQUE INDEX CONCURRENTLY "{name}" ON public."{table}" (subtype)')
    assert _index(dsn, name)["valid"] is False
    (created,) = cold_history.converge_interval_read_indexes(dsn, lock_timeout_ms=5000)["created"]
    assert created["index_name"] == name and created["rebuilt_invalid"] is True
    assert _index(dsn, name)["definition"].endswith(_GIN_DEFINITION)

    # A valid index of another definition under the name (a B-tree, as a
    # spec tied to `rotate` would have left without its predicate).
    _drop(dsn, name)
    with psycopg.connect(dsn, autocommit=True) as conn:
        conn.execute(f'CREATE INDEX "{name}" ON public."{table}" USING gin (attrs jsonb_path_ops)')
    assert "WHERE" not in _index(dsn, name)["definition"]
    wrong = _doctor_codes(dsn).get("graph_read_index_wrong_definition", [])
    assert [f["index_name"] for f in wrong] == [name], wrong
    (created,) = cold_history.converge_interval_read_indexes(dsn, lock_timeout_ms=5000)["created"]
    assert created["index_name"] == name
    assert _index(dsn, name)["definition"].endswith(_GIN_DEFINITION)


def test_gin_build_defers_while_the_ddl_lock_is_held_and_doctor_reports_it(
    clean_runtime: str,
) -> None:
    dsn = clean_runtime
    _seed_small(dsn)
    name = _gin_name(dsn)
    _drop(dsn, name)
    missing = _doctor_codes(dsn).get("graph_read_index_missing", [])
    assert [f["index_name"] for f in missing] == [name], missing
    assert missing[0]["severity"] == "error"
    with psycopg.connect(dsn, autocommit=True) as other:
        other.execute("SELECT pg_advisory_lock(hashtext(%s))", (cold_history._INDEX_DDL_LOCK,))
        result = migrate(dsn)
    step = result.graph_read_indexes
    assert step["status"] == "deferred", step
    assert name in step["pending"]["build"], step
    # Reconciliation did not build it with a plain, write-blocking CREATE INDEX.
    assert _index(dsn, name) is None
    assert migrate(dsn).graph_read_indexes["status"] == "converged"
    assert _index(dsn, name)["valid"]
    assert "graph_read_index_missing" not in _doctor_codes(dsn)


def test_gin_index_is_left_alone_by_hot_index_rotation(clean_runtime: str) -> None:
    dsn = clean_runtime
    identity = _seed_small(dsn)
    s.publish(dsn, identity)
    # The rotation's floor needs more published checkpoints than its
    # headroom: three generations, headroom 1 (as the cold-history tests do).
    for rev in (2, 3):
        s.apply(dsn, [s.node(i, rev) for i in range(0, 200, 5)])
        s.publish(dsn, identity)
    before = _index(dsn, _gin_name(dsn))
    rotated = cold_history.rotate_hot_indexes(
        dsn,
        headroom_generations=1,
        logical_tables=["okg.graph_nodes"],
        suffixes=["node_id_visible"],
        activate=False,
    )
    # The rotation did run: it built a proof hot index for the B-tree spec.
    assert [(i["suffix"], i["state"]) for i in rotated["indexes"]] == [
        ("node_id_visible", "proof")
    ], rotated
    after = _index(dsn, _gin_name(dsn))
    assert after == before and after is not None and after["valid"], (before, after)


def test_containment_selects_the_recompute_rows_for_non_null_scalars(
    clean_runtime: str,
) -> None:
    dsn = clean_runtime
    s.seed(dsn, {})
    values: list[Any] = ["1", 1, 1.0, 1.5, True, False, [1, 2], {"n": 1}, None, "x"]
    facts = [
        NodeFact(f"s1:probe:{i:03d}", "thing", {"k": value, "row": i}, {"r": i}, {"v": 1})
        for i, value in enumerate(values)
    ]
    facts.append(NodeFact("s1:probe:nok", "thing", {"row": 99}, {"r": 99}, {"v": 1}))
    s.apply(dsn, facts)
    table = _node_table(dsn)

    def ids(cur: Any, predicate: str, value: Any) -> set[str]:
        cur.execute(
            f'SELECT node_id FROM public."{table}" '
            f"WHERE deleted = false AND node_id LIKE 's1:probe:%%' AND {predicate}",
            (Jsonb(value),),
        )
        return {str(r[0]) for r in cur.fetchall()}

    with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        for use_index in (False, True):
            # Correctness through the index and without it (test session only).
            cur.execute(f"SET enable_seqscan = {'off' if use_index else 'on'}")
            cur.execute(f"SET enable_bitmapscan = {'on' if use_index else 'off'}")
            for value in ("1", 1, 1.0, True, False, 1.5, "x"):
                contained = ids(cur, "attrs @> jsonb_build_object('k', %s::jsonb)", value)
                recompute = ids(cur, "(attrs -> 'k') = %s::jsonb", value)
                assert contained == recompute, (value, contained, recompute)
                assert "s1:probe:nok" not in contained
            assert ids(cur, "attrs @> jsonb_build_object('k', %s::jsonb)", "1") == {"s1:probe:000"}
            assert ids(cur, "attrs @> jsonb_build_object('k', %s::jsonb)", 1) == {
                "s1:probe:001", "s1:probe:002",
            }
            # JSON null: containment matches the null row, while the recompute
            # for a SQL null value matches nothing. This is why a null value
            # never uses the containment pre-filter.
            cur.execute(
                f'SELECT node_id FROM public."{table}" WHERE deleted = false '
                "AND node_id LIKE 's1:probe:%%' AND attrs @> jsonb_build_object('k', NULL)"
            )
            assert {str(r[0]) for r in cur.fetchall()} == {"s1:probe:008"}
            cur.execute(
                f'SELECT count(*) FROM public."{table}" WHERE deleted = false '
                "AND node_id LIKE 's1:probe:%%' AND (attrs -> 'k') = to_jsonb(NULL::text)"
            )
            assert cur.fetchone()[0] == 0


# --------------------------------------------------------------------------- #
# Supporting indexes                                                           #
# --------------------------------------------------------------------------- #

_NODE_PAGE = cold_history.NODE_SUBTYPE_NODE_ID_SPEC
_EDGE_PAGE = cold_history.EDGE_TYPE_EDGE_ID_SPEC
_PAGE_DEFINITIONS = {
    _NODE_PAGE.suffix: "USING btree (subtype, node_id, live_lo, live_hi) WHERE (deleted = false)",
    _EDGE_PAGE.suffix: (
        "USING btree (edge_type, edge_id, live_lo, live_hi) WHERE (deleted = false)"
    ),
}


def _page_names(dsn: str) -> dict[str, str]:
    tables = s.physical(dsn)
    return {
        _NODE_PAGE.suffix: cold_history._full_index_name(tables["node"], _NODE_PAGE.suffix),
        _EDGE_PAGE.suffix: cold_history._full_index_name(tables["edge"], _EDGE_PAGE.suffix),
    }


def _seed_graph(dsn: str) -> None:
    identity = s.seed(dsn)
    nodes, edges = s.generation_1()
    s.apply(dsn, [*nodes, *edges])
    s.publish(dsn, identity)


def test_supporting_indexes_exist_after_migrate_with_their_definitions(
    clean_runtime: str,
) -> None:
    dsn = clean_runtime
    _seed_graph(dsn)
    names = _page_names(dsn)
    for suffix, name in names.items():
        fresh = _index(dsn, name)
        # A fresh database: schema reconciliation made the same definition.
        assert fresh is not None and fresh["valid"], (suffix, fresh)
        assert fresh["definition"].endswith(_PAGE_DEFINITIONS[suffix]), fresh
        _drop(dsn, name)
    missing = _doctor_codes(dsn).get("graph_read_index_missing", [])
    assert sorted(f["index_name"] for f in missing) == sorted(names.values()), missing

    report = cold_history.converge_interval_read_indexes(dsn, lock_timeout_ms=5000)
    assert report["status"] == "converged", report
    assert sorted(item["index_name"] for item in report["created"]) == sorted(names.values())
    built = {}
    for suffix, name in names.items():
        built[name] = _index(dsn, name)
        assert built[name] is not None and built[name]["valid"], (suffix, built[name])
        assert built[name]["definition"].endswith(_PAGE_DEFINITIONS[suffix]), built[name]
    migrate(dsn)
    assert {name: _index(dsn, name)["oid"] for name in names.values()} == {
        name: row["oid"] for name, row in built.items()
    }
    assert "graph_read_index_missing" not in _doctor_codes(dsn)


def _plan_nodes(plan: dict[str, Any]) -> list[dict[str, Any]]:
    out = [plan]
    for child in plan.get("Plans", []):
        out.extend(_plan_nodes(child))
    return out


def _explain_page(dsn: str, statement: str) -> tuple[dict[str, Any], list[Any]]:
    with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(f"EXPLAIN (FORMAT JSON) {statement}")
        (plan,) = cur.fetchone()[0]
        cur.execute(statement)
        rows = [row[0] for row in cur.fetchall()]
    return plan["Plan"], rows


def test_supporting_indexes_return_one_type_in_id_order_without_a_sort(
    clean_runtime: str,
) -> None:
    """The page shapes: edges of one type in numeric edge-id order (dev's
    effective-view edge order since #3235), nodes of one subtype in node-id
    order, at a branch point, with a Limit and no Sort. Each plan names the
    S1 index with the type in its index condition, so an edge index keyed on
    the text id fails here as well as in the definition test. Swapped keys still give
    an ordered scan on this small fixture; the definition test catches
    those. The fixture is small but the planner picks the index without
    any planner setting changed."""
    dsn = clean_runtime
    _seed_graph(dsn)
    tables = s.physical(dsn)
    names = _page_names(dsn)
    with psycopg.connect(dsn, autocommit=True) as conn:
        for table in tables.values():
            conn.execute(f'ANALYZE public."{table}"')
        bp = int(conn.execute(
            f'SELECT max(live_lo) FROM public."{tables["edge"]}" WHERE NOT deleted'
        ).fetchone()[0])
    visible = f"deleted = false AND live_lo <= {bp} AND live_hi > {bp}"
    cases = [
        (
            _EDGE_PAGE.suffix,
            f'SELECT edge_id FROM public."{tables["edge"]}" '
            f"WHERE edge_type = 'rel' AND {visible} ORDER BY edge_id LIMIT 51",
            "edge_type",
        ),
        (
            _NODE_PAGE.suffix,
            f'SELECT node_id FROM public."{tables["node"]}" '
            f"WHERE subtype = 'other' AND {visible} ORDER BY node_id LIMIT 11",
            "subtype",
        ),
    ]
    for suffix, statement, type_column in cases:
        plan, rows = _explain_page(dsn, statement)
        nodes = _plan_nodes(plan)
        assert plan["Node Type"] == "Limit", plan
        assert not [n for n in nodes if "Sort" in n["Node Type"]], plan
        scans = [n for n in nodes if n.get("Index Name") == names[suffix]]
        assert [n["Node Type"] for n in scans] in (["Index Scan"], ["Index Only Scan"]), plan
        assert type_column in scans[0].get("Index Cond", ""), scans[0]
        assert rows and rows == sorted(rows), rows


# --------------------------------------------------------------------------- #
# Statistics objects                                                           #
# --------------------------------------------------------------------------- #

_QUOTED_KEY = "it's"


def _statistics(dsn: str) -> dict[str, dict[str, Any]]:
    with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            r"""
            SELECT s.stxname AS name, t.relname AS table_name,
                   pg_get_statisticsobjdef_expressions(s.oid) AS expressions
              FROM pg_statistic_ext s JOIN pg_class t ON t.oid = s.stxrelid
             WHERE s.stxname LIKE 'okg\_dstat\_%'
            """
        )
        return {str(r["name"]): dict(r) for r in cur.fetchall()}


def _deparsed(attr_id: str) -> str:
    """The statistics expression as PostgreSQL deparses it: the key literal
    gains ``::text``."""
    return dai.statistics_expression(attr_id).replace(
        dai._literal(attr_id), dai._literal(attr_id) + "::text"
    )


def _unwrapped(expression: str) -> str:
    expression = expression.strip()
    if expression.startswith("(") and expression.endswith(")"):
        return expression[1:-1]
    return expression


def test_statistics_objects_follow_the_declarations_on_a_sized_fixture(
    clean_runtime: str,
) -> None:
    dsn = clean_runtime
    identity = s.seed(dsn, {"grp": "num", _QUOTED_KEY: "num"})
    rows = 20_000
    for start in range(0, rows, 5000):
        s.apply(dsn, [
            NodeFact(f"s1:sized:{i:06d}", "thing", {"grp": i % 200, _QUOTED_KEY: i % 7},
                     {"r": i}, {"v": 1})
            for i in range(start, start + 5000)
        ])
    s.publish(dsn, identity)
    table = _node_table(dsn)
    grp, quoted = dai.statistics_name("node", "grp"), dai.statistics_name("node", _QUOTED_KEY)

    dry = dai.reconcile(dsn, deployment=s.DEPLOYMENT)
    assert sorted(dry["would_create_statistics"]) == sorted([grp, quoted])
    assert _statistics(dsn) == {}

    report = s.reconcile(dsn)
    assert report["status"] == "converged", report
    assert sorted(item["statistics_name"] for item in report["statistics_created"]) == sorted(
        [grp, quoted]
    )
    objects = _statistics(dsn)
    assert set(objects) == {grp, quoted}
    for name, key in ((grp, "grp"), (quoted, _QUOTED_KEY)):
        assert objects[name]["table_name"] == table
        assert [_unwrapped(e) for e in objects[name]["expressions"]] == [_deparsed(key)], (
            objects[name]
        )

    with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor(row_factory=dict_row) as cur:
        cur.execute(f'ANALYZE public."{table}"')
        cur.execute(
            "SELECT e.n_distinct, c.reltuples FROM pg_stats_ext_exprs e "
            "JOIN pg_class c ON c.relname = e.tablename "
            "WHERE e.statistics_name = %s",
            (grp,),
        )
        row = cur.fetchone()
    assert row is not None, "no expression statistics after ANALYZE"
    n_distinct = float(row["n_distinct"])
    estimate = -n_distinct * float(row["reltuples"]) if n_distinct < 0 else n_distinct
    assert 20 <= estimate <= 2000, row

    # An unowned prefixed object on the node table is dropped; one on another
    # table is foreign: reported, never touched.
    with psycopg.connect(dsn, autocommit=True) as conn:
        conn.execute(
            f"CREATE STATISTICS public.okg_dstat_n_manual ON (attrs -> 'zzz') FROM public.\"{table}\""
        )
        conn.execute("CREATE TABLE public.s1_foreign_stats (attrs jsonb)")
        conn.execute(
            "CREATE STATISTICS public.okg_dstat_n_foreign ON (attrs -> 'zzz') "
            "FROM public.s1_foreign_stats"
        )
    again = s.reconcile(dsn)
    assert again["statistics_dropped"] == ["okg_dstat_n_manual"]
    assert again["foreign_statistics"] == ["okg_dstat_n_foreign"]
    doctor = dai.doctor_report(dsn)
    foreign = [f for f in doctor["findings"] if f["code"] == "foreign_dattr_statistics"]
    assert [f["statistics"] for f in foreign] == ["okg_dstat_n_foreign"]

    # Undeclaring drops the object; the other declaration keeps its own.
    s.redeclare(dsn, {_QUOTED_KEY: "num"})
    undeclared = s.reconcile(dsn)
    assert undeclared["statistics_dropped"] == [grp], undeclared
    assert set(_statistics(dsn)) == {quoted, "okg_dstat_n_foreign"}


# --------------------------------------------------------------------------- #
# Plan-cache role check                                                        #
# --------------------------------------------------------------------------- #


def _database(dsn: str) -> str:
    with psycopg.connect(dsn, autocommit=True) as conn:
        return str(conn.execute("SELECT current_database()").fetchone()[0])


@pytest.fixture
def reader_role(clean_runtime: str) -> Any:
    """A login role of this test's own, without plan_cache_mode."""
    role = f"s1_reader_{uuid.uuid4().hex[:8]}"
    password = uuid.uuid4().hex
    database = _database(clean_runtime)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        conn.execute(f"CREATE ROLE {role} LOGIN PASSWORD '{password}'")
        conn.execute(f'GRANT CONNECT ON DATABASE "{database}" TO {role}')
    try:
        yield role, password
    finally:
        with psycopg.connect(clean_runtime, autocommit=True) as conn:
            conn.execute(f'REVOKE CONNECT ON DATABASE "{database}" FROM {role}')
            conn.execute(f"DROP ROLE IF EXISTS {role}")


def test_plan_cache_check_passes_on_a_migrated_database(clean_runtime: str) -> None:
    report = management.read_role_plan_cache_report(clean_runtime, env={})
    assert report["findings"] == [], report
    (mcp,) = [r for r in report["roles"] if r["role"] == "okg_mcp"]
    assert mcp["value"] == "force_custom_plan" and mcp["scope"] == "role"


def test_plan_cache_check_reports_an_in_database_override_of_okg_mcp(
    clean_runtime: str,
) -> None:
    database = _database(clean_runtime)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        conn.execute(f'ALTER ROLE okg_mcp IN DATABASE "{database}" SET plan_cache_mode = auto')
    try:
        report = management.read_role_plan_cache_report(clean_runtime, env={})
        (finding,) = report["findings"]
        assert finding["role"] == "okg_mcp" and finding["severity"] == "error"
        assert finding["observed"] == {"plan_cache_mode": "auto", "scope": "role_in_database"}
        assert "read_role_plan_cache_mode" in _doctor_codes(clean_runtime)
        assert management.database_health_report(clean_runtime)["ok"] is False
    finally:
        with psycopg.connect(clean_runtime, autocommit=True) as conn:
            conn.execute(f'ALTER ROLE okg_mcp IN DATABASE "{database}" RESET plan_cache_mode')


def test_plan_cache_check_names_the_read_dsn_role_and_never_the_owner(
    clean_runtime: str, reader_role: Any
) -> None:
    role, password = reader_role
    read_dsn = make_conninfo(clean_runtime, user=role, password=password)
    report = management.read_role_plan_cache_report(
        clean_runtime, env={"OKG_MCP_RO_DSN": read_dsn}
    )
    (finding,) = report["findings"]
    assert finding["role"] == role and finding["source"] == "OKG_MCP_RO_DSN"
    assert password not in str(report)

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        conn.execute(f"ALTER ROLE {role} SET plan_cache_mode = force_custom_plan")
    assert management.read_role_plan_cache_report(
        clean_runtime, env={"OKG_MCP_RO_DSN": read_dsn}
    )["findings"] == []

    owner = str(conninfo_to_dict(clean_runtime).get("user") or "")
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        me = str(conn.execute("SELECT current_user").fetchone()[0])
    owner_dsn = make_conninfo(clean_runtime, user=owner or me)
    owner_report = management.read_role_plan_cache_report(
        clean_runtime, env={"OKG_MCP_RO_DSN": owner_dsn}
    )
    # Never an error for the owner; a warning, since mcp-serve would read as it.
    assert [(f["code"], f["severity"]) for f in owner_report["findings"]] == [
        ("read_role_is_owner", "warning")
    ], owner_report


def test_mcp_serve_readiness_warns_for_a_read_dsn_role_with_generic_plans(
    clean_runtime: str, reader_role: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    role, password = reader_role
    monkeypatch.setenv(
        "OKG_MCP_RO_DSN", make_conninfo(clean_runtime, user=role, password=password)
    )
    warning = readiness.mcp_read_plan_cache_warning(clean_runtime, surface="okg mcp-serve")
    assert warning is not None and role in warning and "plan_cache_mode = auto" in warning
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        conn.execute(f"ALTER ROLE {role} SET plan_cache_mode = force_custom_plan")
    assert readiness.mcp_read_plan_cache_warning(clean_runtime, surface="okg mcp-serve") is None


def test_plan_cache_check_survives_a_malformed_read_dsn(clean_runtime: str) -> None:
    password = "Hunter2SecretPw"
    report = management.read_role_plan_cache_report(
        clean_runtime, env={"OKG_MCP_RO_DSN": f"host=db password='{password}"}
    )
    assert [f["code"] for f in report["findings"]] == ["read_role_unresolved"], report
    assert password not in str(report)


# --------------------------------------------------------------------------- #
# Review fixes: the GIN build path                                             #
# --------------------------------------------------------------------------- #


def test_deploy_without_migrate_leaves_the_gin_build_to_migrate(clean_runtime: str) -> None:
    """The S1 code running before `okg migrate` (the GIN index absent): the
    post-publish rebuild of a real publish cycle does not build it under the
    publisher lock; it reports it skipped, doctor reports it missing, and
    migrate then builds it."""
    dsn = clean_runtime
    identity = _seed_small(dsn)
    name = _gin_name(dsn)
    _drop(dsn, name)
    s.apply(dsn, [s.node(i, 2) for i in range(0, 200, 4)])
    worker = Publisher(dsn, s.DEPLOYMENT, catalog_identity=identity)
    worker.post_publish_index_rebuild_in_cycle = True
    summary = worker.cycle()
    assert summary["status"] == "published", summary
    convergence = summary["post_publish_index_rebuild"]["graph_index_convergence"]
    assert convergence["status"] == "partial", convergence
    assert convergence["skipped_extended"] == [name], convergence
    assert _index(dsn, name) is None
    assert [f["index_name"] for f in _doctor_codes(dsn)["graph_read_index_missing"]] == [name]
    assert migrate(dsn).graph_read_indexes["status"] == "converged"
    assert _index(dsn, name)["valid"]


def test_a_failed_gin_build_does_not_block_migrate(
    clean_runtime: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A GIN build that fails after its catalog entry exists (here a unique
    build over a duplicated column; disk full or out of memory in real
    life): migrate still applies the schema, the invalid leftover is
    dropped, reconciliation does not build the index the blocking way, and
    doctor reports it missing."""
    dsn = clean_runtime
    _seed_small(dsn)
    name = _gin_name(dsn)
    _drop(dsn, name)
    real = cold_history.spec_index_statement

    def failing(index_name: str, physical: str, spec: Any) -> Any:
        if spec.suffix != _GIN.suffix:
            return real(index_name, physical, spec)
        return psycopg.sql.SQL(
            "CREATE UNIQUE INDEX CONCURRENTLY {} ON {} (subtype)"
        ).format(psycopg.sql.Identifier(index_name), psycopg.sql.Identifier("public", physical))

    monkeypatch.setattr(cold_history, "spec_index_statement", failing)
    result = migrate(dsn)
    step = result.graph_read_indexes
    assert step["status"] == "partial", step
    (failure,) = step["failed"]
    assert failure["index_name"] == name and failure["sqlstate"] == "23505", failure
    assert failure["invalid_index_left"] == []
    assert name in step["pending"]["build"]
    assert _index(dsn, name) is None
    assert [f["index_name"] for f in _doctor_codes(dsn)["graph_read_index_missing"]] == [name]
    monkeypatch.setattr(cold_history, "spec_index_statement", real)
    assert migrate(dsn).graph_read_indexes["status"] == "converged"
    assert _index(dsn, name)["valid"]


def test_a_tuned_gin_index_is_kept(clean_runtime: str) -> None:
    """Storage parameters (`WITH (fastupdate=off)`) are not a wrong
    definition: migrate keeps the index and doctor passes it."""
    dsn = clean_runtime
    _seed_small(dsn)
    name = _gin_name(dsn)
    table = _node_table(dsn)
    _drop(dsn, name)
    with psycopg.connect(dsn, autocommit=True) as conn:
        conn.execute(
            f'CREATE INDEX "{name}" ON public."{table}" USING gin (attrs jsonb_path_ops) '
            "WITH (fastupdate = off) WHERE deleted = false"
        )
    before = _index(dsn, name)
    assert "fastupdate" in before["definition"], before
    assert cold_history.converge_interval_read_indexes(dsn, lock_timeout_ms=5000)["created"] == []
    migrate(dsn)
    assert _index(dsn, name)["oid"] == before["oid"]
    assert not {
        "graph_read_index_wrong_definition", "graph_read_index_missing"
    } & set(_doctor_codes(dsn))


def test_statistics_of_another_expression_are_redefined_and_a_held_name_is_missing(
    clean_runtime: str,
) -> None:
    dsn = clean_runtime
    identity = s.seed(dsn, {"grp": "num", "held": "num"})
    s.apply(dsn, [s.node(i, 1) for i in range(50)])
    s.publish(dsn, identity)
    table = _node_table(dsn)
    grp, held = dai.statistics_name("node", "grp"), dai.statistics_name("node", "held")
    with psycopg.connect(dsn, autocommit=True) as conn:
        conn.execute(f"CREATE STATISTICS public.{grp} ON (attrs -> 'other') FROM public.\"{table}\"")
        conn.execute("CREATE TABLE public.s1_held_stats (attrs jsonb)")
        conn.execute(f"CREATE STATISTICS public.{held} ON (attrs -> 'held') FROM public.s1_held_stats")
    report = s.reconcile(dsn)
    assert [item["statistics_name"] for item in report["statistics_redefined"]] == [grp], report
    assert report["blocked_statistics"] == [held]
    assert report["status"] == "partial", report
    objects = _statistics(dsn)
    assert [_unwrapped(e) for e in objects[grp]["expressions"]] == [_deparsed("grp")]
    assert objects[held]["table_name"] == "s1_held_stats"
    doctor = dai.doctor_report(dsn)
    missing = [f["statistics"] for f in doctor["findings"] if f["code"] == "statistics_missing"]
    assert missing == [held], doctor["findings"]
    with psycopg.connect(dsn, autocommit=True) as conn:
        conn.execute(f"DROP STATISTICS public.{held}")
        conn.execute("DROP TABLE public.s1_held_stats")


# --------------------------------------------------------------------------- #
# Rollback                                                                     #
# --------------------------------------------------------------------------- #


def test_rollback_removes_every_s1_object(
    clean_runtime: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    dsn = clean_runtime
    identity = s.seed(dsn, {"t_num": "num", "t_text": "text"})
    s.apply(dsn, [s.node(i, 1) for i in range(100)])
    s.publish(dsn, identity)
    assert s.reconcile(dsn)["status"] in {"converged", "partial"}
    tables = s.physical(dsn)
    s1_specs = {
        "okg.graph_nodes": (_GIN, _NODE_PAGE),
        "okg.graph_edges": (_EDGE_PAGE,),
    }
    kind = {"okg.graph_nodes": "node", "okg.graph_edges": "edge"}
    names = sorted(
        cold_history._full_index_name(tables[kind[logical]], spec.suffix)
        for logical, specs in s1_specs.items()
        for spec in specs
    )
    assert all(_index(dsn, name) is not None for name in names), names
    assert len(_statistics(dsn)) == 2

    # A plain revert drops the specs but not the indexes: the failing case.
    for logical, specs in s1_specs.items():
        suffixes = {spec.suffix for spec in specs}
        monkeypatch.setitem(
            cold_history.INTERVAL_SOURCE_READ_INDEX_SPECS,
            logical,
            tuple(
                spec for spec in cold_history.INTERVAL_SOURCE_READ_INDEX_SPECS[logical]
                if spec.suffix not in suffixes
            ),
        )
    migrate(dsn)
    assert all(_index(dsn, name) is not None for name in names), names

    # The rollback: the suffixes on the retired list, then migrate.
    for logical, specs in s1_specs.items():
        retired = cold_history.RETIRED_INTERVAL_SOURCE_READ_INDEX_SUFFIXES[logical]
        monkeypatch.setitem(
            cold_history.RETIRED_INTERVAL_SOURCE_READ_INDEX_SUFFIXES,
            logical,
            (*retired, *(spec.suffix for spec in specs)),
        )
    result = migrate(dsn)
    assert sorted(d["index_name"] for d in result.graph_read_indexes["dropped"]) == names
    assert all(_index(dsn, name) is None for name in names), names
    with psycopg.connect(dsn, autocommit=True) as conn:
        conn.execute(dai.DROP_ALL_STATISTICS_SQL)
    assert _statistics(dsn) == {}
    report = management.database_health_report(dsn)
    assert report["ok"] is True, report["findings"]
