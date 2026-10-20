"""Storage redesign stage S1 without a database (PACT lane-s-redesign-s1).

No test here connects to PostgreSQL. The native PostgreSQL half lives in
``test_storage_s1_postgres.py``.

* req.lane-s-storage-redesign.gin -- the GIN spec's statement and
  definition match; the existing plain specs keep the exact SQL okg#3112 step
  1 built (online and in schema reconciliation); an extended spec cannot be
  rotated; rotation, the reclaim advisory and the index-reclaim census leave
  the GIN index alone.
* req.lane-s-storage-redesign.supporting-indexes -- the node (subtype,
  node_id) and edge (edge_type, edge_id) specs render their definitions,
  and a text cast or swapped key is a wrong definition.
* req.lane-s-storage-redesign.statistics -- the statistics expression is
  defined once and is what the DDL inlines; hostile keys are quoted; the
  plan creates, drops and reports foreign objects.
* req.lane-s-storage-redesign.plan-cache -- the effective setting follows
  PostgreSQL's precedence (role in database before role-wide); findings name
  the role and value; the owner is never reported; mcp-serve readiness warns
  and never refuses.
* Review fixes -- the post-publish rebuild never builds an extended spec; a
  failed extended build is reported and the convergence continues; storage
  parameters are not a wrong definition; a malformed read DSN is a finding
  without its text; a read DSN naming the owner is a warning; ``ALTER ROLE
  ALL`` precedence; statistics of another expression are redefined and a
  held name is missing.
"""

from __future__ import annotations

import contextlib
from types import SimpleNamespace
from typing import Any

import pytest
from psycopg import sql

from okg.substrate import declared_attr_indexes as dai
from okg.substrate.db import cold_history, index_reclaim, management, readiness
from okg.substrate.errors import SubstrateError
from okg.substrate.graph_store_internal.common import (
    chronos_interval_index_name,
    quote_ident,
    quote_physical_table,
)
from okg.substrate.graph_store_internal.schema import ChronosGraphSchemaMixin

_GIN = cold_history.NODE_ATTRS_GIN_SPEC
_NODE_PAGE = cold_history.NODE_SUBTYPE_NODE_ID_SPEC
_EDGE_PAGE = cold_history.EDGE_TYPE_EDGE_ID_SPEC
#: Every spec PACT lane-s-redesign-s1 adds, in spec order.
_S1_LOGICAL = (
    ("okg.graph_nodes", _GIN),
    ("okg.graph_nodes", _NODE_PAGE),
    ("okg.graph_edges", _EDGE_PAGE),
)
_S1_SPECS = tuple(spec for _logical, spec in _S1_LOGICAL)
_PHYSICAL = {
    "okg.graph_nodes": "_chronos_b_interval_okg_graph_nodes",
    "okg.graph_edges": "_chronos_b_interval_okg_graph_edges",
}


def _plain_specs() -> list[tuple[str, cold_history.VisibilityIndexSpec]]:
    return [
        (logical, spec)
        for logical, specs in cold_history.INTERVAL_SOURCE_READ_INDEX_SPECS.items()
        for spec in specs
        if not spec.extended
    ]


# --------------------------------------------------------------------------- #
# GIN spec                                                                     #
# --------------------------------------------------------------------------- #


def _dev_online_statement(name: str, physical: str, spec: Any) -> str:
    """The statement okg#3112 step 1 composed for a spec, before S1."""
    columns = sql.SQL(", ").join(sql.Identifier(column) for column in spec.columns)
    predicate = sql.SQL(" WHERE deleted = FALSE") if spec.rotate else sql.SQL("")
    return sql.SQL("CREATE INDEX CONCURRENTLY IF NOT EXISTS {} ON {} ({}){}").format(
        sql.Identifier(name), sql.Identifier("public", physical), columns, predicate,
    ).as_string(None)


def test_existing_specs_build_online_with_byte_identical_sql() -> None:
    plain = _plain_specs()
    assert len(plain) == 12, [spec.suffix for _logical, spec in plain]
    for logical, spec in plain:
        physical = _PHYSICAL[logical]
        name = cold_history._full_index_name(physical, spec.suffix)
        got = cold_history.spec_index_statement(name, physical, spec).as_string(None)
        assert got == _dev_online_statement(name, physical, spec), spec.suffix


class _StopAfterSpecs(Exception):
    pass


class _FakeResult:
    def __init__(self, rows: list[dict[str, Any]] | None = None) -> None:
        self._rows = rows or []

    def fetchall(self) -> list[dict[str, Any]]:
        return list(self._rows)

    def fetchone(self) -> dict[str, Any] | None:
        return self._rows[0] if self._rows else None


def _reconcile_statements(existing_defs: dict[str, str] | None = None) -> list[str]:
    """Drive schema reconciliation's spec loop on a fake database and return
    every statement it sent, stopping at the first fixed expression index
    (the loop that follows the specs)."""
    statements: list[str] = []
    existing_defs = existing_defs or {}

    def execute(statement: str, params: Any = None) -> _FakeResult:
        if "node_subtype_path_visible" in statement:
            raise _StopAfterSpecs()
        statements.append(statement)
        if "FROM pg_indexes" in statement:
            name = params[0]
            if name in existing_defs:
                return _FakeResult([{"indexdef": existing_defs[name]}])
        return _FakeResult()

    tables = {
        logical: {
            "physical_table": physical,
            "columns": (
                "node_id", "edge_id", "subtype", "source", "run_id", "src",
                "dst", "edge_type", "attrs",
            ),
        }
        for logical, physical in _PHYSICAL.items()
    }
    fake = SimpleNamespace(
        context=SimpleNamespace(backend_name="interval", db=SimpleNamespace(execute=execute)),
        _interval_table_metadata=lambda: tables,
    )
    with pytest.raises(_StopAfterSpecs):
        ChronosGraphSchemaMixin._ensure_interval_source_read_indexes(fake)  # type: ignore[arg-type]
    return statements


def _dev_reconcile_sql(name: str, physical: str, spec: Any) -> str:
    """The f-string schema reconciliation rendered for a spec, before S1."""
    quoted_columns = ", ".join(quote_ident(column) for column in spec.columns)
    return (
        "\n" + " " * 20 + f"CREATE INDEX IF NOT EXISTS {quote_ident(name)}\n"
        + " " * 22 + f"ON {quote_physical_table(physical)} ({quoted_columns})\n"
        + " " * 20 + ("WHERE deleted = FALSE" if spec.rotate else "") + "\n"
        + " " * 20
    )


def test_existing_specs_reconcile_with_byte_identical_sql() -> None:
    statements = _reconcile_statements()
    creates = [s for s in statements if "CREATE INDEX IF NOT EXISTS" in s]
    for logical, spec in _plain_specs():
        physical = _PHYSICAL[logical]
        name = chronos_interval_index_name(physical, spec.suffix)
        assert _dev_reconcile_sql(name, physical, spec) in creates, spec.suffix


def test_gin_spec_is_a_live_rows_jsonb_path_ops_index_on_node_attrs() -> None:
    assert _GIN in cold_history.INTERVAL_SOURCE_READ_INDEX_SPECS["okg.graph_nodes"]
    assert all(
        spec.suffix != _GIN.suffix
        for spec in cold_history.INTERVAL_SOURCE_READ_INDEX_SPECS["okg.graph_edges"]
    )
    assert _GIN.rotate is False and _GIN.extended
    physical = _PHYSICAL["okg.graph_nodes"]
    name = cold_history._full_index_name(physical, _GIN.suffix)
    assert len(name) <= 63
    online = cold_history.spec_index_statement(name, physical, _GIN).as_string(None)
    assert online == (
        f'CREATE INDEX CONCURRENTLY IF NOT EXISTS "{name}" ON "public"."{physical}" '
        'USING gin ("attrs" jsonb_path_ops) WHERE deleted = FALSE'
    )
    (reconciled,) = [
        s for s in _reconcile_statements() if "CREATE INDEX" in s and name in s
    ]
    assert 'USING gin ("attrs" jsonb_path_ops)' in reconciled
    assert "WHERE deleted = FALSE" in reconciled


def test_supporting_specs_are_the_prototype_page_indexes() -> None:
    """Requirement supporting-indexes: the node (subtype, node_id) and edge
    (edge_type, edge_id) B-trees, keyed and filtered for one-type pages in
    id order (numeric for edges since #3235), built online and by
    reconciliation alike."""
    assert _NODE_PAGE in cold_history.INTERVAL_SOURCE_READ_INDEX_SPECS["okg.graph_nodes"]
    assert _EDGE_PAGE in cold_history.INTERVAL_SOURCE_READ_INDEX_SPECS["okg.graph_edges"]
    for spec in (_NODE_PAGE, _EDGE_PAGE):
        assert spec.rotate is False and spec.extended and spec.method is None
    expected = {
        _NODE_PAGE: '("subtype", "node_id", "live_lo", "live_hi") WHERE deleted = FALSE',
        _EDGE_PAGE: '("edge_type", "edge_id", "live_lo", "live_hi") WHERE deleted = FALSE',
    }
    reconciled = _reconcile_statements()
    for logical, spec in (("okg.graph_nodes", _NODE_PAGE), ("okg.graph_edges", _EDGE_PAGE)):
        physical = _PHYSICAL[logical]
        name = cold_history._full_index_name(physical, spec.suffix)
        assert len(name) <= 63
        online = cold_history.spec_index_statement(name, physical, spec).as_string(None)
        assert online == (
            f'CREATE INDEX CONCURRENTLY IF NOT EXISTS "{name}" ON "public"."{physical}" '
            + expected[spec]
        ), online
        (create,) = [s for s in reconciled if "CREATE INDEX" in s and name in s]
        assert "USING" not in create and "WHERE deleted = FALSE" in create, create
    match = cold_history.spec_index_definition_matches
    assert match(
        "CREATE INDEX i ON public._t USING btree (subtype, node_id, live_lo, live_hi) "
        "WHERE (deleted = false)",
        _NODE_PAGE,
    ) is True
    assert match(
        "CREATE INDEX i ON public._t USING btree (edge_type, edge_id, live_lo, live_hi) "
        "WHERE (deleted = false)",
        _EDGE_PAGE,
    ) is True
    for wrong, spec in (
        # The text cast (the order before #3235), which the numeric
        # edge-id page cannot use.
        ("USING btree (edge_type, ((edge_id)::text), live_lo, live_hi) WHERE (deleted = false)",
         _EDGE_PAGE),
        ("USING btree (edge_id, edge_type, live_lo, live_hi) WHERE (deleted = false)",
         _EDGE_PAGE),
        ("USING btree (edge_type, edge_id, live_lo, live_hi)", _EDGE_PAGE),
        ("USING btree (node_id, subtype, live_lo, live_hi) WHERE (deleted = false)", _NODE_PAGE),
        ("USING btree (subtype, node_id) WHERE (deleted = false)", _NODE_PAGE),
        ("USING btree (subtype, live_lo, live_hi) WHERE (deleted = false)", _NODE_PAGE),
    ):
        assert match(f"CREATE INDEX i ON public._t {wrong}", spec) is False, wrong


def test_a_predicate_tied_to_rotate_would_drop_the_live_rows_filter() -> None:
    """The failing case the GIN spec's own predicate exists for: today's
    rule gives a non-rotatable spec no predicate at all."""
    tied = cold_history.VisibilityIndexSpec("x", ("attrs",), rotate=False)
    statement = cold_history.spec_index_statement("x", "t", tied).as_string(None)
    assert "WHERE" not in statement
    assert cold_history.spec_index_definition_matches(
        "CREATE INDEX x ON public.t USING gin (attrs jsonb_path_ops)", _GIN
    ) is False


def test_spec_definition_matches_postgres_spelling() -> None:
    match = cold_history.spec_index_definition_matches
    assert match(
        "CREATE INDEX idx ON public._t USING gin (attrs jsonb_path_ops) WHERE (deleted = false)",
        _GIN,
    ) is True
    for wrong in (
        "CREATE INDEX idx ON public._t USING btree (attrs) WHERE (deleted = false)",
        "CREATE INDEX idx ON public._t USING gin (attrs) WHERE (deleted = false)",
        "CREATE INDEX idx ON public._t USING gin (attrs jsonb_path_ops)",
        "CREATE INDEX idx ON public._t USING gin (attrs jsonb_path_ops) WHERE (NOT deleted)",
    ):
        assert match(wrong, _GIN) is False, wrong
    assert match(None, _GIN) is None
    # A plain spec's B-tree definition matches too.
    plain = cold_history.INTERVAL_SOURCE_READ_INDEX_SPECS["okg.graph_nodes"][1]
    assert match(
        "CREATE INDEX i ON public._t USING btree (node_id, live_lo, live_hi) WHERE (deleted = false)",
        plain,
    ) is True


def test_expression_key_spec_renders_and_matches() -> None:
    edge = cold_history.VisibilityIndexSpec(
        "expression_key_example",
        ("edge_type", "edge_id"),
        rotate=False,
        key_expressions=(("edge_id", "(edge_id::text)"),),
        predicate="deleted = FALSE",
    )
    statement = cold_history.spec_index_statement("i", "t", edge).as_string(None)
    assert statement.endswith('("edge_type", (edge_id::text)) WHERE deleted = FALSE')
    assert "USING" not in statement
    assert cold_history.spec_index_definition_matches(
        "CREATE INDEX i ON public.t USING btree (edge_type, ((edge_id)::text)) WHERE (deleted = false)",
        edge,
    ) is True


@pytest.mark.parametrize(
    "kwargs",
    [
        {"method": "gin"},
        {"opclass": "jsonb_path_ops"},
        {"predicate": "deleted = FALSE"},
        {"key_expressions": (("attrs", "(attrs)"),)},
    ],
)
def test_an_extended_spec_cannot_be_rotatable(kwargs: dict[str, Any]) -> None:
    with pytest.raises(ValueError, match="cannot be rotatable"):
        cold_history.VisibilityIndexSpec("x", ("attrs",), rotate=True, **kwargs)


@pytest.mark.parametrize(
    "kwargs",
    [
        {"method": "gin; drop table x"},
        {"opclass": "Jsonb Path"},
        {"key_expressions": (("other", "(x)"),)},
        {"key_expressions": (("attrs", "attrs::text"),)},
        {"predicate": "true; select 1"},
    ],
)
def test_spec_fields_inlined_into_ddl_are_validated(kwargs: dict[str, Any]) -> None:
    with pytest.raises(ValueError):
        cold_history.VisibilityIndexSpec("x", ("attrs",), rotate=False, **kwargs)


def test_a_valid_index_of_another_definition_is_rebuilt_only_for_extended_specs() -> None:
    plain = cold_history.INTERVAL_SOURCE_READ_INDEX_SPECS["okg.graph_nodes"][1]
    wrong = {"indisvalid": True, "indisready": True,
             "indexdef": "CREATE INDEX i ON public.t USING btree (attrs)"}
    assert cold_history._spec_index_current(wrong, _GIN) is False
    # A plain spec keeps okg#3112 step 1's rule: validity alone.
    assert cold_history._spec_index_current(wrong, plain) is True
    good = {**wrong, "indexdef": (
        "CREATE INDEX i ON public.t USING gin (attrs jsonb_path_ops) WHERE (deleted = false)"
    )}
    assert cold_history._spec_index_current(good, _GIN) is True
    assert cold_history._spec_index_current({**good, "indisvalid": False}, _GIN) is False
    assert cold_history._spec_index_current(None, _GIN) is False


def test_doctor_index_check_covers_only_the_extended_specs(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """S1 reports the GIN index (and any later extended spec); a missing
    plain spec index or a retired one still present stays unreported, as
    before S1."""
    gin = cold_history._full_index_name(_PHYSICAL["okg.graph_nodes"], _GIN.suffix)
    plain = cold_history._full_index_name(_PHYSICAL["okg.graph_nodes"], "node_id_visible")
    retired = cold_history._full_index_name(
        _PHYSICAL["okg.graph_nodes"], "node_live_lo_visible"
    )

    @contextlib.contextmanager
    def connect(*_args: Any, **_kwargs: Any) -> Any:
        @contextlib.contextmanager
        def cursor() -> Any:
            yield object()

        yield SimpleNamespace(cursor=cursor)

    monkeypatch.setattr(cold_history, "db_connect", connect)
    monkeypatch.setattr(cold_history, "registered_graph_tables", lambda _cur: dict(_PHYSICAL))
    monkeypatch.setattr(
        cold_history,
        "pending_interval_index_changes",
        lambda _cur, _tables: {"build": [plain, gin], "drop": [retired]},
    )
    monkeypatch.setattr(cold_history, "_index_catalog_row", lambda _cur, _name: None)
    findings = cold_history.graph_read_index_findings("postgresql://unused.invalid/none")
    assert [(f["code"], f["index_name"]) for f in findings] == [
        ("graph_read_index_missing", gin)
    ]
    assert findings[0]["severity"] == "error"


def test_hot_index_rotation_refuses_the_gin_index() -> None:
    """Rotation only rebuilds rotatable B-trees; it refuses the GIN suffix
    before it connects."""
    with pytest.raises(SubstrateError, match="non-rotatable"):
        cold_history.rotate_hot_indexes(
            "postgresql://unused.invalid/none", suffixes=[_GIN.suffix]
        )


def test_reclaim_advisory_never_counts_the_gin_index() -> None:
    report = {
        "floor": {"floor": 10, "headroom_generations": 100},
        "indexes": [{
            "logical_table": "okg.graph_nodes",
            "physical_table": _PHYSICAL["okg.graph_nodes"],
            "suffix": _GIN.suffix,
            "rotatable": _GIN.rotate,
            "full_index": {"index_name": "g", "bytes": 10**9,
                           "indisvalid": True, "indisready": True},
        }],
    }
    advisory = cold_history.summarize_cold_history_reclaim(report, deployment="d")
    assert advisory["status"] == "no_candidates"
    assert advisory["rotatable_full_index_bytes"] == 0


def test_index_reclaim_census_does_not_judge_a_gin_index() -> None:
    class Cursor:
        def execute(self, *_args: Any) -> None:
            return None

        def fetchall(self) -> list[dict[str, Any]]:
            return [{
                "table_schema": "public", "table_name": "_t", "index_schema": "public",
                "index_name": "g", "access_method": "gin", "valid": True,
                "partial": True, "bytes": 10**9, "index_tuples": 10.0,
                "table_tuples": 10.0, "fillfactor": 90, "widths": [None],
            }]

    (item,) = index_reclaim.judge_indexes(
        Cursor(), ["public._t"], ratio=1.5, min_reclaim_bytes=1
    )
    assert item["verdict"] == "not_btree"


# --------------------------------------------------------------------------- #
# Statistics objects                                                           #
# --------------------------------------------------------------------------- #

_HOSTILE_KEYS = [
    "plain",
    "it's",
    "''",
    "back\\slash",
    'dq"uote',
    "semi; DROP TABLE x; --",
    "$$dollar$$",
    "Ünïcode ✓",
    " lead and trail ",
]


def test_statistics_expression_is_defined_once_and_inlined_by_the_ddl() -> None:
    for key in _HOSTILE_KEYS:
        expression = dai.statistics_expression(key)
        assert expression == "attrs -> '" + key.replace("'", "''") + "'"
        ddl = dai.statistics_ddl(physical_table="tbl", target_kind="node", attr_id=key)
        assert f" ON ({expression}) FROM " in ddl, ddl
        assert ddl.startswith(f'CREATE STATISTICS "public"."{dai.statistics_name("node", key)}"')
        assert ddl.endswith(' FROM "public"."tbl"')
    assert dai.statistics_expression("k", alias="g") == "g.attrs -> 'k'"


def test_a_key_with_a_nul_character_is_refused() -> None:
    with pytest.raises(SubstrateError):
        dai.statistics_expression("a\x00b")


def test_statistics_names_use_the_reserved_prefix_and_differ_per_key() -> None:
    names = {dai.statistics_name("node", key) for key in _HOSTILE_KEYS}
    assert len(names) == len(_HOSTILE_KEYS)
    for name in names:
        assert name.startswith("okg_dstat_n_") and len(name) <= 63
    assert not dai.statistics_name("node", "k").startswith(dai.INDEX_NAME_PREFIX)
    assert "okg\\_dstat\\_" in dai.DROP_ALL_STATISTICS_SQL


def test_desired_statistics_cover_every_declared_node_attribute_only() -> None:
    tables = {"node": "_n", "edge": "_e"}
    desired = dai.desired_statistics({"a": "text", "b": "num", "c": "ts"}, tables)
    assert [(d.target_kind, d.attr_id, d.physical_table) for d in desired] == [
        ("node", "a", "_n"), ("node", "b", "_n"), ("node", "c", "_n"),
    ]


def test_statistics_plan_creates_drops_and_reports_foreign_objects() -> None:
    tables = {"node": "_n", "edge": "_e"}
    desired = dai.desired_statistics({"keep": "text", "new": "num"}, tables)
    keep = dai.statistics_name("node", "keep")
    gone = dai.statistics_name("node", "gone")

    def row(table: str, schema: str = "public") -> dict[str, Any]:
        return {"schema": schema, "table_name": table, "table_schema": schema}

    catalog = {
        keep: row("_n"),
        gone: row("_n"),
        "okg_dstat_n_elsewhere": row("someone_else"),
        "okg_dstat_n_other_schema": row("_n", schema="okg"),
    }
    plan = dai._statistics_plan(catalog, desired, tables)
    assert plan["create"] == [dai.statistics_name("node", "new")]
    assert plan["drop"] == [gone]
    assert plan["foreign"] == ["okg_dstat_n_elsewhere", "okg_dstat_n_other_schema"]


# --------------------------------------------------------------------------- #
# Plan-cache role check                                                        #
# --------------------------------------------------------------------------- #


def test_effective_setting_prefers_the_role_in_this_database() -> None:
    rows = [
        {"scope": "role", "setconfig": ["plan_cache_mode=force_custom_plan"]},
        {"scope": "role_in_database", "setconfig": ["work_mem=4MB", "plan_cache_mode=auto"]},
        {"scope": "database", "setconfig": ["plan_cache_mode=force_generic_plan"]},
    ]
    assert management.effective_role_setting(rows, "plan_cache_mode") == (
        "auto", "role_in_database"
    )
    assert management.effective_role_setting(rows[:1], "plan_cache_mode") == (
        "force_custom_plan", "role"
    )
    assert management.effective_role_setting(rows[2:], "plan_cache_mode") == (
        "force_generic_plan", "database"
    )
    assert management.effective_role_setting([], "plan_cache_mode") == (None, None)


def test_an_in_database_override_is_reported_where_rolconfig_alone_passes() -> None:
    """The failing case for a check that reads only ``rolconfig`` (the
    role-wide row, which is what the pre-S1 check read): it sees
    force_custom_plan and passes, while PostgreSQL logs the role in with
    ``auto``."""
    rows = [
        {"scope": "role", "setconfig": ["plan_cache_mode=force_custom_plan"]},
        {"scope": "role_in_database", "setconfig": ["plan_cache_mode=auto"]},
    ]
    rolconfig_only = rows[0]["setconfig"]
    assert "plan_cache_mode=force_custom_plan" in rolconfig_only
    value, scope = management.effective_role_setting(rows, "plan_cache_mode")
    (finding,) = management.plan_cache_role_findings(
        [{"role": "okg_mcp", "source": "mcp_read_role", "exists": True,
          "owner": False, "value": value, "scope": scope}],
        database="okg",
    )
    assert finding["severity"] == "error" and finding["role"] == "okg_mcp"
    assert finding["observed"] == {"plan_cache_mode": "auto", "scope": "role_in_database"}
    assert 'IN DATABASE "okg" RESET plan_cache_mode' in finding["fix_hint"]


def test_plan_cache_findings_cover_every_read_role_and_never_the_owner() -> None:
    roles = [
        {"role": "okg_mcp", "source": "mcp_read_role", "exists": True, "owner": False,
         "value": "force_custom_plan", "scope": "role"},
        {"role": "projection_remediator", "source": "projection_read_role",
         "exists": False, "owner": False, "value": None, "scope": None},
        {"role": "managed_reader", "source": "OKG_MCP_RO_DSN", "exists": True,
         "owner": False, "value": None, "scope": None},
        {"role": "okg_owner", "source": "mcp_read_role", "exists": True, "owner": True,
         "value": None, "scope": None},
        {"role": "gone_reader", "source": "OKG_MCP_RO_DSN", "exists": False,
         "owner": False, "value": None, "scope": None},
    ]
    findings = management.plan_cache_role_findings(roles, database="okg")
    by_role = {f["role"]: f for f in findings}
    assert set(by_role) == {"managed_reader", "gone_reader"}
    assert by_role["managed_reader"]["code"] == "read_role_plan_cache_mode"
    assert by_role["managed_reader"]["observed"]["plan_cache_mode"].startswith("unknown")
    assert by_role["managed_reader"]["fix_hint"] == (
        'ALTER ROLE "managed_reader" SET plan_cache_mode = force_custom_plan'
    )
    assert by_role["gone_reader"]["code"] == "read_role_missing"


def test_a_server_wide_force_custom_plan_counts_and_a_server_wide_auto_is_reported() -> None:
    """postgresql.conf or ALTER SYSTEM: the last step of PostgreSQL's order,
    used only when no role or database row sets the value."""
    server_force = management.server_setting_value(
        {"setting": "force_custom_plan", "source": "configuration file"}
    )
    assert server_force == "force_custom_plan"
    value, scope = management.effective_role_setting([], "plan_cache_mode", server=server_force)
    assert (value, scope) == ("force_custom_plan", "server")
    assert management.plan_cache_role_findings(
        [{"role": "okg_mcp", "source": "mcp_read_role", "exists": True, "owner": False,
          "value": value, "scope": scope}],
        database="okg",
    ) == []

    server_auto = management.server_setting_value({"setting": "auto", "source": "default"})
    value, scope = management.effective_role_setting([], "plan_cache_mode", server=server_auto)
    (finding,) = management.plan_cache_role_findings(
        [{"role": "okg_mcp", "source": "mcp_read_role", "exists": True, "owner": False,
          "value": value, "scope": scope}],
        database="okg",
    )
    assert finding["role"] == "okg_mcp" and finding["severity"] == "error"
    assert finding["observed"] == {"plan_cache_mode": "auto", "scope": "server"}

    # A role row still wins over the server value.
    assert management.effective_role_setting(
        [{"scope": "role", "setconfig": ["plan_cache_mode=force_custom_plan"]}],
        "plan_cache_mode",
        server="auto",
    ) == ("force_custom_plan", "role")
    # The doctor's own role or session setting hides the server value.
    for source in ("user", "database", "database user", "session", "client"):
        assert management.server_setting_value(
            {"setting": "force_custom_plan", "source": source}
        ) is None
    assert management.server_setting_value(
        {"setting": "force_custom_plan", "source": "override"}
    ) == "force_custom_plan"


def test_read_roles_include_the_login_role_of_the_read_dsn() -> None:
    # projection_remediator is NOLOGIN: a role setting never applies to it.
    assert [r["role"] for r in management.read_roles_to_check({})] == ["okg_mcp"]
    roles = management.read_roles_to_check(
        {"OKG_MCP_RO_DSN": f"postgresql://managed_reader:{_FAKE_PASSWORD}@db.example/okg"}
    )
    assert roles[-1] == {"role": "managed_reader", "source": "OKG_MCP_RO_DSN"}
    same = management.read_roles_to_check({"OKG_MCP_RO_DSN": "host=db user=okg_mcp"})
    assert [r["role"] for r in same] == ["okg_mcp"]
    libpq_default = management.read_roles_to_check(
        {"OKG_MCP_RO_DSN": "host=db dbname=okg", "PGUSER": "pg_env_reader"}
    )
    assert libpq_default[-1]["role"] == "pg_env_reader"


class _FakeConnection:
    def __init__(self, row: tuple[str, str]) -> None:
        self.row = row
        self.statements: list[str] = []

    def __enter__(self) -> "_FakeConnection":
        return self

    def __exit__(self, *_exc: Any) -> None:
        return None

    @contextlib.contextmanager
    def cursor(self) -> Any:
        conn = self

        class Cursor:
            def execute(self, statement: str) -> None:
                conn.statements.append(statement)

            def fetchone(self) -> tuple[str, str]:
                return conn.row

        yield Cursor()


def test_mcp_serve_readiness_warns_for_a_read_role_without_custom_plans(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import okg.substrate.db.connection as connection

    seen: dict[str, Any] = {}
    fake = _FakeConnection(("managed_reader", "auto"))

    def connect(dsn: str, **kwargs: Any) -> _FakeConnection:
        seen["dsn"] = dsn
        return fake

    monkeypatch.setattr(connection, "connect", connect)
    monkeypatch.setenv("OKG_MCP_RO_DSN", f"postgresql://managed_reader:{_FAKE_PASSWORD}@db/okg")
    warning = readiness.mcp_read_plan_cache_warning(
        "postgresql://owner@localhost/okg", surface="okg mcp-serve"
    )
    assert seen["dsn"] == f"postgresql://managed_reader:{_FAKE_PASSWORD}@db/okg"
    assert "current_setting('plan_cache_mode')" in fake.statements[0]
    assert warning is not None
    assert "managed_reader" in warning and "plan_cache_mode = auto" in warning
    assert _FAKE_PASSWORD not in warning

    fake.row = ("managed_reader", "force_custom_plan")
    assert readiness.mcp_read_plan_cache_warning("x", surface="okg mcp-serve") is None


def test_mcp_serve_readiness_reports_a_check_it_could_not_run(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import okg.substrate.db.connection as connection

    def connect(dsn: str, **kwargs: Any) -> Any:
        raise OSError(f"connection refused for password {_FAKE_PASSWORD}")

    monkeypatch.setattr(connection, "connect", connect)
    monkeypatch.setenv("OKG_MCP_RO_DSN", f"postgresql://reader:{_FAKE_PASSWORD}@db/okg")
    warning = readiness.mcp_read_plan_cache_warning("x", surface="okg mcp-serve")
    assert warning is not None and "could not check" in warning
    assert _FAKE_PASSWORD not in warning and "***" in warning


# --------------------------------------------------------------------------- #
# Review fixes (adversarial review of lane-s-redesign-s1)                      #
# --------------------------------------------------------------------------- #


class _ConvergeCursor:
    """A cursor stand-in for ``converge_on_cursor``: a usable autocommit
    connection, no statements of its own."""

    def __init__(self) -> None:
        import psycopg

        self.connection = SimpleNamespace(
            closed=False,
            info=SimpleNamespace(transaction_status=psycopg.pq.TransactionStatus.IDLE),
        )

    def execute(self, *_args: Any) -> None:
        raise AssertionError("no statement expected")


def _fake_convergence(
    monkeypatch: pytest.MonkeyPatch, failures: dict[str, BaseException] | None = None
) -> list[str]:
    """Every spec index missing; records each build attempt by suffix and
    raises the given error for a suffix."""
    attempted: list[str] = []
    failures = failures or {}
    monkeypatch.setattr(cold_history, "_regclass_exists", lambda _cur, _name: False)
    monkeypatch.setattr(cold_history, "_active_hot", lambda _cur: set())
    monkeypatch.setattr(
        cold_history, "_table_columns",
        lambda _cur, _physical: {
            column
            for specs in cold_history.INTERVAL_SOURCE_READ_INDEX_SPECS.values()
            for spec in specs
            for column in spec.columns
        },
    )
    monkeypatch.setattr(cold_history, "_index_catalog_row", lambda _cur, _name: None)
    monkeypatch.setattr(
        cold_history, "pending_interval_index_changes",
        lambda _cur, _tables: {"build": ["still-pending"], "drop": []},
    )

    def build(_cur: Any, *, physical_table: str, spec: Any, building: list[str],
              **_kwargs: Any) -> dict[str, Any]:
        attempted.append(spec.suffix)
        if spec.suffix in failures:
            raise failures[spec.suffix]
        return {"suffix": spec.suffix, "index_name": spec.suffix, "bytes": 1,
                "seconds": 0.0, "rebuilt_invalid": False}

    monkeypatch.setattr(cold_history, "_build_spec_index_concurrently", build)
    return attempted


def test_post_publish_convergence_never_builds_an_extended_spec(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Finding 1: the post-publish rebuild runs each build as one step under
    the publisher lock, so it must not build the GIN index (deployed without
    `okg migrate`, say). Plain specs build as before; the GIN index is
    reported as skipped."""
    attempted = _fake_convergence(monkeypatch)
    report = cold_history.converge_on_cursor(
        _ConvergeCursor(), tables=dict(_PHYSICAL), say=lambda _m: None,  # type: ignore[arg-type]
        build_extended=False,
    )
    assert not {spec.suffix for spec in _S1_SPECS} & set(attempted)
    assert sorted(attempted) == sorted(spec.suffix for _logical, spec in _plain_specs())
    gin = cold_history._full_index_name(_PHYSICAL["okg.graph_nodes"], _GIN.suffix)
    assert report["status"] == "partial", report
    assert report["reason"] == "extended_builds_need_migrate"
    assert gin in report["skipped_extended"]
    assert report["skipped_extended"] == [
        cold_history._full_index_name(_PHYSICAL[logical], spec.suffix)
        for logical, spec in _S1_LOGICAL
    ]

    # okg migrate (the default) does build it.
    attempted.clear()
    migrate_report = cold_history.converge_on_cursor(
        _ConvergeCursor(), tables=dict(_PHYSICAL), say=lambda _m: None,  # type: ignore[arg-type]
    )
    assert _GIN.suffix in attempted and migrate_report["status"] == "converged"


def test_post_publish_rebuild_skips_a_pending_gin_build_without_the_ddl_lock(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Finding 1 at the rebuild's entry: only the GIN build pending (the S1
    code deployed without `okg migrate`) takes no lock, runs no step and is
    reported; a plain build pending still runs the convergence, with
    extended builds turned off."""
    gin = cold_history._full_index_name(_PHYSICAL["okg.graph_nodes"], _GIN.suffix)
    plain = cold_history._full_index_name(_PHYSICAL["okg.graph_nodes"], "node_id_visible")
    pending = {"build": [gin], "drop": []}
    monkeypatch.setattr(cold_history, "registered_graph_tables", lambda _cur: dict(_PHYSICAL))
    monkeypatch.setattr(
        cold_history, "pending_interval_index_changes", lambda _cur, _tables: pending
    )
    calls: list[dict[str, Any]] = []

    def converge(_cur: Any, **kwargs: Any) -> dict[str, Any]:
        calls.append(kwargs)
        return {"status": "partial", "skipped_extended": [gin]}

    monkeypatch.setattr(cold_history, "converge_on_cursor", converge)

    def no_step(_action: Any) -> Any:
        raise AssertionError("no step may run under the publisher lock")

    report = index_reclaim._finish_graph_index_convergence(
        _ConvergeCursor(), step=no_step
    )
    assert report is not None and report["status"] == "partial", report
    assert report["skipped_extended"] == [gin] and calls == []

    class LockCursor(_ConvergeCursor):
        def execute(self, *_args: Any) -> None:
            return None

        def fetchone(self) -> dict[str, Any]:
            return {"got": True}

    pending["build"] = [plain, gin]
    index_reclaim._finish_graph_index_convergence(LockCursor(), step=no_step)
    assert [call["build_extended"] for call in calls] == [False]


def test_a_failed_extended_build_is_reported_and_the_convergence_continues(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Finding 2: disk full (or out of memory, a temp-file limit) while
    building the GIN index does not stop `okg migrate`: the failure is
    reported, the rest converges, and the status says what is pending."""
    import psycopg

    attempted = _fake_convergence(
        monkeypatch, {_GIN.suffix: psycopg.errors.DiskFull("could not extend file")}
    )
    report = cold_history.converge_on_cursor(
        _ConvergeCursor(), tables=dict(_PHYSICAL), say=lambda _m: None,  # type: ignore[arg-type]
    )
    assert report["status"] == "partial", report
    assert report["reason"] == "extended_index_build_failed"
    (failure,) = report["failed"]
    assert failure["suffix"] == _GIN.suffix and failure["sqlstate"] == "53100"
    assert "could not extend file" in failure["error"]
    assert report["pending"] == {"build": ["still-pending"], "drop": []}
    # Every spec after the GIN index was still attempted.
    assert sorted(attempted) == sorted(
        spec.suffix for specs in cold_history.INTERVAL_SOURCE_READ_INDEX_SPECS.values()
        for spec in specs
    )


def test_a_failed_plain_build_and_an_extended_lock_wait_keep_their_old_handling(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import psycopg

    plain_suffix = _plain_specs()[0][1].suffix
    _fake_convergence(monkeypatch, {plain_suffix: psycopg.errors.DiskFull("full")})
    with pytest.raises(psycopg.errors.DiskFull):
        cold_history.converge_on_cursor(
            _ConvergeCursor(), tables=dict(_PHYSICAL), say=lambda _m: None,  # type: ignore[arg-type]
        )
    _fake_convergence(monkeypatch, {_GIN.suffix: psycopg.errors.LockNotAvailable("busy")})
    deferred = cold_history.converge_on_cursor(
        _ConvergeCursor(), tables=dict(_PHYSICAL), say=lambda _m: None,  # type: ignore[arg-type]
    )
    assert deferred["status"] == "deferred" and deferred["reason"] == "lock_timeout"


def test_storage_parameters_are_not_a_wrong_definition() -> None:
    """Finding 3: an operator's `WITH (fastupdate=off)` tunes the GIN index;
    it must not make migrate drop and rebuild it."""
    match = cold_history.spec_index_definition_matches
    tuned = (
        "CREATE INDEX i ON public._t USING gin (attrs jsonb_path_ops) "
        "WITH (fastupdate=off, gin_pending_list_limit='1024') WHERE (deleted = false)"
    )
    assert match(tuned, _GIN) is True
    row = {"indisvalid": True, "indisready": True, "indexdef": tuned}
    assert cold_history._spec_index_current(row, _GIN) is True
    # Still a wrong definition when the keys or predicate differ.
    assert match(
        "CREATE INDEX i ON public._t USING gin (attrs) WITH (fastupdate=off) "
        "WHERE (deleted = false)",
        _GIN,
    ) is False
    assert match(
        "CREATE INDEX i ON public._t USING gin (attrs jsonb_path_ops) WITH (fastupdate=off)",
        _GIN,
    ) is False


_FAKE_PASSWORD = "Hunter2SecretPw"


@pytest.mark.parametrize(
    "dsn",
    [
        f"host=db user=reader password='{_FAKE_PASSWORD}",
        f"host=db password={_FAKE_PASSWORD} user",
        f"postgresql://reader:{_FAKE_PASSWORD}@db/okg?bogus=1",
    ],
)
def test_a_malformed_read_dsn_is_a_finding_that_never_echoes_it(dsn: str) -> None:
    """Finding 4: a malformed OKG_MCP_RO_DSN used to raise out of doctor.
    It is now one finding that names the source and holds no DSN text."""
    roles = management.read_roles_to_check({"OKG_MCP_RO_DSN": dsn})
    assert roles[-1]["unresolved"] == "OKG_MCP_RO_DSN could not be parsed"
    findings = management.plan_cache_role_findings(
        [{**item, "exists": False, "owner": False, "value": None, "scope": None}
         for item in roles if item.get("unresolved")],
        database="okg",
    )
    (finding,) = findings
    assert finding["code"] == "read_role_unresolved" and finding["severity"] == "error"
    assert finding["source"] == "OKG_MCP_RO_DSN"
    assert _FAKE_PASSWORD not in repr(findings) and _FAKE_PASSWORD not in repr(roles)


def test_an_unreadable_os_user_is_a_finding(monkeypatch: pytest.MonkeyPatch) -> None:
    import getpass

    def no_user() -> str:
        raise OSError("no user name")

    monkeypatch.setattr(getpass, "getuser", no_user)
    roles = management.read_roles_to_check({"OKG_MCP_RO_DSN": "host=db dbname=okg"})
    assert "operating-system user name" in roles[-1]["unresolved"]


def test_a_read_dsn_naming_the_owner_is_a_warning() -> None:
    """Finding 5: mcp-serve warns on that connection; doctor must not pass it
    silently. A named role that is the owner stays unreported."""
    findings = management.plan_cache_role_findings(
        [
            {"role": "okg_owner", "source": "OKG_MCP_RO_DSN", "exists": True,
             "owner": True, "value": "auto", "scope": "server"},
            {"role": "okg_mcp", "source": "mcp_read_role", "exists": True,
             "owner": True, "value": "auto", "scope": "server"},
        ],
        database="okg",
    )
    (finding,) = findings
    assert finding["code"] == "read_role_is_owner" and finding["severity"] == "warning"
    assert finding["role"] == "okg_owner"


def test_alter_role_all_sits_between_the_database_and_the_server() -> None:
    """Finding 7: `ALTER ROLE ALL SET` (pg_db_role_setting setrole = 0,
    setdatabase = 0) applies after the database row and before the server
    value."""
    global_auto = {"scope": "global", "setconfig": ["plan_cache_mode=auto"]}
    assert management.effective_role_setting(
        [global_auto], "plan_cache_mode", server="force_custom_plan"
    ) == ("auto", "global")
    assert management.effective_role_setting(
        [global_auto, {"scope": "database", "setconfig": ["plan_cache_mode=force_custom_plan"]}],
        "plan_cache_mode",
    ) == ("force_custom_plan", "database")
    # The doctor session's own value under ALTER ROLE ALL is not the server's.
    assert management.server_setting_value(
        {"setting": "auto", "source": "global"}
    ) is None


def test_statistics_plan_redefines_a_wrong_expression_and_reports_a_held_name() -> None:
    """Finding 8: an owned object of another expression is dropped and
    recreated; a wanted name held by a foreign object is missing, not
    converged."""
    tables = {"node": "_n", "edge": "_e"}
    desired = dai.desired_statistics({"good": "text", "bad": "text", "held": "num"}, tables)
    good, bad, held = (dai.statistics_name("node", k) for k in ("good", "bad", "held"))

    def row(table: str, key: str) -> dict[str, Any]:
        return {"schema": "public", "table_name": table, "table_schema": "public",
                "expressions": [f"attrs -> '{key}'::text"]}

    catalog = {good: row("_n", "good"), bad: row("_n", "other"), held: row("elsewhere", "held")}
    plan = dai._statistics_plan(catalog, desired, tables)
    assert plan["redefine"] == [bad]
    assert plan["create"] == []
    assert plan["blocked"] == [held] and plan["missing"] == [held]
    assert plan["foreign"] == [held]
    assert plan["drop"] == []


@pytest.mark.parametrize(
    ("printed", "key", "ok"),
    [
        ("attrs -> 'k'::text", "k", True),
        ("(attrs -> 'k'::text)", "k", True),
        ("attrs -> 'it''s'::text", "it's", True),
        ("attrs -> 'k'::text", "K", False),
        ("attrs ->> 'k'::text", "k", False),
        ("other -> 'k'::text", "k", False),
    ],
)
def test_statistics_expression_matches_postgres_spelling(
    printed: str, key: str, ok: bool
) -> None:
    assert dai.statistics_expression_matches([printed], key) is ok
    assert dai.statistics_expression_matches([printed, printed], key) is False
    assert dai.statistics_expression_matches(None, key) is False
