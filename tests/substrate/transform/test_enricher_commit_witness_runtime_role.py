"""Issue #2965: enricher writes keep their commit witness under the runtime role.

The fact tables' ``source_program_checksum`` default reads the setting
``okg.source_program_checksum``. The publisher used to set it for one
transaction only, on a connection that commits after every statement, so the
setting was gone before any enricher INSERT ran. Under a superuser the rows were
stored with an empty checksum; under the runtime role ``app_rw`` the witness
trigger refused them with ``commit_witness_missing``.

``app_rw`` itself cannot run the enricher stage: it has no read grant on the
Chronos graph tables (``permission denied for table _chronos_branch_tables``),
and ``okg run --once`` refuses it earlier at the readiness check. The login
that can publish and is still bound by the trigger is one that holds the table
owner's privileges through role membership without being the owner or a
superuser. The stage test runs as such a role; the direct test also runs the
witness as ``app_rw``. Each test first checks the role is neither superuser nor
owner, and requires every row written to carry a checksum that resolves in
``okg.ir_programs``.
"""

from __future__ import annotations

from collections.abc import Iterable
from pathlib import Path
from typing import Any

import psycopg
import psycopg.sql
import pytest
import yaml
from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer
from tests.substrate.conftest import alter_role_under_cluster_lock

pytestmark = pytest.mark.postgres

_DEPLOYMENT = "test-enricher-commit-witness"
_APP_RW_PASSWORD = "okg_rw"
_RUNTIME_ROLE = "okg_witness_runtime"
_RUNTIME_PASSWORD = "okg_witness_runtime"


def _role_dsn(owner_dsn: str, user: str, password: str) -> str:
    info = psycopg.conninfo.conninfo_to_dict(owner_dsn)
    return psycopg.conninfo.make_conninfo(
        host=info.get("host", "localhost"),
        port=info.get("port", "5432"),
        dbname=info["dbname"],
        user=user,
        password=password,
    )


def _app_rw_dsn(owner_dsn: str) -> str:
    return _role_dsn(owner_dsn, "app_rw", _APP_RW_PASSWORD)


def _runtime_dsn(owner_dsn: str) -> str:
    """A publishing login bound by the witness trigger.

    It inherits the fact-table owner's privileges by membership, so it can
    read the Chronos graph and write the publisher's bookkeeping, but it is
    neither that owner nor a superuser. Roles are cluster-wide, so the DDL
    runs under the test suite's cluster role lock.
    """
    with psycopg.connect(owner_dsn, autocommit=True) as owner:
        table_owner = owner.execute(
            "SELECT tableowner FROM pg_tables "
            "WHERE schemaname = 'okg' AND tablename = 'edge_facts'"
        ).fetchone()[0]
    alter_role_under_cluster_lock(
        owner_dsn,
        psycopg.sql.SQL(
            "DO $do$ BEGIN "
            "IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = {name}) "
            "THEN CREATE ROLE {role} LOGIN NOSUPERUSER INHERIT PASSWORD {pw}; "
            "END IF; END $do$"
        ).format(
            name=psycopg.sql.Literal(_RUNTIME_ROLE),
            role=psycopg.sql.Identifier(_RUNTIME_ROLE),
            pw=psycopg.sql.Literal(_RUNTIME_PASSWORD),
        ),
        lock_role=None,
    )
    alter_role_under_cluster_lock(
        owner_dsn,
        psycopg.sql.SQL("GRANT {} TO {}").format(
            psycopg.sql.Identifier(table_owner),
            psycopg.sql.Identifier(_RUNTIME_ROLE),
        ),
        lock_role=None,
    )
    return _role_dsn(owner_dsn, _RUNTIME_ROLE, _RUNTIME_PASSWORD)


def _enable_app_rw_login(owner_dsn: str) -> None:
    """``okg migrate`` creates app_rw as NOLOGIN; a deployment that
    authenticates as it enables LOGIN (docs/reference/security.md)."""
    alter_role_under_cluster_lock(
        owner_dsn,
        psycopg.sql.SQL("ALTER ROLE app_rw LOGIN PASSWORD {}").format(
            psycopg.sql.Literal(_APP_RW_PASSWORD),
        ),
        lock_role="app_rw",
    )


def _assert_runtime_role(conn, expected_user: str) -> None:
    """The refusal only binds a role that is neither superuser nor owner."""
    row = conn.execute(
        """
        SELECT current_user,
               r.rolsuper,
               (SELECT tableowner FROM pg_tables
                 WHERE schemaname = 'okg' AND tablename = 'edge_facts'),
               (SELECT tableowner FROM pg_tables
                 WHERE schemaname = 'okg' AND tablename = 'node_facts')
          FROM pg_roles r
         WHERE r.rolname = current_user
        """
    ).fetchone()
    user, is_super, edge_owner, node_owner = row
    assert user == expected_user
    assert is_super is False
    assert user not in {edge_owner, node_owner}


def _seed_graph(owner_dsn: str, tmp_path: Path) -> None:
    """One live graph that makes each semantic linker and the code-reference
    resolver emit at least one edge (fixture shape from
    test_semantic_symbol_resolution and test_code_reference_call_site_refresh_db)."""
    from okg.substrate.catalog.modules_apply import apply_composed_catalog
    from okg.substrate.catalog.modules_compose import compose_catalog
    from okg.substrate.ingest import SourceRunner
    from okg.substrate.library.sources.base import EdgeFact, NodeFact, SourceRun

    dep = tmp_path / "dep"
    dep.mkdir()
    (dep / "deployment.yaml").write_text(yaml.safe_dump({
        "name": _DEPLOYMENT,
        "description": "enricher commit-witness fixture",
        "modules": [
            "git_graph",
            "openspec",
            "agent_sessions",
            "compute_env",
            "ci",
            "code_graph",
            "quality_evidence",
        ],
    }))
    (dep / "schemas").mkdir()
    with psycopg.connect(owner_dsn) as conn:
        apply_composed_catalog(conn, compose_catalog(dep))

    key = "code-symbol:demo:python:src.app.handle_request:function"
    symbol_id = "symbol:demo:src/app.py:handle_request:90"
    caller_id = "symbol:demo:tests/test_mw.py:test_json_write:88"
    callee_id = "symbol:demo:tinydb/database.py:TinyDB:20"
    line = 42

    def node(node_id: str, subtype: str, **attrs: Any) -> NodeFact:
        return NodeFact(
            node_id=node_id,
            subtype=subtype,
            attrs=attrs,
            source_record_id={"node_id": node_id},
            source_revision={"rev": 1},
        )

    def symbol(node_id: str, path: str, name: str, start: int) -> NodeFact:
        return node(
            node_id, "code_symbol", repo="demo", path=path, kind="function",
            name=name, qualified_name=name, start_line=start,
            end_line=start + 20,
        )

    class FixtureSource:
        name = "fixture"
        profile = "discovery_crawl"

        def run(self, run_id: str, *, mode: str, cursor=None) -> SourceRun:
            del run_id, mode, cursor

            def facts() -> Iterable[NodeFact | EdgeFact]:
                yield node(
                    "file:demo:src/app.py", "source_file",
                    repo="demo", path="src/app.py",
                )
                yield node(
                    symbol_id, "code_symbol", repo="demo", path="src/app.py",
                    language="python", kind="function",
                    symbol_kind="function", name="handle_request",
                    qualified_name="handle_request",
                    canonical_name="src.app.handle_request",
                    semantic_key=key,
                    semantic_key_version="okg.code_symbol.semantic_key.v1",
                    module_name="src.app", start_line=90, end_line=110,
                )
                yield node(
                    "tool-use:fixture:moved", "tool_use_event",
                    tool_name="exec_command", file_path="src/app.py",
                    line=line, symbol_semantic_key=key,
                )
                yield node(
                    "stack-frame:fixture:moved:0", "stack_frame",
                    path="src/app.py", line=line, function="handle_request",
                    symbol_semantic_key=key,
                )
                yield node(
                    "runtime-event:fixture:moved", "runtime_event",
                    kind="test_failure", status="failed",
                )
                yield node(
                    "test-case:fixture:test_moved", "test_case",
                    classname="tests.test_app", name="test_moved",
                )
                yield EdgeFact(
                    src="runtime-event:fixture:moved",
                    dst="test-case:fixture:test_moved",
                    edge_type="references",
                    source_record_id={"edge": "event-case"},
                    source_revision={"rev": 1},
                )
                yield EdgeFact(
                    src="runtime-event:fixture:moved",
                    dst="stack-frame:fixture:moved:0",
                    edge_type="contains",
                    source_record_id={"edge": "event-frame"},
                    source_revision={"rev": 1},
                )
                yield node(
                    "asset:demo:sql_relation:orders", "data_asset",
                    asset_id="asset:demo:sql_relation:orders",
                    kind="sql_relation", name="orders", path="src/app.py",
                    line=line, access_kind="read", source_semantic_key=key,
                )
                yield node(
                    "flow:semgrep:moved", "data_flow_observation",
                    tool="semgrep", rule_id="demo.moved", path="src/app.py",
                    start_line=line, asset_hint="orders",
                    symbol_semantic_key=key,
                )
                yield node(
                    "security:finding:moved", "security_finding",
                    tool="semgrep", rule_id="demo.security",
                    path="src/app.py", start_line=line,
                    symbol_semantic_key=key,
                )
                yield node(
                    "profile-hotspot:moved", "profile_hotspot",
                    profile_run_id="profile-run:moved",
                    name="handle_request", path="src/app.py", line=line,
                    symbol_semantic_key=key,
                )
                yield node(
                    "endpoint:demo:http:moved", "interface_endpoint",
                    kind="http_route", name="GET /moved", path="src/app.py",
                    start_line=line, handler_semantic_key=key,
                )
                yield node(
                    "coverage-report:demo:moved", "coverage_report",
                    covered_symbols={key: 3},
                )
                yield symbol(caller_id, "tests/test_mw.py", "test_json_write", 88)
                yield symbol(callee_id, "tinydb/database.py", "TinyDB", 20)
                yield node(
                    "reference:demo:tests/test_mw.py:TinyDB:106",
                    "code_reference", repo="demo", path="tests/test_mw.py",
                    language="python", name="TinyDB",
                    qualified_name="TinyDB", reference_kind="call",
                    enclosing_symbol_id=caller_id, start_line=106,
                    end_line=106,
                )

            return SourceRun(facts=facts(), completed_scope=True)

    SourceRunner(owner_dsn, source_name="fixture.commit-witness").run(
        FixtureSource(),
        mode="scope_complete",
    )


def _enrichers() -> list[Any]:
    from okg.substrate.library.linkers.code_reference import (
        CodeReferenceResolver,
    )
    from okg.substrate.library.linkers.coverage_symbol import CoverageSymbolLinker
    from okg.substrate.library.linkers.data_asset import DataAssetLinker
    from okg.substrate.library.linkers.data_flow_evidence import (
        DataFlowEvidenceLinker,
    )
    from okg.substrate.library.linkers.interface_handler import (
        InterfaceHandlerLinker,
    )
    from okg.substrate.library.linkers.profile_hotspot import ProfileHotspotLinker
    from okg.substrate.library.linkers.security_finding import (
        SecurityFindingLinker,
    )
    from okg.substrate.library.linkers.test_symbol import TestSymbolLinker
    from okg.substrate.library.linkers.tool_use_symbol import ToolUseSymbolLinker

    # The workspace-graph profile's shipped enricher set, minus the
    # declarative and acceptance-criteria linkers, which this fixture gives no
    # input (they share the same stage and write through the same helpers).
    return [
        ToolUseSymbolLinker(),
        CodeReferenceResolver(),
        TestSymbolLinker(),
        CoverageSymbolLinker(),
        InterfaceHandlerLinker(),
        DataAssetLinker(),
        DataFlowEvidenceLinker(),
        SecurityFindingLinker(),
        ProfileHotspotLinker(),
    ]


def _enricher_rows(owner_dsn: str) -> list[tuple[str, str, bool]]:
    with psycopg.connect(owner_dsn) as conn:
        rows = conn.execute(
            """
            SELECT f.source,
                   coalesce(f.source_program_checksum, ''),
                   p.program_id IS NOT NULL
              FROM (
                    SELECT source, source_program_checksum FROM okg.edge_facts
                    UNION ALL
                    SELECT source, source_program_checksum FROM okg.node_facts
                   ) f
              LEFT JOIN okg.ir_programs p
                ON p.program_checksum = f.source_program_checksum
             WHERE f.source LIKE '\\_enricher:%'
            """
        ).fetchall()
    return [(str(r[0]), str(r[1]), bool(r[2])) for r in rows]


def test_enricher_stage_as_runtime_role_writes_resolvable_witnesses(
    clean_runtime: str,
    tmp_path: Path,
) -> None:
    from okg.substrate.publisher import Publisher
    from okg.substrate.transform.programs import enrichment_program

    _seed_graph(clean_runtime, tmp_path)
    runtime_dsn = _runtime_dsn(clean_runtime)
    enrichers = _enrichers()
    publisher = Publisher(
        clean_runtime,
        _DEPLOYMENT,
        enrichers=enrichers,
        catalog_identity=bind_test_catalog_writer(clean_runtime, _DEPLOYMENT),
    )
    with psycopg.connect(clean_runtime) as owner:
        catalog_version_id = publisher._current_catalog_version_id(owner)

    # The publish connection: autocommit, as Publisher.cycle opens it.
    with psycopg.connect(runtime_dsn, autocommit=True) as rw:
        _assert_runtime_role(rw, _RUNTIME_ROLE)
        runs = publisher._run_enrichers(
            rw,
            generation_id=0,
            catalog_version_id=int(catalog_version_id),
        )
        # The stage clears its pin: a later write on the same connection that
        # names no program is refused, not stamped as enrichment.
        after = rw.execute(
            "SELECT current_setting('okg.source_program_checksum', true)"
        ).fetchone()[0]
        assert after in (None, "")

    statuses = {
        str(run.get("name")): (run.get("status"), run.get("reason"))
        for run in runs
        if not str(run.get("name", "")).startswith("_")
    }
    expected = {str(e.name) for e in enrichers}
    assert set(statuses) == expected, statuses
    failed = {
        name: status for name, status in statuses.items()
        if status[0] != "ok"
    }
    assert not failed, failed

    rows = _enricher_rows(clean_runtime)
    writers = {source for source, _checksum, _resolves in rows}
    # Every enricher in the set wrote at least one fact, so none of them is
    # vacuously "witnessed".
    assert writers == {f"_enricher:{name}" for name in expected}, writers
    checksum = enrichment_program().checksum()
    bad = [r for r in rows if r[1] != checksum or not r[2]]
    assert not bad, bad


def test_witness_survives_autocommit_and_chronos_reads(
    clean_runtime: str,
) -> None:
    """The pin survives the two things that cleared the per-transaction
    value: a statement-level commit and a Chronos checkout read. After the
    session ends, an unnamed write is refused again (failure case)."""
    from okg.substrate.library.linkers import _chronos
    from okg.substrate.transform.commit import (
        commit_witness_session,
        persist_transform_program,
    )
    from okg.substrate.transform.programs import enrichment_program

    program = enrichment_program()
    _enable_app_rw_login(clean_runtime)
    runtime_dsn = _runtime_dsn(clean_runtime)
    with psycopg.connect(clean_runtime, autocommit=True) as owner:
        persist_transform_program(owner, program)

    insert = """
        INSERT INTO okg.edge_facts
          (edge_id, src, dst, edge_type, attrs, provenance, confidence,
           source, source_record_id, source_revision, observed_at,
           dedupe_key, run_id, op)
        VALUES (%s, 'n:a', 'n:b', 'references', '{}'::jsonb,
                'derived_deterministic', 1.0, '_enricher:witness-probe',
                '{}'::jsonb, '{}'::jsonb, now(), %s, gen_random_uuid(), 'I')
    """
    with psycopg.connect(_app_rw_dsn(clean_runtime), autocommit=True) as rw:
        _assert_runtime_role(rw, "app_rw")

        # Dev's mechanism: a transaction-scoped pin on an autocommit
        # connection is gone by the next statement.
        rw.execute(
            "SELECT set_config('okg.source_program_checksum', %s, true)",
            (program.checksum(),),
        )
        with pytest.raises(psycopg.errors.RaiseException) as refused:
            rw.execute(insert, (9_650_001, "witness-probe-local"))
        assert "commit_witness_missing" in str(refused.value)

        with commit_witness_session(rw, program.checksum()):
            rw.execute(insert, (9_650_002, "witness-probe-app-rw-1"))
            rw.execute(insert, (9_650_003, "witness-probe-app-rw-2"))

        with pytest.raises(psycopg.errors.RaiseException) as released:
            rw.execute(insert, (9_650_004, "witness-probe-released"))
        assert "commit_witness_missing" in str(released.value)

    # A Chronos checkout read between the pin and the INSERT (app_rw cannot
    # read the Chronos tables, so this half runs as the runtime role).
    with psycopg.connect(runtime_dsn, autocommit=True) as rt:
        _assert_runtime_role(rt, _RUNTIME_ROLE)
        with commit_witness_session(rt, program.checksum()):
            with rt.cursor() as cur:
                _chronos.query_strict(
                    cur, "SELECT node_id FROM okg.graph_nodes",
                )
            rt.execute(insert, (9_650_005, "witness-probe-after-read"))
    with psycopg.connect(runtime_dsn) as rt_tx:
        with commit_witness_session(rt_tx, program.checksum()):
            with rt_tx.cursor() as cur:
                _chronos.query_strict(
                    cur, "SELECT node_id FROM okg.graph_nodes",
                )
            rt_tx.execute(insert, (9_650_006, "witness-probe-in-tx"))
        rt_tx.commit()

    with psycopg.connect(clean_runtime) as owner:
        rows = owner.execute(
            """
            SELECT e.dedupe_key, e.source_program_checksum,
                   p.program_id IS NOT NULL
              FROM okg.edge_facts e
              LEFT JOIN okg.ir_programs p
                ON p.program_checksum = e.source_program_checksum
             WHERE e.source = '_enricher:witness-probe'
             ORDER BY e.dedupe_key
            """
        ).fetchall()
    assert [(r[0], r[1], r[2]) for r in rows] == [
        ("witness-probe-after-read", program.checksum(), True),
        ("witness-probe-app-rw-1", program.checksum(), True),
        ("witness-probe-app-rw-2", program.checksum(), True),
        ("witness-probe-in-tx", program.checksum(), True),
    ]



# ---------------------------------------------------------------------------
# A stamp must never outlive its writer (review finding F1 on #2967).
# Each case ends with a write that names no program, from an unrelated
# producer, and requires the trigger to REFUSE it under the restricted role.
# ---------------------------------------------------------------------------

_PROBE_INSERT = """
    INSERT INTO okg.edge_facts
      (edge_id, src, dst, edge_type, attrs, provenance, confidence,
       source, source_record_id, source_revision, observed_at,
       dedupe_key, run_id, op)
    VALUES (%s, 'n:a', 'n:b', 'references', '{}'::jsonb,
            'derived_deterministic', 1.0, %s,
            '{}'::jsonb, '{}'::jsonb, now(), %s, gen_random_uuid(), 'I')
"""


def _persist_enrichment(owner_dsn: str) -> str:
    from okg.substrate.transform.commit import persist_transform_program
    from okg.substrate.transform.programs import enrichment_program

    program = enrichment_program()
    with psycopg.connect(owner_dsn, autocommit=True) as owner:
        persist_transform_program(owner, program)
    return program.checksum()


def _assert_unnamed_write_refused(conn, edge_id: int, key: str) -> None:
    with pytest.raises(psycopg.errors.RaiseException) as refused:
        conn.execute(_PROBE_INSERT, (edge_id, "_lens:unrelated", key))
    assert "commit_witness_missing" in str(refused.value)
    if not conn.autocommit:
        conn.rollback()


def test_stamp_does_not_return_when_a_later_transaction_rolls_back(
    clean_runtime: str,
) -> None:
    """Outside autocommit: commit inside the session, then roll back the
    transaction the session's clear ran in. The stamp must not come back."""
    from okg.substrate.transform.commit import commit_witness_session

    checksum = _persist_enrichment(clean_runtime)
    _enable_app_rw_login(clean_runtime)
    with psycopg.connect(_app_rw_dsn(clean_runtime)) as rw:
        _assert_runtime_role(rw, "app_rw")
        with commit_witness_session(rw, checksum):
            rw.execute(
                _PROBE_INSERT,
                (9_651_001, "_enricher:witness-probe", "f1-rollback-named"),
            )
            rw.commit()
        rw.rollback()
        _assert_unnamed_write_refused(rw, 9_651_002, "f1-rollback-unnamed")


def test_stamp_does_not_survive_a_failed_transaction(clean_runtime: str) -> None:
    """Outside autocommit: the writer's transaction fails, so the clear
    cannot run. After the rollback the stamp must be gone."""
    from okg.substrate.transform.commit import commit_witness_session

    checksum = _persist_enrichment(clean_runtime)
    _enable_app_rw_login(clean_runtime)
    with psycopg.connect(_app_rw_dsn(clean_runtime)) as rw:
        _assert_runtime_role(rw, "app_rw")
        with pytest.raises(psycopg.errors.DivisionByZero):
            with commit_witness_session(rw, checksum):
                rw.execute(
                    _PROBE_INSERT,
                    (9_651_011, "_enricher:witness-probe", "f1-failed-named"),
                )
                rw.commit()
                rw.execute("SELECT 1 / 0")
        rw.rollback()
        _assert_unnamed_write_refused(rw, 9_651_012, "f1-failed-unnamed")


def test_pool_return_clears_a_stamp_left_behind(clean_runtime: str) -> None:
    """A borrower that pins and never releases must not hand the stamp to
    the next borrower of the same backend."""
    from okg.substrate.db.pool import get_pool
    from okg.substrate.transform.commit import pin_commit_witness

    checksum = _persist_enrichment(clean_runtime)
    _enable_app_rw_login(clean_runtime)
    pool = get_pool(_app_rw_dsn(clean_runtime), max_size=2)
    with pool.connection() as first:
        _assert_runtime_role(first, "app_rw")
        stamped_pid = first.info.backend_pid
        pin_commit_witness(first, checksum)
    # Hold both pooled connections at once, so one is the stamped backend.
    with pool.connection() as a, pool.connection() as b:
        pids = {a.info.backend_pid, b.info.backend_pid}
        assert stamped_pid in pids
        for n, conn in enumerate((a, b)):
            assert conn.execute(
                "SELECT current_setting('okg.source_program_checksum', true)"
            ).fetchone()[0] in (None, "")
            _assert_unnamed_write_refused(
                conn, 9_651_021 + n, f"f1-pool-unnamed-{n}",
            )


# ---------------------------------------------------------------------------
# The per-enricher re-pin (F3) and the extractor stamp (F2).
# ---------------------------------------------------------------------------


class _WitnessEnricherBase:
    publisher_skip_when_clean = False
    reads_subtypes = ("code_reference", "code_symbol")
    reads_edge_types = ()
    emits_edge_types = ("references",)
    requires_narrowings = (("code_reference", "references", "code_symbol"),)


class _CommitsThenFails(_WitnessEnricherBase):
    """Ends the transaction the way a committing checkout read does, then
    fails; the publisher then rolls the connection back."""

    name = "witness_committer"

    def enrich(self, conn, *, generation_id: int, incremental=None):
        conn.commit()
        raise RuntimeError("failed after its checkout committed")


class _WritesOneEdge(_WitnessEnricherBase):
    name = "witness_writer"

    def enrich(self, conn, *, generation_id: int, incremental=None):
        from okg.substrate.enrichers.base import EnrichResult

        conn.execute(
            _PROBE_INSERT,
            (9_651_031, "_enricher:witness_writer", "f3-next-enricher"),
        )
        return EnrichResult(self.name, n_edges_emitted=1)


def test_next_enricher_is_stamped_after_one_fails_outside_autocommit(
    clean_runtime: str,
    tmp_path: Path,
) -> None:
    from okg.substrate.publisher import Publisher
    from okg.substrate.transform.programs import enrichment_program

    _seed_graph(clean_runtime, tmp_path)
    runtime_dsn = _runtime_dsn(clean_runtime)
    publisher = Publisher(
        clean_runtime,
        _DEPLOYMENT,
        enrichers=[_CommitsThenFails(), _WritesOneEdge()],
        catalog_identity=bind_test_catalog_writer(clean_runtime, _DEPLOYMENT),
    )
    with psycopg.connect(clean_runtime) as owner:
        catalog_version_id = publisher._current_catalog_version_id(owner)

    with psycopg.connect(runtime_dsn) as rt:
        assert rt.autocommit is False
        _assert_runtime_role(rt, _RUNTIME_ROLE)
        runs = publisher._run_enrichers(
            rt,
            generation_id=0,
            catalog_version_id=int(catalog_version_id),
        )
        rt.commit()

    statuses = {str(r.get("name")): r.get("status") for r in runs}
    assert statuses["witness_committer"] == "failed", runs
    assert statuses["witness_writer"] == "ok", runs
    with psycopg.connect(clean_runtime) as owner:
        rows = owner.execute(
            "SELECT e.source_program_checksum, p.program_id IS NOT NULL "
            "FROM okg.edge_facts e LEFT JOIN okg.ir_programs p "
            "ON p.program_checksum = e.source_program_checksum "
            "WHERE e.source = '_enricher:witness_writer'"
        ).fetchall()
    assert rows == [(enrichment_program().checksum(), True)]


def test_extractor_edges_are_stamped_under_the_runtime_role(
    clean_runtime: str,
) -> None:
    """A full Publisher.cycle() as the restricted runtime login writes
    `_extractor:*` mention edges, each carrying the enrichment witness."""
    from okg.substrate.catalog.aliases import add_alias
    from okg.substrate.transform.programs import enrichment_program
    from tests.substrate.test_publisher_chronos_alias_recovery import (
        _publisher,
        _seed_catalog_and_graph,
    )

    with psycopg.connect(clean_runtime, autocommit=True) as owner:
        _seed_catalog_and_graph(owner)
        add_alias(owner, alias="dm", canonical="concept:diabetes", scope="concept")
    runtime_dsn = _runtime_dsn(clean_runtime)
    with psycopg.connect(runtime_dsn) as rt:
        _assert_runtime_role(rt, _RUNTIME_ROLE)

    # Constructed and run entirely as the runtime login.
    out = _publisher(runtime_dsn).cycle()
    assert out["status"] == "published", out
    assert out["extractor_edges_emitted"] >= 1, out

    with psycopg.connect(clean_runtime) as owner:
        rows = owner.execute(
            "SELECT e.source, e.source_program_checksum, "
            "       p.program_id IS NOT NULL "
            "FROM okg.edge_facts e LEFT JOIN okg.ir_programs p "
            "ON p.program_checksum = e.source_program_checksum "
            "WHERE e.source LIKE '\\_extractor:%'"
        ).fetchall()
    assert rows, "the cycle wrote no _extractor:* facts"
    assert {(r[1], r[2]) for r in rows} == {
        (enrichment_program().checksum(), True),
    }, rows



# ---------------------------------------------------------------------------
# Review round 3: the combined pool reset, and the two session-stamp exits
# that skipped or deferred the clear.
# ---------------------------------------------------------------------------


def _other_session_can_take(owner_dsn: str, key: int) -> bool:
    with psycopg.connect(owner_dsn, autocommit=True) as other:
        free = other.execute(
            "SELECT pg_try_advisory_lock(%s)", (key,),
        ).fetchone()[0]
        if free:
            other.execute("SELECT pg_advisory_unlock(%s)", (key,))
    return bool(free)


def test_non_autocommit_pool_return_drops_lock_and_stamp(
    clean_runtime: str,
) -> None:
    """The shared pool's one reset releases a borrower's session advisory
    lock AND clears its committed stamp, on a pool outside autocommit, and
    leaves the connection reusable (same backend on re-borrow)."""
    from psycopg_pool import ConnectionPool

    from okg.substrate.db.connection import connection_kwargs
    from okg.substrate.db.pool import reset_returned_connection

    checksum = _persist_enrichment(clean_runtime)
    _enable_app_rw_login(clean_runtime)
    dsn = _app_rw_dsn(clean_runtime)
    key = 29_652_968
    pool = ConnectionPool(
        conninfo=dsn,
        min_size=1,
        max_size=1,
        kwargs={**connection_kwargs(dsn=dsn), "autocommit": False},
        reset=reset_returned_connection,
        open=True,
        name="okg-test-witness-reset",
    )
    try:
        with pool.connection() as conn:
            _assert_runtime_role(conn, "app_rw")
            pid = conn.info.backend_pid
            conn.execute("SELECT pg_advisory_lock(%s)", (key,))
            conn.execute(
                "SELECT set_config('okg.source_program_checksum', %s, false)",
                (checksum,),
            )
            conn.commit()
        assert _other_session_can_take(clean_runtime, key)
        with pool.connection() as conn:
            assert conn.info.backend_pid == pid
            assert conn.execute(
                "SELECT current_setting('okg.source_program_checksum', true)"
            ).fetchone()[0] in (None, "")
            _assert_unnamed_write_refused(conn, 9_651_041, "r3-pool-unnamed")
        assert pool.get_stats().get("returns_bad", 0) == 0
    finally:
        pool.close()


def test_autocommit_change_mid_session_clears_stamp_before_raising(
    clean_runtime: str,
) -> None:
    from okg.substrate.transform.commit import commit_witness_session

    checksum = _persist_enrichment(clean_runtime)
    _enable_app_rw_login(clean_runtime)
    with psycopg.connect(_app_rw_dsn(clean_runtime), autocommit=True) as rw:
        _assert_runtime_role(rw, "app_rw")
        with pytest.raises(RuntimeError, match="left autocommit"):
            with commit_witness_session(rw, checksum):
                rw.autocommit = False
        assert rw.autocommit is False
        _assert_unnamed_write_refused(rw, 9_651_051, "r3-mode-unnamed")


def test_failed_explicit_transaction_on_autocommit_clears_stamp(
    clean_runtime: str,
) -> None:
    """An explicit BEGIN inside the session on an autocommit connection
    fails; the committed session stamp must still be cleared."""
    from okg.substrate.transform.commit import commit_witness_session

    checksum = _persist_enrichment(clean_runtime)
    _enable_app_rw_login(clean_runtime)
    with psycopg.connect(_app_rw_dsn(clean_runtime), autocommit=True) as rw:
        _assert_runtime_role(rw, "app_rw")
        with pytest.raises(psycopg.errors.DivisionByZero):
            with commit_witness_session(rw, checksum):
                rw.execute("BEGIN")
                rw.execute("SELECT 1 / 0")
        rw.execute("ROLLBACK")
        _assert_unnamed_write_refused(rw, 9_651_061, "r3-begin-unnamed")
