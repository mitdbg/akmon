"""Issue 2941 fix 1: the effective-attribute-view content hash on parallel workers.

The merge statement hashes every merged row with okg.attribute_content_hash.
The raw rows it reads used to sit in a TEMP table, which parallel workers
cannot scan, so the hash ran on one core. A large stage now uses an UNLOGGED
table in the okg schema instead. These tests pin three things:

* parity: the unlogged, parallel path writes exactly the hashes, version rows
  and snapshot rows that the old temp, serial path writes, on attrs with
  nested objects and arrays, unicode, scale-different numbers and the
  ``nomos_source_lineage`` envelope, with and without an overlay;
* the planner actually puts the hash statement under a Gather;
* the staging table never outlives its transaction, on success or on error,
  and is refused outside a transaction.
"""

from __future__ import annotations

import re
import uuid

import pytest

psycopg = pytest.importorskip("psycopg")
import okg.substrate.attributes as attributes_module  # noqa: E402

from okg.substrate.errors import SubstrateError  # noqa: E402
from okg.substrate.graph_store import ChronosGraphStore  # noqa: E402
from okg.substrate.library.sources.base import EdgeFact, NodeFact  # noqa: E402
from tests.pytest.catalog_fixture_builders import seed_catalog  # noqa: E402

pytestmark = pytest.mark.postgres

_INSTR = attributes_module._MATERIALIZE_INSTRUMENTATION


def _payload(i: int) -> dict:
    return {
        "title": f"doc {i} \u00e9t\u00e9 \u2014 \u6f22\u5b57 \U0001f600",
        "combining": "e\u0301",
        "quote": 'say "hi"\\n\ttab',
        "n_int": i,
        "n_scale_a": 1.0,
        "n_scale_b": 1.25,
        "n_big": 12345678901234567890,
        "flag": i % 2 == 0,
        "none": None,
        "nested": {"z": [3, {"b": 2, "a": 1}, []], "a": {"deep": {"x": "y"}}},
        "empty_obj": {},
        "nomos_source_lineage": {
            "source_ids": [f"src-{i % 3}.git-files"],
            "sources": [
                {
                    "source_id": f"src-{i % 3}.git-files",
                    "source_revisions": [{"head_sha": f"{i:040x}"}],
                    "source_records": [{"id": f"rec:{i}", "run_id": "r1"}],
                }
            ],
        },
    }


def _seed(dsn: str) -> dict:
    suffix = uuid.uuid4().hex[:8]
    nodes = [f"doc:{i}:{suffix}" for i in range(24)]
    with psycopg.connect(dsn, autocommit=True) as conn:
        seed_catalog(
            conn,
            subtypes=["doc"],
            narrowings=[("doc", "cites", "doc")],
        )
        facts: list = [
            NodeFact(node, "doc", _payload(i), {"id": node}, {"v": 1})
            for i, node in enumerate(nodes)
        ]
        facts += [
            EdgeFact(
                nodes[i],
                nodes[(i + 1) % len(nodes)],
                "cites",
                attrs={"weight": i / 4, "label": f"\u00fcber {i}", **_payload(i)},
                source_record_id={"id": f"edge:{i}:{suffix}"},
                source_revision={"v": 1},
            )
            for i in range(len(nodes))
        ]
        ChronosGraphStore.from_connection(conn).apply_facts(
            facts, source="default", strict_endpoints=True
        )
    return {"nodes": nodes, "suffix": suffix}


def _overlay(dsn: str, generation: str, nodes: list[str]) -> None:
    """Accepted overlays on a few nodes, so raw_attrs is non-NULL there and
    both canonical-text calls of the hash run."""
    with psycopg.connect(dsn, autocommit=True) as conn:
        for node in nodes[:3]:
            attributes_module.record_attribute_observation(
                conn,
                target_kind="node",
                target_id=node,
                generation_id=generation,
                attr_key="enriched",
                value={"why": "overlay \u2713", "score": 0.50},
                producer="test",
            )
        attributes_module.materialize_effective_attributes_for_generation(
            conn, generation_id=generation
        )


def _capture(conn, generation: str) -> dict:
    with conn.cursor() as cur:
        cur.execute(
            f"SELECT v.target_kind, v.target_id, v.attrs_hash, v.overlay_count, "
            f"v.raw_text_hash, v.subtype, v.src, v.dst, v.edge_type, "
            f"av.attrs, av.raw_attrs "
            f"FROM {attributes_module.ATTRIBUTE_EFFECTIVE_VIEW_TABLE} v "
            "JOIN okg.attribute_versions av "
            "  ON av.target_kind = v.target_kind AND av.target_id = v.target_id "
            " AND av.attrs_hash = v.attrs_hash "
            "WHERE v.generation_id = %s AND v.namespace = 'default'",
            (generation,),
        )
        return {(r[0], r[1]): tuple(r[2:]) for r in cur.fetchall()}


def _materialize(conn, generation: str) -> dict:
    return attributes_module.materialize_effective_attribute_view_for_generation(
        conn,
        generation_id=generation,
        previous_generation_id=None,
        empty_overlay_fast_path=False,
    )


def _stage_tables(conn) -> list[str]:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT c.relname FROM pg_class c "
            "JOIN pg_namespace n ON n.oid = c.relnamespace "
            "WHERE n.nspname = 'okg' AND c.relname LIKE '\\_eff\\_stage\\_%'"
        )
        return [r[0] for r in cur.fetchall()]


def _run_path(
    dsn: str, generation: str, monkeypatch: pytest.MonkeyPatch, *, parallel: bool
) -> tuple[dict, dict]:
    """Materialize one full generation inside an outer transaction (the publish
    shape: the materialize's own transaction is a savepoint), capture the rows,
    then roll everything back so the next path starts from the same state."""
    if parallel:
        monkeypatch.setattr(attributes_module, "_PARALLEL_HASH_MIN_ROWS", 0)
    else:
        monkeypatch.setattr(
            attributes_module, "_raw_stage_is_unlogged", lambda **_: False
        )
    attributes_module._reset_materialize_instrumentation()
    with psycopg.connect(dsn) as conn:
        result = _materialize(conn, generation)
        rows = _capture(conn, generation)
        instr = dict(_INSTR)
        assert _stage_tables(conn) == []
        conn.rollback()
    monkeypatch.undo()
    assert result["materialized"] is True, result
    instr["parallel_hash"] = result["parallel_hash"]
    return rows, instr


def test_parallel_stage_writes_identical_hashes_and_rows(
    clean_runtime: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    seeded = _seed(clean_runtime)
    generation = f"gen-parity-{seeded['suffix']}"
    _overlay(clean_runtime, generation, seeded["nodes"])

    serial, serial_instr = _run_path(
        clean_runtime, generation, monkeypatch, parallel=False
    )
    parallel, parallel_instr = _run_path(
        clean_runtime, generation, monkeypatch, parallel=True
    )

    # The paths really differed: the old one staged in TEMP, serially.
    assert serial_instr["unlogged_stages"] == 0, serial_instr
    assert serial_instr["parallel_hash_stages"] == 0, serial_instr
    assert parallel_instr["unlogged_stages"] == 2, parallel_instr
    assert parallel_instr["parallel_hash_stages"] == 2, parallel_instr
    for kind in ("node", "edge"):
        assert parallel_instr["parallel_hash"][kind].startswith("parallel: "), parallel_instr

    n = len(seeded["nodes"])
    kinds = sorted(k for k, _ in serial)
    assert kinds.count("node") == n and kinds.count("edge") == n, kinds
    overlaid = [key for key, row in serial.items() if row[-1] is not None]
    assert len(overlaid) == 3, overlaid
    lineage = [key for key, row in serial.items() if "nomos_source_lineage" in row[-2]]
    assert len(lineage) == 2 * n, len(lineage)

    assert set(parallel) == set(serial), set(parallel) ^ set(serial)
    for key, row in serial.items():
        assert parallel[key] == row, (key, parallel[key], row)


def test_hash_statement_is_planned_under_gather(
    clean_runtime: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    seeded = _seed(clean_runtime)
    generation = f"gen-plan-{seeded['suffix']}"
    monkeypatch.setattr(attributes_module, "_PARALLEL_HASH_MIN_ROWS", 0)
    plans: list[str] = []
    settings: list[tuple] = []
    requested: list[int] = []
    original = attributes_module._execute_hash_stage

    class _ExplainFirst:
        def __init__(self, inner, conn) -> None:
            self._inner = inner
            self._conn = conn

        def execute(self, query, params):
            if "_eff_merged_" in query and query.startswith("CREATE TEMP TABLE"):
                # EXPLAIN ANALYZE the SELECT alone: analyzing the CREATE TABLE AS
                # would create the table the real statement then creates.
                select = query.split(" ON COMMIT DROP AS ", 1)[1]
                cur = self._inner.execute(
                    "EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF) " + select,
                    params,
                )
                lines = [r[0] if not isinstance(r, dict) else next(iter(r.values()))
                         for r in cur.fetchall()]
                plans.append("\n".join(lines))
                with self._conn.cursor() as c:
                    c.execute(
                        "SELECT current_setting('parallel_setup_cost'), "
                        "current_setting('min_parallel_table_scan_size')"
                    )
                    settings.append(tuple(c.fetchone()))
            return self._inner.execute(query, params)

    def _wrapped(conn, db, stage_sql, params, *, rows, workers):
        requested.append(workers)
        return original(
            conn, _ExplainFirst(db, conn), stage_sql, params, rows=rows, workers=workers
        )

    monkeypatch.setattr(attributes_module, "_execute_hash_stage", _wrapped)
    with psycopg.connect(clean_runtime) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT current_setting('parallel_setup_cost')")
            before = cur.fetchone()[0]
        _materialize(conn, generation)
        with conn.cursor() as cur:
            # The planner settings are put back right after the statement.
            cur.execute("SELECT current_setting('parallel_setup_cost')")
            assert cur.fetchone()[0] == before
        conn.rollback()

    assert len(plans) == 2, plans
    assert settings == [("0", "0")] * 2, settings
    launched: list[int] = []
    for plan, cap in zip(plans, requested):
        assert "Gather" in plan, plan
        planned = int(re.search(r"Workers Planned: (\d+)", plan).group(1))
        assert 0 < planned <= cap, (planned, cap, plan)
        launched.append(int(re.search(r"Workers Launched: (\d+)", plan).group(1)))
    if not all(launched):
        pytest.skip(
            f"the server launched no parallel worker (launched {launched}); "
            "every worker slot was busy, so this run cannot prove the parallel path ran"
        )


@pytest.mark.parametrize(
    ("pool", "expected"), [(0, None), (2, 2), (4, 2), (6, 3), (8, 4), (32, 8)]
)
def test_worker_request_leaves_half_the_pool(
    clean_runtime: str, pool: int, expected: int | None
) -> None:
    with psycopg.connect(clean_runtime) as conn:
        with conn.cursor() as cur:
            cur.execute(f"SET LOCAL max_parallel_workers = {int(pool)}")
        workers, reason = attributes_module._parallel_hash_workers(conn)
        conn.rollback()
    if expected is None:
        assert (workers, reason) == (0, "max_parallel_workers is 0 (parallel query disabled)")
    else:
        assert (workers, reason) == (expected, None)


def _serial_fallback_run(
    dsn: str, generation: str, monkeypatch: pytest.MonkeyPatch, *, setup_sql: str
) -> tuple[dict, dict, dict, list[str]]:
    monkeypatch.setattr(attributes_module, "_PARALLEL_HASH_MIN_ROWS", 0)
    attributes_module._reset_materialize_instrumentation()
    with psycopg.connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(setup_sql)
        result = _materialize(conn, generation)
        with conn.cursor() as cur:
            cur.execute("RESET ROLE")
        rows = _capture(conn, generation)
        leftovers = _stage_tables(conn)
        conn.rollback()
    monkeypatch.undo()
    return result, rows, dict(_INSTR), leftovers


def _grant_table_rights(dsn: str, role: str) -> None:
    """Grant after seeding: the chronos tables are created by the first write."""
    with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        for schema in ("okg", "okg_derived", "public"):
            cur.execute(f"GRANT USAGE ON SCHEMA {schema} TO {role}")
            cur.execute(
                f"GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA {schema} TO {role}"
            )
            cur.execute(f"GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA {schema} TO {role}")
            cur.execute(f"GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA {schema} TO {role}")
        cur.execute(
            "SELECT has_schema_privilege(%s, 'okg', 'CREATE'), "
            "has_database_privilege(%s, current_database(), 'TEMP')",
            (role, role),
        )
        assert cur.fetchone() == (False, True)


@pytest.fixture
def no_create_role(clean_runtime: str):
    """A login-less role shaped like app_rw where it matters here: it may read
    and write every table and call every function, and it has TEMP on the
    database (PUBLIC's default), but it has no CREATE on schema okg --
    schema.sql revokes exactly that from app_rw. app_rw itself cannot stand in:
    in a bare test database it also lacks the chronos branch-table grants that
    a runtime bootstrap adds, so its materialize fails before the hash."""
    role = f"pah_no_create_{uuid.uuid4().hex[:8]}"
    with psycopg.connect(clean_runtime, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute("SELECT has_schema_privilege('app_rw', 'okg', 'CREATE')")
        assert cur.fetchone()[0] is False, "app_rw is expected to lack CREATE on okg"
        cur.execute(f"CREATE ROLE {role} NOLOGIN")
    try:
        yield role
    finally:
        with psycopg.connect(clean_runtime, autocommit=True) as conn, conn.cursor() as cur:
            cur.execute(f"DROP OWNED BY {role}")
            cur.execute(f"DROP ROLE {role}")


def test_publisher_without_create_on_okg_hashes_serially_and_says_so(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    no_create_role: str,
) -> None:
    """A publishing role without CREATE on schema okg (app_rw's restriction)
    must still materialize, on the old TEMP path, with the same rows as the
    owner's parallel path, and must report why it was not parallel."""
    seeded = _seed(clean_runtime)
    _grant_table_rights(clean_runtime, no_create_role)
    generation = f"gen-no-create-{seeded['suffix']}"

    owner, _ = _run_path(clean_runtime, generation, monkeypatch, parallel=True)
    caplog.set_level("WARNING", logger=attributes_module.__name__)
    result, rows, instr, leftovers = _serial_fallback_run(
        clean_runtime, generation, monkeypatch, setup_sql=f"SET ROLE {no_create_role}"
    )

    assert result["materialized"] is True, result
    reason = "unavailable: no CREATE on schema okg for the publishing role"
    assert result["parallel_hash"] == {"node": reason, "edge": reason}, result
    assert instr["parallel_hash_unavailable"] == 2, instr
    assert instr["unlogged_stages"] == 0 and instr["parallel_hash_stages"] == 0, instr
    assert leftovers == []
    warnings = [
        r.getMessage() for r in caplog.records if "parallel content hash" in r.getMessage()
    ]
    assert len(warnings) == 2, warnings
    assert set(rows) == set(owner) and len(rows) == 2 * len(seeded["nodes"])
    for key, row in owner.items():
        assert rows[key] == row, (key, rows[key], row)


def test_operator_disabled_parallel_query_is_honored(
    clean_runtime: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    seeded = _seed(clean_runtime)
    generation = f"gen-no-par-{seeded['suffix']}"
    result, rows, instr, leftovers = _serial_fallback_run(
        clean_runtime,
        generation,
        monkeypatch,
        setup_sql="SET max_parallel_workers_per_gather = 0",
    )
    reason = "unavailable: max_parallel_workers_per_gather is 0 (parallel query disabled)"
    assert result["parallel_hash"] == {"node": reason, "edge": reason}, result
    assert instr["parallel_hash_stages"] == 0 and instr["unlogged_stages"] == 0, instr
    assert leftovers == [] and len(rows) == 2 * len(seeded["nodes"])


def test_stage_table_dropped_when_hash_statement_fails(
    clean_runtime: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    seeded = _seed(clean_runtime)
    generation = f"gen-fail-{seeded['suffix']}"
    seen: list[list[str]] = []

    def _boom(conn, db, stage_sql, params, *, rows, workers):
        seen.append(_stage_tables(conn))
        raise RuntimeError("injected hash failure")

    monkeypatch.setattr(attributes_module, "_execute_hash_stage", _boom)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        with pytest.raises(RuntimeError, match="injected hash failure"):
            _materialize(conn, generation)
        # It existed while the statement ran, and the rollback removed it.
        assert len(seen) == 1 and len(seen[0]) == 1, seen
        assert seen[0][0].startswith(f"_eff_stage_node_{conn.info.backend_pid}_")
        assert _stage_tables(conn) == []
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT count(*) FROM {attributes_module.ATTRIBUTE_EFFECTIVE_VIEW_TABLE} "
                "WHERE generation_id = %s",
                (generation,),
            )
            assert cur.fetchone()[0] == 0
    with psycopg.connect(clean_runtime, autocommit=True) as other:
        assert _stage_tables(other) == []


def test_stage_table_refused_outside_a_transaction(clean_runtime: str) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        with pytest.raises(SubstrateError, match="requires an open transaction"):
            attributes_module._unlogged_stage_table_name(conn, "node")


def test_dirty_publish_through_unlogged_stage_matches_full_rebuild(
    clean_runtime: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A real incremental publish whose merge set crosses the threshold stages
    through the unlogged table via the keyed interval lookup, and its snapshot
    is byte-identical to a full re-materialize of the same graph."""
    from okg.substrate.publisher import Publisher
    from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer

    # The byte-identity of a snapshot against a full rebuild is a snapshot-arm
    # contract (see test_effective_attribute_view_oracle.py).
    monkeypatch.setenv("OKG_IVM_INTERVAL_VIEW", "0")
    seeded = _seed(clean_runtime)

    def _publish() -> str:
        result = Publisher(
            clean_runtime,
            "default",
            catalog_identity=bind_test_catalog_writer(clean_runtime, "default"),
        ).cycle()
        assert result["status"] == "published", result
        return str(result["generation_id"])

    g1 = _publish()
    changed = seeded["nodes"][0]
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        ChronosGraphStore.from_connection(conn).apply_facts(
            [NodeFact(changed, "doc", {**_payload(99), "rev": 2}, {"id": changed}, {"v": 2})],
            source="default",
            strict_endpoints=True,
        )
    monkeypatch.setattr(attributes_module, "_PARALLEL_HASH_MIN_ROWS", 1)
    attributes_module._reset_materialize_instrumentation()
    g2 = _publish()
    assert g2 != g1
    # Nodes crossed the (lowered) threshold with one dirty target; the edge
    # merge set was empty and kept the temp path.
    assert _INSTR["unlogged_stages"] == 1, dict(_INSTR)
    assert _INSTR["parallel_hash_stages"] == 1, dict(_INSTR)
    assert _INSTR["staged_rows"] == 1, dict(_INSTR)

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        assert _stage_tables(conn) == []
        incr = _capture(conn, g2)
        _materialize(conn, f"{g2}:full")
        full = _capture(conn, f"{g2}:full")
    assert incr[("node", changed)][-2]["rev"] == 2
    assert set(incr) == set(full), set(incr) ^ set(full)
    for key, row in full.items():
        assert incr[key] == row, (key, incr[key], row)


def test_small_dirty_cycle_keeps_temp_staging() -> None:
    small = attributes_module._PARALLEL_HASH_MIN_ROWS - 1
    assert attributes_module._raw_stage_is_unlogged(dirty_driven=False, merge_count=0)
    assert not attributes_module._raw_stage_is_unlogged(
        dirty_driven=True, merge_count=small
    )
    assert attributes_module._raw_stage_is_unlogged(
        dirty_driven=True, merge_count=small + 1
    )
