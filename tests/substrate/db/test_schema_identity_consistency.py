"""Identity-convention contract for the v1.0 schema (schema-identity-convergence).

Three requirements are covered here:

* ``generation-identity`` — the TEXT checkpoint ref is the only generation
  reference in the granted surface; the ``okg.generation_ordinals`` grant to
  ``okg_mcp`` is gone; the documented ``work_ledger`` <-> ``llm_calls`` cost
  join executes cast-free on populated data; fork-without-ordinal stays a loud
  refusal.
* ``branch-naming`` and ``deployment-scoping`` — enforced today as a FREEZE
  gate: every divergence is registered with an owning PACT, and an
  unregistered one reds the build. The three mutation fixtures prove the gate
  is not vacuous.

The mutation fixtures apply their defect inside a transaction on the real
migrated schema and roll it back, so the check sees the true catalog plus
exactly the defect under test and nothing leaks into the shared test schema
(guarded by `test_generation_ordinals_grant_does_not_survive_the_rollback`).
"""
from __future__ import annotations

from contextlib import contextmanager
import importlib.util
from pathlib import Path
import sys
from typing import Any, Iterator
from uuid import uuid4

import psycopg
import pytest
import yaml


pytestmark = pytest.mark.postgres

REPO_ROOT = Path(__file__).resolve().parents[3]
SCRIPTS_DIR = REPO_ROOT / "scripts"
REGISTRY_PATH = (
    REPO_ROOT / "src" / "okg" / "substrate" / "db" / "identity_divergences.yaml"
)


def _load_script(name: str) -> Any:
    """Import a ``scripts/<name>.py`` helper (they are not a package)."""
    spec = importlib.util.spec_from_file_location(name, SCRIPTS_DIR / f"{name}.py")
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="module")
def identity_check() -> Any:
    return _load_script("check_schema_identity")


@contextmanager
def mutated_schema(dsn: str, *statements: str) -> Iterator[psycopg.Connection[Any]]:
    """Apply `statements` inside a transaction, yield the connection, roll back.

    Postgres DDL is transactional, so the identity check sees the real migrated
    catalog plus exactly the defect under test, and the database is untouched
    afterwards. `run_identity_checks(conn=...)` runs on this same connection so
    it observes the uncommitted mutation — a second connection would not.
    """
    with psycopg.connect(dsn) as conn:
        try:
            with conn.cursor() as cur:
                for statement in statements:
                    cur.execute(statement)
            yield conn
        finally:
            conn.rollback()


# ---------------------------------------------------------------------------
# The gate itself
# ---------------------------------------------------------------------------


def test_identity_registry_is_well_formed(identity_check: Any) -> None:
    """Every registered divergence names an owning PACT and a reason.

    An empty SECTION is allowed; an empty registry is not. A convergence lane
    proves it landed by DELETING its rows, so `generation_functions` emptied
    when `schema-canonical-read-path` retyped okg.current_pin() to the TEXT
    checkpoint ref — the last entry it held. Requiring every section to be
    non-empty would have made "finish converging a whole class" the one thing
    the gate refuses.

    What stays non-negotiable is that the registry as a whole still describes
    real, unconverged divergences: if EVERY section empties, this file has
    become a test of nothing and should be replaced by the gate alone.
    """
    registry = identity_check.load_registry(REGISTRY_PATH)
    sections = (
        "generation_refs",
        "generation_functions",
        "branch_naming",
        "deployment_scoping",
    )
    assert set(sections) <= set(registry)
    total = 0
    for section in sections:
        entries = registry[section] or []
        total += len(entries)
        for entry in entries:
            assert entry["owner"].strip()
            assert entry["reason"].strip()
    assert total, "the registry is empty; retire this test with the last row"


def test_identity_section_is_green_on_the_real_tree(
    identity_check: Any, okg_migrated: str
) -> None:
    """The shipped schema passes; unregistered divergences are the failure."""
    result = identity_check.run_identity_checks(dsn=okg_migrated)
    assert result["violations"] == []
    assert result["ok"] is True
    # Not vacuous: the checks actually looked at a populated catalog.
    assert result["checks"]["generation_refs"]["granted_relations"] > 20
    assert result["checks"]["generation_refs"]["granted_role_present"] is True


def test_identity_section_is_wired_into_schema_quality(okg_migrated: str) -> None:
    """`okg schema quality` carries a BLOCKING identity section."""
    from okg.substrate.cli.schema import run_quality

    envelope = run_quality(dsn=okg_migrated, include_lint=False)
    identity = [s for s in envelope["sections"] if s["name"] == "identity"]
    assert len(identity) == 1
    assert identity[0]["gating"] is True
    assert identity[0]["status"] == "pass", identity[0]


# --- mutation fixture 1: an exposed ordinal column in the granted surface ---


def test_identity_gate_is_red_on_exposed_ordinal_generation_column(
    identity_check: Any, okg_migrated: str
) -> None:
    with mutated_schema(
        okg_migrated,
        """
        CREATE TABLE okg.identity_fixture_runs (
          run_id          BIGSERIAL PRIMARY KEY,
          deployment_name TEXT NOT NULL,
          target_generation_id BIGINT NOT NULL
        )
        """,
        "GRANT SELECT ON okg.identity_fixture_runs TO okg_mcp",
    ) as conn:
        result = identity_check.run_identity_checks(conn=conn)

    assert result["ok"] is False
    offenders = [
        v for v in result["violations"]
        if v["relation"] == "okg.identity_fixture_runs"
    ]
    assert offenders, result["violations"]
    assert offenders[0]["check"] == "generation_refs"
    assert offenders[0]["column"] == "target_generation_id"


def test_identity_gate_ignores_an_ordinal_column_outside_the_granted_surface(
    identity_check: Any, okg_migrated: str
) -> None:
    """The predicate is scoped to the granted surface, and provably so.

    Without this, the previous test would pass for the wrong reason — a check
    that flagged every ordinal generation column anywhere would also be red,
    and we could not tell the grant closure was doing any work. The two tables
    differ only in the GRANT.
    """
    with mutated_schema(
        okg_migrated,
        """
        CREATE TABLE okg.identity_fixture_internal (
          run_id BIGSERIAL PRIMARY KEY,
          target_generation_id BIGINT NOT NULL
        )
        """,
    ) as conn:
        result = identity_check.run_identity_checks(conn=conn)

    offenders = [
        v for v in result["violations"]
        if v["relation"] == "okg.identity_fixture_internal"
    ]
    assert offenders == []


def test_identity_gate_is_red_on_an_ordinal_column_reached_through_a_view(
    identity_check: Any, okg_migrated: str
) -> None:
    """The granted surface is pg_depend-closed, not just the direct grants.

    The base table is ungranted; only the view over it is granted. A check that
    looked at direct grants alone would miss this, which is exactly the hole
    D2 amendment (c) names.
    """
    with mutated_schema(
        okg_migrated,
        """
        CREATE TABLE okg.identity_fixture_hidden (
          run_id BIGSERIAL PRIMARY KEY,
          target_generation_id BIGINT NOT NULL
        )
        """,
        """
        CREATE VIEW okg.v_identity_fixture_hidden AS
        SELECT run_id, target_generation_id FROM okg.identity_fixture_hidden
        """,
        "GRANT SELECT ON okg.v_identity_fixture_hidden TO okg_mcp",
    ) as conn:
        result = identity_check.run_identity_checks(conn=conn)

    assert result["ok"] is False
    offending = {v["relation"] for v in result["violations"]}
    assert "okg.identity_fixture_hidden" in offending, result["violations"]
    assert "okg.v_identity_fixture_hidden" in offending, result["violations"]


# --- mutation fixture 2: a type-divergent branch_id -------------------------


def test_identity_gate_is_red_on_type_divergent_branch_id(
    identity_check: Any, okg_migrated: str
) -> None:
    with mutated_schema(
        okg_migrated,
        """
        CREATE TABLE okg.identity_fixture_branch (
          branch_id TEXT NOT NULL,
          note      TEXT
        )
        """,
    ) as conn:
        result = identity_check.run_identity_checks(conn=conn)

    assert result["ok"] is False
    offenders = [
        v for v in result["violations"]
        if v["relation"] == "okg.identity_fixture_branch"
    ]
    assert offenders, result["violations"]
    assert offenders[0]["check"] == "branch_naming"
    assert offenders[0]["type"] == "text"
    assert offenders[0]["expected"] == "bigint"


def test_identity_gate_accepts_a_correctly_named_chronos_branch_column(
    identity_check: Any, okg_migrated: str
) -> None:
    """The rule is about the NAME `branch_id`, not about TEXT branch ids.

    A chronos TEXT branch identifier carried under the prescribed
    `chronos_branch_id` name is exactly what §4 asks for and must pass.
    """
    with mutated_schema(
        okg_migrated,
        """
        CREATE TABLE okg.identity_fixture_chronos_branch (
          chronos_branch_id TEXT NOT NULL,
          note              TEXT
        )
        """,
    ) as conn:
        result = identity_check.run_identity_checks(conn=conn)

    offenders = [
        v for v in result["violations"]
        if v["relation"] == "okg.identity_fixture_chronos_branch"
    ]
    assert offenders == []


# --- mutation fixture 3: a bare `deployment` column -------------------------


def test_identity_gate_is_red_on_bare_deployment_column(
    identity_check: Any, okg_migrated: str
) -> None:
    with mutated_schema(
        okg_migrated,
        """
        CREATE TABLE okg.identity_fixture_deployment (
          deployment TEXT NOT NULL,
          note       TEXT
        )
        """,
    ) as conn:
        result = identity_check.run_identity_checks(conn=conn)

    assert result["ok"] is False
    offenders = [
        v for v in result["violations"]
        if v["relation"] == "okg.identity_fixture_deployment"
    ]
    assert offenders, result["violations"]
    assert offenders[0]["check"] == "deployment_scoping"


# --- review follow-ups: the arms that used to be blind ----------------------


def test_identity_gate_refuses_to_pass_without_the_read_role(
    identity_check: Any, okg_migrated: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """No okg_mcp role means the granted surface is undefined -- not a pass.

    Without this the generation-reference and direct-grant arms silently
    examine nothing and the section reports PASS, which is the
    default-as-fallback P0: the check would be quietest exactly where it can
    see least.
    """
    monkeypatch.setattr(identity_check, "_GRANTED_ROLE", "a_role_that_does_not_exist")
    result = identity_check.run_identity_checks(dsn=okg_migrated)

    assert result["ok"] is False
    missing = [v for v in result["violations"] if v["check"] == "granted_role_missing"]
    assert missing, result["violations"]
    assert result["checks"]["generation_refs"]["granted_role_present"] is False


def test_identity_gate_is_red_on_an_ordinal_generation_function(
    identity_check: Any, okg_migrated: str
) -> None:
    """EXECUTE-granted functions are in the freeze surface too (design §5).

    okg_mcp holds GRANT EXECUTE ON ALL FUNCTIONS, so a function taking a
    bigint generation parameter exposes ordinal generation identity exactly as
    a column would. Before this arm existed the gate could not see them at all.
    """
    with mutated_schema(
        okg_migrated,
        """
        CREATE FUNCTION okg.identity_fixture_fn(p_generation_id BIGINT)
        RETURNS BOOLEAN LANGUAGE sql IMMUTABLE
        AS $$ SELECT p_generation_id > 0 $$
        """,
        "GRANT EXECUTE ON FUNCTION okg.identity_fixture_fn(BIGINT) TO okg_mcp",
    ) as conn:
        result = identity_check.run_identity_checks(conn=conn)

    assert result["ok"] is False
    offenders = [
        v for v in result["violations"]
        if v["relation"] == "okg.identity_fixture_fn"
    ]
    assert offenders, result["violations"]
    assert offenders[0]["check"] == "generation_functions"
    assert "p_generation_id" in offenders[0]["column"]


def test_identity_gate_accepts_a_text_generation_function_parameter(
    identity_check: Any, okg_migrated: str
) -> None:
    """The function arm objects to the TYPE, not to the parameter name."""
    with mutated_schema(
        okg_migrated,
        """
        CREATE FUNCTION okg.identity_fixture_fn_text(p_generation_id TEXT)
        RETURNS BOOLEAN LANGUAGE sql IMMUTABLE
        AS $$ SELECT length(p_generation_id) > 0 $$
        """,
        "GRANT EXECUTE ON FUNCTION okg.identity_fixture_fn_text(TEXT) TO okg_mcp",
    ) as conn:
        result = identity_check.run_identity_checks(conn=conn)

    offenders = [
        v for v in result["violations"]
        if v["relation"] == "okg.identity_fixture_fn_text"
    ]
    assert offenders == []


def test_registry_rows_for_absent_relations_are_not_stale(
    identity_check: Any, okg_migrated: str, tmp_path: Path
) -> None:
    """A registered relation that does not exist has not converged.

    Several registered relations are runtime-ensured and are absent until the
    first publish. Treating "relation absent" as "row is stale" would red a
    freshly migrated database for no divergence at all, and would make the
    gate's verdict depend on whether some earlier test happened to publish.
    """
    registry = identity_check.load_registry(REGISTRY_PATH)
    registry["deployment_scoping"].append({
        "relation": "okg.a_runtime_table_not_created_yet",
        "owner": "schema-single-source-ddl",
        "reason": "planted by a test",
    })
    doctored = tmp_path / "identity_divergences.yaml"
    doctored.write_text(yaml.safe_dump(registry, sort_keys=False), encoding="utf-8")

    result = identity_check.run_identity_checks(
        dsn=okg_migrated, registry_path=doctored
    )

    assert result["ok"] is True, result["violations"]
    hygiene = result["checks"]["registry_hygiene"]
    assert hygiene["stale_registrations"] == 0
    assert "okg.a_runtime_table_not_created_yet" in (
        hygiene["registered_relations_absent"]
    )


def test_schema_quality_names_an_unavailable_gating_tier(okg_migrated: str) -> None:
    """A gating tier that could not be certified must appear in the cause.

    It already failed the envelope; without this it failed it anonymously, and
    the operator had to read the JSON to find out which tier was unavailable.
    """
    from okg.substrate.cli import schema as schema_cli

    envelope = schema_cli.run_quality(dsn=None, include_lint=False)
    assert envelope["ok"] is False
    assert envelope["gating_failures"], envelope
    assert "sql_assertions" in envelope["gating_failures"]
    assert "sql_assertions" in envelope["summary"]


# --- mutation fixture 5: a registry row that no longer matches anything -----


def test_identity_gate_is_red_on_a_stale_registration(
    identity_check: Any, okg_migrated: str, tmp_path: Path
) -> None:
    """A relation that EXISTS but no longer diverges must lose its row.

    This is the "absence dressed as agreement" case: a stale exemption sits
    there forever, ready to silently cover the same divergence if it comes
    back, and nothing ever forces a convergence lane to delete its row. The
    check must refuse it rather than carrying it quietly.

    `okg.branches` is the fixture because it exists on every deployment and
    carries no bare `deployment` column — so a deployment_scoping row naming it
    is exactly a converged divergence somebody forgot to delete. Contrast
    `test_registry_rows_for_absent_relations_are_not_stale`: a relation that
    does not exist at all has not converged, it just has not been created.
    """
    registry = identity_check.load_registry(REGISTRY_PATH)
    registry["deployment_scoping"].append({
        "relation": "okg.branches",
        "owner": "nobody",
        "reason": "planted by a test",
    })
    doctored = tmp_path / "identity_divergences.yaml"
    doctored.write_text(yaml.safe_dump(registry, sort_keys=False), encoding="utf-8")

    result = identity_check.run_identity_checks(
        dsn=okg_migrated, registry_path=doctored
    )

    assert result["ok"] is False
    stale = [v for v in result["violations"] if v["check"] == "stale_registration"]
    assert len(stale) == 1, result["violations"]
    assert stale[0]["relation"] == "okg.branches"
    assert result["checks"]["registry_hygiene"]["stale_registrations"] == 1


# --- mutation fixture 4: re-granting the ordinal registry -------------------


def test_identity_gate_is_red_when_generation_ordinals_is_regranted(
    identity_check: Any, okg_migrated: str
) -> None:
    """D2 amendment (a): okg_mcp must not hold a direct grant on the registry.

    This is the regression guard for the one grant this change removed — and
    `attributes.py::_grant_interval_view_to_mcp` still contains a code path
    that would re-add it (see the PR handoff), so the guard is load-bearing.
    """
    with psycopg.connect(okg_migrated) as probe, probe.cursor() as cur:
        cur.execute("SELECT to_regclass('okg.generation_ordinals')")
        if cur.fetchone()[0] is None:
            pytest.skip("okg.generation_ordinals is not present on this database")

    with mutated_schema(
        okg_migrated,
        "GRANT SELECT ON okg.generation_ordinals TO okg_mcp",
    ) as conn:
        result = identity_check.run_identity_checks(conn=conn)

    assert result["ok"] is False
    offenders = [
        v for v in result["violations"] if v["check"] == "direct_grant_forbidden"
    ]
    assert offenders, result["violations"]
    assert offenders[0]["relation"] == "okg.generation_ordinals"


def test_generation_ordinals_grant_does_not_survive_the_rollback(
    identity_check: Any, okg_migrated: str
) -> None:
    """The mutation fixtures must not leak into the shared test schema.

    Without this the whole rolled-back-DDL approach could corrupt every later
    test in the session and nobody would notice until something unrelated went
    red.
    """
    result = identity_check.run_identity_checks(dsn=okg_migrated)
    assert result["violations"] == []


# ---------------------------------------------------------------------------
# Requirement: generation-identity — the shipped column shapes
# ---------------------------------------------------------------------------


def test_generation_reference_columns_are_text_c_collated(okg_migrated: str) -> None:
    """The converted columns are TEXT COLLATE "C" (D2 amendment (d))."""
    expected = {
        ("admission_envelopes", "target_generation_ref"),
        ("ir_invocations", "generation_id"),
        ("ir_program_bindings", "generation_id"),
        ("llm_calls", "generation_id"),
        ("nomos_config_sets", "source_generation_id"),
        ("nomos_policy_bundles", "source_generation_id"),
        ("work_ledger", "generation_id"),
    }
    with psycopg.connect(okg_migrated) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT c.relname, a.attname,
                   format_type(a.atttypid, a.atttypmod),
                   co.collname
              FROM pg_class c
              JOIN pg_namespace n ON n.oid = c.relnamespace
              JOIN pg_attribute a ON a.attrelid = c.oid
                                 AND a.attnum > 0
                                 AND NOT a.attisdropped
              LEFT JOIN pg_collation co ON co.oid = a.attcollation
             WHERE n.nspname = 'okg'
            """
        )
        observed = {
            (relname, attname): (typ, collname)
            for relname, attname, typ, collname in cur.fetchall()
        }
    for key in expected:
        assert key in observed, f"{key} is missing from the migrated schema"
        typ, collation = observed[key]
        assert typ == "text", f"{key} is {typ}, expected text"
        assert collation == "C", f"{key} collation is {collation!r}, expected 'C'"


def test_admission_envelopes_has_no_numeric_generation_token(
    okg_migrated: str,
) -> None:
    """D2: target_generation_ref is the ONLY generation reference."""
    with psycopg.connect(okg_migrated) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT attname
              FROM pg_attribute
             WHERE attrelid = 'okg.admission_envelopes'::regclass
               AND attnum > 0 AND NOT attisdropped
               AND attname LIKE '%%generation%%'
            """
        )
        columns = {row[0] for row in cur.fetchall()}
        cur.execute(
            """
            SELECT attname
              FROM pg_attribute
             WHERE attrelid = 'okg.v_admission_envelopes'::regclass
               AND attnum > 0 AND NOT attisdropped
               AND attname LIKE '%%generation%%'
            """
        )
        view_columns = {row[0] for row in cur.fetchall()}
    assert columns == {"target_generation_ref"}
    assert view_columns == {"target_generation_ref", "canonical_generation_ref"}


def test_mcp_reads_generation_ordinals_through_the_text_ref_view(
    okg_migrated: str,
) -> None:
    """The replacement surface exists, is granted, and is keyed on the ref."""
    with psycopg.connect(okg_migrated) as conn, conn.cursor() as cur:
        cur.execute("SELECT to_regclass('okg.generation_ordinals')")
        if cur.fetchone()[0] is None:
            pytest.skip("okg.generation_ordinals is not present on this database")
        cur.execute(
            "SELECT has_table_privilege('okg_mcp', %s, 'SELECT')",
            ("okg.generation_ordinals",),
        )
        assert cur.fetchone()[0] is False
        cur.execute(
            "SELECT has_table_privilege('okg_mcp', %s, 'SELECT')",
            ("okg.v_generation_interval_stab",),
        )
        assert cur.fetchone()[0] is True
        cur.execute(
            """
            SELECT attname
              FROM pg_attribute
             WHERE attrelid = 'okg.v_generation_interval_stab'::regclass
               AND attnum > 0 AND NOT attisdropped
            """
        )
        columns = {row[0] for row in cur.fetchall()}
    # The lookup key is the TEXT ref; the ordinal is present only under a name
    # that cannot be mistaken for a generation identity.
    assert "generation_ref" in columns
    assert "chronos_branch_id" in columns
    assert "stab_ordinal" in columns
    assert "checkpoint_id" not in columns
    assert "ordinal" not in columns


def test_mcp_server_reads_the_view_not_the_ordinal_table() -> None:
    """The MCP interval reader was switched to the TEXT-ref-keyed view."""
    source = (
        REPO_ROOT / "src" / "okg" / "substrate" / "mcp" / "server.py"
    ).read_text(encoding="utf-8")
    assert "FROM okg.generation_ordinals" not in source
    assert "okg.v_generation_interval_stab" in source


def test_ir_binding_dedupe_sentinel_cannot_be_forged(clean_runtime: str) -> None:
    """The '' dedupe sentinel is enforced, not merely asserted in a comment.

    `ir_program_bindings_dedupe_idx` collapses a NULL generation to '' (D2
    retired the numeric -1 sentinel with the BIGINT column). That is sound only
    while no real generation ref can BE the empty string; without the CHECK, an
    empty-string ref written by a caller bug would dedupe against every
    unbound binding instead of failing.
    """
    from psycopg import errors

    with psycopg.connect(clean_runtime, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT conname, convalidated
              FROM pg_constraint
             WHERE conrelid = 'okg.ir_program_bindings'::regclass
               AND conname = 'ir_program_bindings_generation_id_nonempty_chk'
            """
        )
        constraint = cur.fetchone()
        assert constraint is not None, "the nonempty CHECK must exist"
        assert constraint[1] is True, "the CHECK must be born VALID, not NOT VALID"

        cur.execute(
            """
            INSERT INTO okg.ir_programs (
              program_id, program_checksum, ir_version, program_kind,
              determinism, payload_mode, program_json
            )
            VALUES (
              gen_random_uuid(), %s, 'v1', 'read',
              'deterministic', 'full', '{}'::jsonb
            )
            RETURNING program_id::text
            """,
            (f"sentinel-{uuid4().hex[:12]}",),
        )
        program_id = cur.fetchone()[0]

        with pytest.raises(errors.CheckViolation):
            cur.execute(
                """
                INSERT INTO okg.ir_program_bindings (
                  program_id, deployment_name, owner_kind, owner_id,
                  program_role, generation_id
                )
                VALUES (%s::uuid, 'd', 'mcp_call', 'o', 'read', '')
                """,
                (program_id,),
            )


# ---------------------------------------------------------------------------
# Requirement: generation-identity — the cost join, on populated data
# ---------------------------------------------------------------------------


def test_work_ledger_llm_calls_cost_join_runs_cast_free_on_rows(
    clean_runtime: str,
) -> None:
    """The documented cost join returns real spend for a Chronos generation ref.

    This is the defect the requirement names. While ``llm_calls.generation_id``
    was BIGINT, ``record_llm_call`` coerced every Chronos checkpoint ref to
    NULL and the join reported an all-zero spend — a silent wrong answer that
    read as "this generation charged nothing".
    """
    from okg.substrate.telemetry.work_ledger import model_call_spend

    # okg.llm_calls and okg.work_ledger are append-only ledgers that
    # `clean_runtime` deliberately does NOT truncate, so a fixed deployment
    # name would accumulate rows across runs and make the counts drift.
    unique = uuid4().hex[:12]
    generation_ref = f"okg:test:branch:default:gen:20260804T000000000000Z:{unique}"
    deployment = f"identity-cost-join-{unique}"
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        with conn.cursor() as cur:
            for tokens, cost in ((100, "0.01"), (250, "0.02")):
                cur.execute(
                    """
                    INSERT INTO okg.llm_calls
                        (deployment_name, caller, model, prompt_tokens,
                         completion_tokens, total_tokens, cost_usd, success,
                         generation_id)
                    VALUES (%s, 'invariants:test', 'test-model', %s, %s, %s,
                            %s, TRUE, %s)
                    """,
                    (deployment, tokens, tokens, tokens * 2, cost, generation_ref),
                )
            # A failed call and another generation's call must not be counted.
            cur.execute(
                """
                INSERT INTO okg.llm_calls
                    (deployment_name, caller, model, prompt_tokens, completion_tokens,
                     total_tokens, cost_usd, success, generation_id)
                VALUES (%s, 'invariants:test', 'test-model', 999, 999, 1998,
                        '9.99', FALSE, %s)
                """,
                (deployment, generation_ref),
            )
            cur.execute(
                """
                INSERT INTO okg.work_ledger
                    (deployment_name, generation_id, category, measured, work_units)
                VALUES (%s, %s, 'model_calls', TRUE, 2)
                """,
                (deployment, generation_ref),
            )
            # The join executes with no cast on either side: both columns are
            # TEXT COLLATE "C" and the predicate compares them directly.
            cur.execute(
                """
                SELECT count(*)
                  FROM okg.work_ledger w
                  JOIN okg.llm_calls l
                    ON l.generation_id = w.generation_id
                   AND l.deployment_name = w.deployment_name
                 WHERE w.generation_id = %s
                   AND l.success
                """,
                (generation_ref,),
            )
            joined = cur.fetchone()[0]

        spend = model_call_spend(
            conn, deployment=deployment, generation_id=generation_ref
        )

    assert joined == 2, "the SQL join itself must match both successful calls"
    assert spend.calls == 2
    assert spend.prompt_tokens == 350
    assert spend.total_tokens == 700
    assert spend.cost_usd == pytest.approx(0.03)


def test_llm_call_recorder_stores_the_chronos_ref_verbatim(
    clean_runtime: str,
) -> None:
    """``record_llm_call`` no longer drops non-numeric generation refs."""
    from okg.substrate.llm.tracker import record_llm_call

    # okg.llm_calls is append-only and survives `clean_runtime`, so the caller
    # tag has to be unique per run or a re-run reads a previous row.
    unique = uuid4().hex[:12]
    caller = f"invariants:verbatim:{unique}"
    generation_ref = f"okg:test:branch:default:gen:20260804T111111111111Z:{unique}"
    record_llm_call(
        clean_runtime,
        caller=caller,
        model="test-model",
        fragment_id=None,
        prompt_tokens=1,
        completion_tokens=1,
        total_tokens=2,
        cost_usd=0.0,
        latency_ms=1,
        success=True,
        deployment="identity-verbatim",
        generation_id=generation_ref,
    )
    with psycopg.connect(clean_runtime) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT generation_id FROM okg.llm_calls WHERE caller = %s",
            (caller,),
        )
        rows = cur.fetchall()
    assert len(rows) == 1, "exactly one telemetry row must be written"
    assert rows[0][0] == generation_ref


# ---------------------------------------------------------------------------
# Requirement: generation-identity — fork-without-ordinal stays loud
# ---------------------------------------------------------------------------


def test_fork_without_base_ordinal_refuses_loudly() -> None:
    """D2 amendment (b): never a wall-clock or silent fallback.

    A fork whose base generation has no ordinal must raise, not invent an
    ordinal — inventing one silently misplaces the entire fork in causal order.
    """
    from okg.substrate.errors import SubstrateError
    from okg.substrate.graph_store_internal import generation_ordinal as mod

    class _FakeDb:
        def execute(self, query: str, params: tuple[Any, ...] = ()) -> Any:
            class _Cur:
                @staticmethod
                def fetchone() -> None:
                    # No ordinal row exists for the fork base.
                    return None

            return _Cur()

    class _Info:
        metadata = {"base_generation_id": "generation:missing-base"}

    class _FakeContext:
        @staticmethod
        def get_branch(branch: str) -> _Info:
            return _Info()

    with pytest.raises(SubstrateError) as excinfo:
        mod._resolve_fork_base_ordinal(_FakeDb(), _FakeContext(), "forked-branch")

    message = str(excinfo.value)
    assert "fork base generation has no ordinal" in message
    assert "backfill" in message


# ---------------------------------------------------------------------------
# Requirement: generation-identity — the correlation that replaced the column
# ---------------------------------------------------------------------------


def test_lens_finalize_only_claims_envelopes_from_its_own_cycle(
    clean_runtime: str,
) -> None:
    """A later cycle must not adopt an earlier cycle's unlinked envelope.

    Dropping `okg.admission_envelopes.target_generation_id` removed the key the
    publisher used to identify "the envelopes THIS cycle drained". The
    replacement stamps the cycle token into the envelope's verdict JSONB. If
    that correlation were dropped in favour of "any still-unlinked envelope on
    this deployment+branch", a cycle that minted its checkpoint and then crashed
    before finalizing would have its envelopes adopted by the NEXT cycle and
    attributed to the wrong generation — confidently wrong, not loudly
    incomplete. This test pins the narrow behaviour directly, without needing to
    stage a real crash.
    """
    from okg.substrate.publisher.publisher import Publisher

    deployment = f"identity-finalize-{uuid4().hex[:8]}"
    stranded_ref = "okg:x:branch:default:gen:20260101T000000000000Z:stranded0000"
    stranded_token = "111111111111"
    mine_token = "222222222222"
    new_ref = "okg:x:branch:default:gen:20260804T000000000000Z:mine00000000"

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        with conn.cursor() as cur:
            # Two lens envelopes on the same deployment+branch, both published
            # and both still unlinked. They differ ONLY in their cycle token.
            for key, token in (
                ("stranded", stranded_token),
                ("mine", mine_token),
            ):
                cur.execute(
                    """
                    INSERT INTO okg.admission_envelopes (
                      deployment_name, producer_id, producer_kind, intent,
                      admission_mode, status, idempotency_key, verdict
                    )
                    VALUES (
                      %(deployment)s, 'lens:test', 'lens', 'publish',
                      'direct', 'published', %(key)s,
                      jsonb_build_object(
                        'applied',
                        jsonb_build_object('cycle_token', %(token)s::text)
                      )
                    )
                    """,
                    {"deployment": deployment, "key": key, "token": token},
                )

        publisher = Publisher(clean_runtime, deployment)
        linked = publisher._finalize_lens_promotion_generation_refs(
            conn,
            cycle_token=mine_token,
            generation_ref=new_ref,
            generation_status="published",
        )

        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT idempotency_key, target_generation_ref
                  FROM okg.admission_envelopes
                 WHERE deployment_name = %s
                 ORDER BY idempotency_key
                """,
                (deployment,),
            )
            rows = dict(cur.fetchall())

    assert linked == 1, "exactly this cycle's envelope may be linked"
    assert rows["mine"] == new_ref
    assert rows["stranded"] is None, (
        "an envelope left unlinked by an earlier cycle must stay unlinked, "
        "never be adopted by a later generation"
    )
    assert stranded_ref not in rows.values()
