"""Every MCP operator, exercised on a connection that IS okg_mcp.

The rest of the MCP suite builds its server against the test DSN, which is the
deployment owner -- a superuser on the local compose stack. That is how
`okg.subtype_ancestors` went unnoticed: profiled search reads it on the read
pool, no allow-list ever contained it, and it worked in production only while
`role_split.sql`'s blanket `GRANT SELECT ON ALL TABLES` happened to run after
`views.sql`. Every test passed throughout, because none of them was ever the
constrained role.

`schema-grant-coherence` deleted that blanket, so the registry is now the only
thing standing between the read pool and `permission denied`. This module is
the floor that makes a missing registry row fail HERE rather than in an
operator's session: one call per operator, on a connection whose session role
is okg_mcp, asserting no InsufficientPrivilege.

It deliberately does NOT assert result contents -- the operator suites own
that. It asserts reachability, which is the property the grant surface
decides.

WHAT THIS CAN AND CANNOT CATCH, measured rather than assumed. The session
fixture migrates, and migrate re-applies okg.grant_registry, so a privilege
revoked out of band before the run is HEALED before the first assertion --
verified: `REVOKE SELECT ON okg.subtype_ancestors FROM okg_mcp` reads `f`
immediately after the revoke and `t` after pytest, and the test passes. So this
module does not detect ad-hoc drift, and is not trying to (okg.grant_registry_
drift() and `okg doctor --check grant-registry-drift` own that). What it
detects is a relation the read pool needs and the REGISTRY does not list, which
no amount of re-applying can fix -- the F1 shape, and the only shape that
survives a migrate.
"""
from __future__ import annotations

import psycopg
import pytest

from okg.substrate.deployment_apply import ensure_deployment_apply_schema

pytest.importorskip("mcp")

pytestmark = pytest.mark.postgres


@pytest.fixture
def as_okg_mcp(clean_runtime: str):
    """A connection whose session role is okg_mcp for the whole test.

    ``SET SESSION ROLE`` rather than a separate login: okg_mcp is NOLOGIN by
    design on a managed host, and the privilege question is decided by the
    session role, not by how the session authenticated. RESET on the way out so
    the shared database is left as found.
    """
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        exists = conn.execute(
            "SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'okg_mcp')"
        ).fetchone()[0]
        assert exists, (
            "okg_mcp is absent -- every assertion in this module would pass "
            "vacuously against the owner role"
        )
        conn.execute("SET SESSION ROLE okg_mcp")
        current = conn.execute("SELECT current_user").fetchone()[0]
        assert current == "okg_mcp", (
            f"session role is {current!r}, not okg_mcp: this module would test "
            "the owner's privileges, which is exactly the gap it exists to close"
        )
        try:
            yield conn
        finally:
            conn.execute("RESET ROLE")


#: (label, SQL) — one read per operator family, naming the relations that
#: family reaches on the read pool. Each is the privilege-relevant core of the
#: query the operator issues, not the operator's full plan.
OPERATOR_READS = [
    ("inspect/expand: graph checkout", "SELECT 1 FROM okg.graph_nodes LIMIT 1"),
    ("expand: edges", "SELECT 1 FROM okg.graph_edges LIMIT 1"),
    ("search: alias resolution", "SELECT 1 FROM okg.aliases LIMIT 1"),
    (
        "search: identifier mentions",
        "SELECT 1 FROM okg.identifier_patterns LIMIT 1",
    ),
    ("map/filter: catalog", "SELECT 1 FROM okg.subtypes LIMIT 1"),
    (
        "search profiles: refinement closure",
        "SELECT 1 FROM okg.subtype_ancestors LIMIT 1",
    ),
    ("map: narrowings", "SELECT 1 FROM okg.narrowings LIMIT 1"),
    ("aggregate/filter: declared-key index readiness",
     "SELECT 1 FROM okg.declared_attr_indexes LIMIT 1"),
    (
        "aggregate: effective attributes",
        "SELECT 1 FROM okg.attribute_effective_attrs LIMIT 1",
    ),
    (
        "aggregate/filter: overlay generation marker",
        "SELECT 1 FROM okg.attribute_overlay_generations LIMIT 1",
    ),
    ("query: generation status", "SELECT 1 FROM okg.graph_generations LIMIT 1"),
    ("query: source status", "SELECT 1 FROM okg.source_progress LIMIT 1"),
    ("startup: migration contract", "SELECT 1 FROM okg.schema_migrations LIMIT 1"),
    # The startup check is TWO reads, not one. migration_status() calls
    # multiphase_in_progress_rows(), which selects okg.schema_migration_phases.
    # Probing only the row above said "startup reachable" while `okg mcp serve`
    # died on this table (Kubernetes lifecycle proof, run 33513581450).
    (
        "startup: multiphase migration progress",
        "SELECT 1 FROM okg.schema_migration_phases LIMIT 1",
    ),
    ("lineage: producer provenance", "SELECT 1 FROM okg.producer_profiles LIMIT 1"),
    (
        "nomos: read-time authorization",
        "SELECT 1 FROM okg.nomos_permission_grants LIMIT 1",
    ),
]


@pytest.mark.parametrize(
    ("label", "statement"), OPERATOR_READS, ids=[row[0] for row in OPERATOR_READS]
)
def test_operator_read_is_reachable_as_okg_mcp(
    as_okg_mcp: psycopg.Connection, label: str, statement: str
) -> None:
    try:
        as_okg_mcp.execute(statement).fetchall()
    except psycopg.errors.InsufficientPrivilege as exc:
        pytest.fail(
            f"{label}: the MCP read pool cannot reach this relation -- "
            f"{str(exc).splitlines()[0]}. Add the okg.grant_registry row in "
            "src/okg/substrate/db/views.sql's okg_grant_registry_seed block."
        )
    except psycopg.errors.UndefinedTable:
        pytest.skip(f"{label}: relation absent on this database")


def _assert_write_reachable(
    conn: psycopg.Connection, label: str, statement: str
) -> None:
    """Only InsufficientPrivilege is a failure.

    A NOT NULL / FK complaint means the statement got PAST the privilege system
    and was rejected on its contents, which is the answer this module wants: the
    grant is present. Asserting a successful insert instead would couple this
    privilege check to each table's column contract.
    """
    try:
        with conn.transaction() as tx:
            conn.execute(statement)
            tx.rollback = True  # never leave a probe row behind
    except psycopg.errors.InsufficientPrivilege as exc:
        pytest.fail(f"{label}: {str(exc).splitlines()[0]}")
    except psycopg.Error:
        # Rejected on contents, not on privilege. That is a pass here.
        pass


def test_audit_write_is_reachable_as_okg_mcp(
    as_okg_mcp: psycopg.Connection,
) -> None:
    """The read pool's one legitimate write: its own per-call audit row."""
    _assert_write_reachable(
        as_okg_mcp,
        "okg_mcp cannot append its own audit row",
        "INSERT INTO okg.mcp_call_log (deployment_name, operator_name) "
        "VALUES ('default', 'grant_surface_probe')",
    )


def test_pact_coordination_write_is_reachable_as_okg_mcp(
    as_okg_mcp: psycopg.Connection,
) -> None:
    """`okg pact` writes coordination events AS okg_mcp (#1064's handoff).

    This is the grant the schema-wide REVOKE used to strip with nothing to
    restore it, because the ledger's own repair is probe-skipped once the
    tables exist.
    """
    _assert_write_reachable(
        as_okg_mcp,
        "okg_mcp cannot write a coordination event",
        "INSERT INTO okg.pact_coordination_events (event_type, change_id) "
        "VALUES ('intent', 'grant-surface-probe')",
    )


#: Deliberately NOT okg.source_watermarks or okg.source_progress -- both are
#: registered reads (source status is an operator surface). The write side
#: proper is the fact log and the admission/apply ledgers.
#: Relations the write side creates on first use rather than in migrate; the
#: negative test creates them as the owner so it never has to skip.
LAZILY_CREATED_RELATIONS = {
    "okg.deployment_apply_runs": ensure_deployment_apply_schema,
}

DENIED_RELATIONS = (
    "okg.node_facts",
    "okg.edge_facts",
    "okg.admission_envelopes",
    "okg.deployment_apply_runs",
    "okg.generation_ordinals",
)


@pytest.mark.parametrize("relation", DENIED_RELATIONS)
def test_okg_mcp_still_cannot_read_the_write_side(
    as_okg_mcp: psycopg.Connection, clean_runtime: str, relation: str
) -> None:
    """The negative half. Without it this module would pass just as well
    against a blanket grant -- the state it exists to prevent."""
    present = as_okg_mcp.execute(
        "SELECT to_regclass(%s) IS NOT NULL", (relation,)
    ).fetchone()[0]
    if not present:
        # okg.deployment_apply_runs is created lazily by the first deployment
        # apply, not by migrate, so a migrated-only database lacks it. A skip
        # here proves nothing (the affected-test authority refuses it); create
        # the relation as the owner the way production does and keep asserting.
        creator = LAZILY_CREATED_RELATIONS.get(relation)
        if creator is None:
            pytest.fail(f"{relation} absent after migrate; the denied list is stale")
        with psycopg.connect(clean_runtime) as owner:
            creator(owner)
            owner.commit()
    with pytest.raises(psycopg.errors.InsufficientPrivilege):
        as_okg_mcp.execute(f"SELECT 1 FROM {relation} LIMIT 1").fetchall()
