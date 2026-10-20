"""Decision-log identity, dedup, batching, and retention (issue #635).

The decision identity hash must be content-of-access: volatile publish
coordinates (generation_id / branch_name / catalog_version_id) belong in the
audit-only target, never in the identity-bearing resource, so re-reading the
same rows after a new publish adds zero okg.nomos_decisions rows. The log is
bounded by an idempotent legacy-dedup migration plus age-based retention that
never deletes a row another table references.
"""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path
from time import perf_counter
from typing import Any

import psycopg
import pytest

from okg.substrate.mcp import inspect_node, open_session
from okg.substrate.nomos.core import NomosPolicyError
from okg.substrate.nomos.refinement import build_refinement_state_from_postgres
from okg.substrate.nomos.retention import _REFERENCING_TABLES, prune_nomos_decisions
from tests.substrate.mcp.test_nomos_source_lineage_mcp import (
    _activate_source_policy,
    _append_restricted_lineage_graph,
    _publish_lineage_graph,
)


pytestmark = pytest.mark.postgres

REPO_ROOT = Path(__file__).resolve().parents[3]

_DEDUP_TAG = "$nomos_decision_volatile_identity_dedup$"

_DEDUP_STATEMENT_BEGIN = (
    "-- BEGIN nomos-decision-volatile-identity-dedup-statement"
)

_DEDUP_STATEMENT_END = (
    "-- END nomos-decision-volatile-identity-dedup-statement"
)

_VOLATILE_KEYS = ("generation_id", "branch_name", "catalog_version_id")


def test_external_action_decision_references_are_retained() -> None:
    external = {
        (table, decision_column)
        for table, _deployment_column, decision_column in _REFERENCING_TABLES
        if table.startswith("external_action_")
    }
    assert external == {
        ("external_action_auth_audits", "nomos_decision_id"),
        ("external_action_decisions", "nomos_decision_id"),
        ("external_action_transition_idempotency", "nomos_decision_id"),
        ("external_action_executions", "nomos_decision_id"),
        ("external_action_outcomes", "nomos_decision_id"),
        ("external_action_events", "nomos_decision_ref"),
        ("external_action_owner_unavailability_attestations", "nomos_decision_id"),
        ("external_action_execution_epoch_leases", "nomos_decision_id"),
        ("external_action_restore_attestations", "nomos_decision_id"),
        ("external_action_restore_sessions", "nomos_decision_id"),
    }


def _read_decision_rows(conn: psycopg.Connection) -> list[dict[str, Any]]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT decision_id, resource, target
              FROM okg.nomos_decisions
             WHERE deployment_name = 'default'
               AND action = 'graph.read.mcp'
             ORDER BY decision_row_id
            """
        )
        return [
            {"decision_id": r[0], "resource": r[1], "target": r[2]}
            for r in cur.fetchall()
        ]


def _enable_enforcement(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "1")
    monkeypatch.setenv("OKG_NOMOS_SUBJECT", "subject:mcp-client")


def test_cross_generation_reads_dedup_to_one_decision(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Identical access across a publish adds zero decision rows, and the
    volatile publish coordinates live only in the audit target."""
    # Both generations publish BEFORE enforcement turns on (publishing under
    # the enforce env override blocks the publisher itself); the reads then
    # pin one session per generation.
    first_generation = _publish_lineage_graph(
        clean_runtime, include_restricted=False,
    )
    second_generation = _append_restricted_lineage_graph(clean_runtime)
    assert second_generation != first_generation
    _activate_source_policy(clean_runtime)
    _enable_enforcement(monkeypatch)

    with psycopg.connect(clean_runtime) as conn:
        session = open_session(
            conn,
            deployment_name="default",
            requested_generation=first_generation,
        )
        assert inspect_node(conn, session, "node:public-docs") is not None
        first_pass = _read_decision_rows(conn)
    assert first_pass, "the enforce-mode read must record decisions"

    # The same access re-read at the newer pin must dedup onto existing rows.
    with psycopg.connect(clean_runtime) as conn:
        session = open_session(conn, deployment_name="default")
        assert session.generation_id != first_generation
        assert inspect_node(conn, session, "node:public-docs") is not None
        second_pass = _read_decision_rows(conn)

    first_ids = {row["decision_id"] for row in first_pass}
    second_public = [
        row
        for row in second_pass
        if row["resource"].get("node_id") == "node:public-docs"
        or row["resource"].get("graph_ref") == "graph_node:node:public-docs"
    ]
    assert {row["decision_id"] for row in second_public} <= first_ids
    for row in second_pass:
        for key in _VOLATILE_KEYS:
            assert key not in row["resource"], (
                f"volatile key {key!r} leaked into decision identity: "
                f"{row['resource']}"
            )
        assert row["target"].get("generation_id") is not None
        assert row["target"].get("branch_name") is not None
        assert row["target"].get("catalog_version_id") is not None


def test_no_resource_builder_emits_volatile_keys(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """After exercising the row-read paths AND the boundary decision paths
    (MCP tool boundary, run_sql audit bypass, CLI search boundary), no
    persisted decision resource may carry a volatile publish coordinate."""
    _publish_lineage_graph(clean_runtime)
    _activate_source_policy(clean_runtime)
    _enable_enforcement(monkeypatch)

    # Row-read decisions (read filter + fine-grained targets).
    with psycopg.connect(clean_runtime) as conn:
        session = open_session(conn, deployment_name="default")
        inspect_node(conn, session, "node:public-docs")
        inspect_node(conn, session, "node:google-drive")

    # MCP tool-boundary decisions (mcp.tool.call + graph.read.mcp).
    from okg.substrate.mcp.server import build_server

    mcp = build_server(dsn=clean_runtime, deployment_name="default")
    try:
        mcp._tool_manager._tools["filter"].fn(
            input={"kind": "nodes"},
            predicate={},
            select=["node_id"],
            limit=5,
        )
    finally:
        mcp._okg_pool.close()
        if mcp._okg_write_pool:
            mcp._okg_write_pool.close()

    # run_sql graph-backed audit-bypass decision (audit mode records the
    # bypass and allows).
    from okg.substrate.mcp.run_sql import run_sql

    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "audit")
    from okg.substrate.nomos.runtime_enforcement import (
        _clear_nomos_enforcement_mode_cache,
    )

    _clear_nomos_enforcement_mode_cache()
    with psycopg.connect(clean_runtime) as conn:
        session = open_session(conn, deployment_name="default")
        run_sql(conn, session, "SELECT node_id FROM okg.v_nodes LIMIT 1")
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "1")
    _clear_nomos_enforcement_mode_cache()

    # CLI search boundary decision (graph.read.cli_search).
    from types import SimpleNamespace

    from okg.substrate.cli import _read_side
    from okg.substrate.cli.dsn_authority import ReadDsnResolution

    monkeypatch.setattr(
        _read_side,
        "resolve_read_dsn",
        lambda _args, *, surface: ReadDsnResolution(
            dsn=clean_runtime,
            deployment="default",
            authority={"mode": "test", "surface": surface},
            target=None,
        ),
    )
    _read_side.cmd_search(
        SimpleNamespace(
            deployment="default",
            dsn=None,
            generation_id=None,
            query="docs",
            method="lexical",
            subtype=None,
            attr_filter=None,
            select=None,
            limit=5,
            offset=0,
        ),
        as_json=True,
    )

    with psycopg.connect(clean_runtime) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT count(*), array_agg(DISTINCT enforcement_point)
                  FROM okg.nomos_decisions
                 WHERE resource ?| %s::text[]
                """,
                (list(_VOLATILE_KEYS),),
            )
            leaked, leaking_points = cur.fetchone()
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM okg.nomos_decisions")
            total = int(cur.fetchone()[0])
    assert total > 0, "the exercised paths must have recorded decisions"
    assert int(leaked) == 0, (
        f"volatile keys leaked into decision identity at {leaking_points}"
    )


def test_repeat_read_adds_zero_rows_and_bounded_statements(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A repeat read adds zero rows, and one filter pass issues one batched
    decision INSERT statement regardless of row count."""
    _publish_lineage_graph(clean_runtime, include_restricted=False)
    _activate_source_policy(clean_runtime)
    _enable_enforcement(monkeypatch)

    executed: list[str] = []

    class CountingCursor(psycopg.Cursor):
        def execute(self, query: Any, params: Any = None, **kwargs: Any):
            executed.append(str(query))
            return super().execute(query, params, **kwargs)

    def _decision_inserts() -> int:
        return sum(
            1
            for q in executed
            if "INSERT INTO okg.nomos_decisions" in q
        )

    def _decision_targets(conn: psycopg.Connection) -> dict[str, Any]:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT decision_id, target FROM okg.nomos_decisions "
                "WHERE deployment_name = 'default' ORDER BY decision_id"
            )
            return {r[0]: r[1] for r in cur.fetchall()}

    with psycopg.connect(clean_runtime, cursor_factory=CountingCursor) as conn:
        session = open_session(conn, deployment_name="default")
        # One pass over multiple rows: search-free direct filter via the row
        # read (node + its fine-grained targets all buffer into one flush).
        assert inspect_node(conn, session, "node:public-docs") is not None
        first_pass_inserts = _decision_inserts()
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM okg.nomos_decisions")
            rows_after_first = int(cur.fetchone()[0])
        targets_after_first = _decision_targets(conn)

        executed.clear()
        assert inspect_node(conn, session, "node:public-docs") is not None
        second_pass_inserts = _decision_inserts()
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM okg.nomos_decisions")
            rows_after_second = int(cur.fetchone()[0])
        targets_after_second = _decision_targets(conn)

    assert first_pass_inserts == 1, (
        "one filter pass must flush decisions as one batched statement; "
        f"saw {first_pass_inserts}"
    )
    assert second_pass_inserts <= 1
    assert rows_after_second == rows_after_first, (
        "a repeat read must not add decision rows"
    )
    # First-observed target preserved: DO NOTHING must not rewrite any
    # existing row's target on the repeat read.
    assert targets_after_first, "the first read must have recorded decisions"
    assert targets_after_second == targets_after_first


def test_filter_pass_batches_decision_persists(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """One filter pass over N multi-source rows issues ONE batched decision
    INSERT statement (statements independent of N), and all distinct
    decisions land."""
    from okg.substrate.graph_store import ChronosGraphStore
    from okg.substrate.mcp import open_session as _open_session
    from okg.substrate.nomos.read_filter import filter_node_rows_for_session

    _publish_lineage_graph(clean_runtime)  # 4 page nodes incl. multi-source
    _activate_source_policy(clean_runtime)
    _enable_enforcement(monkeypatch)

    executed: list[str] = []

    class CountingCursor(psycopg.Cursor):
        def execute(self, query: Any, params: Any = None, **kwargs: Any):
            executed.append(str(query))
            return super().execute(query, params, **kwargs)

    with psycopg.connect(clean_runtime, cursor_factory=CountingCursor) as conn:
        session = _open_session(conn, deployment_name="default")
        store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
        graph = store.checkout_generation(
            session.generation_id, branch=session.branch_name,
        )
        rows = graph.query(
            "SELECT node_id, subtype, attrs, source, source_record_id, "
            "source_revision FROM okg.graph_nodes ORDER BY node_id",
        )
        assert len(rows) >= 4, "fixture must provide a multi-row pass"
        executed.clear()
        visible = filter_node_rows_for_session(conn, session, rows)
        inserts = [
            q for q in executed if "INSERT INTO okg.nomos_decisions" in q
        ]
        with conn.cursor() as cur:
            cur.execute(
                "SELECT count(*) FROM okg.nomos_decisions "
                "WHERE action = 'graph.read.mcp'"
            )
            recorded = int(cur.fetchone()[0])

    assert len(inserts) == 1, (
        f"a {len(rows)}-row multi-source pass must batch decision persists "
        f"into one statement; saw {len(inserts)}"
    )
    assert recorded > len(rows), (
        "the pass must record per-(row x source x fine-grained) decisions"
    )
    assert [row["node_id"] for row in visible] == [
        "node:public-docs",
        "node:public-more",
    ]


def _seed_legacy_duplicates(
    conn: psycopg.Connection,
    *,
    subject: str = "subject:legacy",
    generations: int = 3,
) -> list[str]:
    """Insert legacy-format rows: same access recorded once per generation."""
    decision_ids = []
    with conn.cursor() as cur:
        for generation in range(1, generations + 1):
            decision_id = f"nomos:decision:legacy-{subject}-{generation}"
            decision_ids.append(decision_id)
            cur.execute(
                """
                INSERT INTO okg.nomos_decisions (
                  deployment_name, decision_id, input_hash, subject, action,
                  resource, classification, context, effect, reason_code,
                  matched_rule_ids, obligations, enforcement_point, target,
                  created_at
                )
                VALUES (
                  'default', %(decision_id)s, %(input_hash)s, %(subject)s,
                  'graph.read.mcp',
                  %(resource)s::jsonb, 'internal', '{}'::jsonb, 'allow',
                  'allow_public_docs_mcp_read', ARRAY['allow_public_docs'],
                  '[]'::jsonb, 'mcp.source_lineage_filter', '{}'::jsonb,
                  now() - make_interval(days => %(age_days)s)
                )
                """,
                {
                    "decision_id": decision_id,
                    "input_hash": f"sha256:legacy-{subject}-{generation}",
                    "subject": subject,
                    "resource": (
                        '{"deployment": "default", "scope": '
                        '"deployment:default", "kind": "node", '
                        '"node_id": "node:public-docs", '
                        f'"generation_id": {generation}, '
                        '"branch_name": "default", '
                        '"catalog_version_id": 1}'
                    ),
                    "age_days": generations - generation,
                },
            )
    conn.commit()
    return decision_ids


def _dedup_migration_sql() -> str:
    schema = (
        REPO_ROOT / "src" / "okg" / "substrate" / "db" / "schema.sql"
    ).read_text(encoding="utf-8")
    parts = schema.split(_DEDUP_TAG)
    assert len(parts) == 3, (
        "expected exactly one dollar-quoted dedup migration block in "
        "schema.sql"
    )
    return f"DO {_DEDUP_TAG}{parts[1]}{_DEDUP_TAG}"


def _dedup_delete_sql() -> str:
    migration = _dedup_migration_sql()
    parts = migration.split(_DEDUP_STATEMENT_BEGIN)
    assert len(parts) == 2, "dedup statement start marker must be unique"
    statement_parts = parts[1].split(_DEDUP_STATEMENT_END)
    assert len(statement_parts) == 2, "dedup statement end marker must be unique"
    return statement_parts[0].strip().removesuffix(";")


def _walk_plan(node: dict[str, Any]) -> Iterator[dict[str, Any]]:
    yield node
    for child in node.get("Plans", []):
        yield from _walk_plan(child)


def _seed_legacy_scale_fixture(
    conn: psycopg.Connection,
    *,
    fixture_name: str = "scale",
) -> None:
    """Insert 20k duplicate rows plus 1k legacy singletons set-wise."""
    with conn.cursor() as cur:
        cur.execute(
            """
            WITH duplicate_rows AS (
              SELECT identity_no, generation_no
                FROM generate_series(1, 5000) AS identity_no
                CROSS JOIN generate_series(1, 4) AS generation_no
            ),
            singleton_rows AS (
              SELECT identity_no, 1 AS generation_no
                FROM generate_series(5001, 6000) AS identity_no
            ),
            fixture_rows AS (
              SELECT * FROM duplicate_rows
              UNION ALL
              SELECT * FROM singleton_rows
            )
            INSERT INTO okg.nomos_decisions (
              deployment_name, decision_id, input_hash, subject, action,
              resource, classification, context, effect, reason_code,
              matched_rule_ids, obligations, enforcement_point, target,
              created_at
            )
            SELECT
              'default',
              format(
                'nomos:decision:%%s:%%s:%%s',
                %(fixture_name)s::text, identity_no, generation_no
              ),
              format(
                'sha256:%%s:%%s:%%s',
                %(fixture_name)s::text, identity_no, generation_no
              ),
              'subject:' || %(fixture_name)s::text,
              'graph.read.mcp',
              jsonb_build_object(
                'deployment', 'default',
                'kind', 'node',
                'node_id', format(
                  'node:%%s:%%s', %(fixture_name)s::text, identity_no
                ),
                'generation_id', generation_no,
                'branch_name', 'default',
                'catalog_version_id', generation_no
              ),
              'internal',
                jsonb_build_object('tenant', identity_no %% 10),
              'allow',
              'allow_scale_fixture',
              ARRAY['allow_scale_fixture'],
              '[]'::jsonb,
              'mcp.source_lineage_filter',
              '{}'::jsonb,
              TIMESTAMPTZ '2026-01-01 00:00:00+00'
                + generation_no * INTERVAL '1 second'
            FROM fixture_rows
            """,
            {"fixture_name": fixture_name},
        )
        assert cur.rowcount == 21_000
    conn.commit()


def test_legacy_dedup_migration_is_idempotent_and_reference_safe(
    clean_runtime: str,
) -> None:
    """The migration keeps one newest row per identity-without-volatile-keys,
    preserves rows referenced through all seven guarded surfaces, and deletes
    nothing on a second run."""
    migration = _dedup_migration_sql()
    # A real policy activation must exist so the non-FK
    # nomos_policy_activations.activation_decision_id reference is exercised
    # non-vacuously.
    _publish_lineage_graph(clean_runtime, include_restricted=False)
    _activate_source_policy(clean_runtime)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        ids = _seed_legacy_duplicates(conn, generations=9)
        protected_singletons = {
            "nomos:decision:legacy-context-variant",
            "nomos:decision:legacy-resource-variant",
            "nomos:decision:new-scheme-match",
        }
        with conn.cursor() as cur:
            # Force the two newest duplicate rows to the same timestamp so
            # decision_row_id is the exercised and observable tie-break.
            cur.execute(
                """
                UPDATE okg.nomos_decisions
                   SET created_at = (
                     SELECT created_at
                       FROM okg.nomos_decisions
                      WHERE decision_id = %s
                   )
                 WHERE decision_id = %s
                """,
                (ids[-1], ids[-2]),
            )
            # Exact identity separation: context and stable-resource variants
            # are legacy singletons, while a matching new-scheme row has no
            # volatile keys and must remain outside legacy cleanup entirely.
            cur.execute(
                """
                INSERT INTO okg.nomos_decisions (
                  deployment_name, decision_id, policy_bundle_id,
                  policy_version, policy_fingerprint, input_hash, subject,
                  action, resource, classification, context, effect,
                  reason_code, matched_rule_ids, obligations,
                  enforcement_point, target, created_at
                )
                SELECT
                  deployment_name,
                  variant.decision_id,
                  policy_bundle_id,
                  policy_version,
                  policy_fingerprint,
                  'sha256:' || variant.decision_id,
                  subject,
                  action,
                  CASE variant.kind
                    WHEN 'resource' THEN jsonb_set(
                      resource, '{node_id}', '"node:other"'::jsonb
                    )
                    WHEN 'new_scheme' THEN
                      resource - 'generation_id' - 'branch_name'
                               - 'catalog_version_id'
                    ELSE resource
                  END,
                  classification,
                  CASE variant.kind
                    WHEN 'context' THEN
                      context || '{"variant":"context"}'::jsonb
                    ELSE context
                  END,
                  effect,
                  reason_code,
                  matched_rule_ids,
                  obligations,
                  enforcement_point,
                  target,
                  created_at
                FROM okg.nomos_decisions source
                CROSS JOIN (
                  VALUES
                    ('nomos:decision:legacy-context-variant', 'context'),
                    ('nomos:decision:legacy-resource-variant', 'resource'),
                    ('nomos:decision:new-scheme-match', 'new_scheme')
                ) AS variant(decision_id, kind)
                WHERE source.decision_id = %s
                """,
                (ids[-1],),
            )
            # Protect seven older duplicates, one through every reference
            # surface guarded by the migration. The eighth row remains
            # deliberately unreferenced so the same run also proves deletion.
            cur.execute(
                """
                INSERT INTO okg.federation_connection_requests (
                  request_id, local_authority_id, requester_authority_id,
                  target_authority_id, requested_action, target_address,
                  requested_visibility, key_fingerprint, status,
                  nomos_decision_id
                )
                VALUES (
                  'request-dedup-guard', 'okg:local', 'okg:requester',
                  'okg:target', 'federation.message.send',
                  'okg://target/boards/public', 'public',
                  'fingerprint-dedup-guard', 'approved', %s
                )
                """,
                (ids[0],),
            )
            cur.execute(
                """
                INSERT INTO okg.federation_peer_trust_pins (
                  local_authority_id, remote_authority_id, key_fingerprint,
                  allowed_actions, status, nomos_decision_id
                )
                VALUES (
                  'okg:local', 'okg:remote', 'fingerprint-dedup-guard',
                  ARRAY['federation.message.send']::TEXT[], 'active', %s
                )
                """,
                (ids[1],),
            )
            cur.execute(
                """
                INSERT INTO okg.federation_route_events (
                  route_event_id, local_authority_id, remote_authority_id,
                  subject, action, target_address, visibility, decision,
                  reason_code, nomos_decision_id
                )
                VALUES (
                  'route-dedup-guard', 'okg:local', 'okg:remote',
                  'okg:subject', 'federation.message.send',
                  'okg://target/boards/public', 'public', 'allow',
                  'dedup.guard.allow', %s
                )
                """,
                (ids[2],),
            )
            cur.execute(
                """
                INSERT INTO okg.poros_incorporation_events (
                  receiver_authority_id, sender_authority_id, message_id,
                  event_type, status, reason_code, nomos_decision_id
                )
                VALUES (
                  'okg:receiver', 'okg:sender', 'message-dedup-guard',
                  'nomos_decided', 'allow', 'dedup.guard.allow', %s
                )
                """,
                (ids[3],),
            )
            cur.execute(
                """
                INSERT INTO okg.nomos_external_claims (
                  deployment_name, provider_id, provider_type, issuer,
                  external_subject, claim_fingerprint, verification_status,
                  nomos_decision_id
                )
                VALUES (
                  'default', 'test-provider', 'oidc', 'https://issuer.test',
                  'user@test', 'fp:test', 'verified', %s
                )
                """,
                (ids[4],),
            )
            # The policy activation is an existing non-FK reference surface.
            cur.execute(
                """
                UPDATE okg.nomos_policy_activations
                   SET activation_decision_id = %s
                 WHERE deployment_name = 'default'
                """,
                (ids[5],),
            )
            assert cur.rowcount > 0, (
                "fixture must hold a policy activation to make the non-FK "
                "reference check non-vacuous"
            )
            cur.execute(
                """
                INSERT INTO okg.nomos_config_sets (
                  deployment_name, config_set_id, config_version,
                  config_fingerprint, compiler_version, config_set
                )
                VALUES (
                  'default', 'nomos:test:dedup-guard', 'v1',
                  'sha256:dedup-guard-config', 'test', '{}'::jsonb
                )
                """
            )
            cur.execute(
                """
                INSERT INTO okg.nomos_config_activations (
                  deployment_name, activation_id, scope,
                  config_fingerprint, status, activated_by,
                  activation_decision_id
                )
                VALUES (
                  'default', 'nomos:config_activation:dedup-guard',
                  'deployment:dedup-guard', 'sha256:dedup-guard-config',
                  'active', 'test:operator', %s
                )
                """,
                (ids[6],),
            )
            cur.execute(
                """
                SELECT decision_id, resource, context, created_at
                  FROM okg.nomos_decisions
                 WHERE decision_id = %s
                """,
                (ids[-1],),
            )
            newest_before = cur.fetchone()

        with conn.cursor() as cur:
            cur.execute(migration)
            cur.execute(
                """
                SELECT decision_id FROM okg.nomos_decisions
                 WHERE decision_id = ANY(%s)
                 ORDER BY decision_id
                """,
                (ids,),
            )
            surviving = [r[0] for r in cur.fetchall()]

        # Newest survives as the representative; all seven referenced rows
        # survive regardless of age; the unreferenced penultimate duplicate
        # is deleted.
        assert set(surviving) == {*ids[:7], ids[-1]}
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT decision_id, resource, context, created_at
                  FROM okg.nomos_decisions
                 WHERE decision_id = %s
                """,
                (ids[-1],),
            )
            assert cur.fetchone() == newest_before
            cur.execute(
                """
                SELECT decision_id
                  FROM okg.nomos_decisions
                 WHERE decision_id = ANY(%s)
                """,
                (list(protected_singletons),),
            )
            assert {row[0] for row in cur.fetchall()} == protected_singletons
            cur.execute(
                """
                SELECT resource ?| %s::text[]
                  FROM okg.nomos_decisions
                 WHERE decision_id = 'nomos:decision:new-scheme-match'
                """,
                (list(_VOLATILE_KEYS),),
            )
            assert cur.fetchone()[0] is False

        # Idempotence: a second run deletes zero rows.
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM okg.nomos_decisions")
            before = int(cur.fetchone()[0])
            cur.execute(migration)
            cur.execute("SELECT count(*) FROM okg.nomos_decisions")
            after = int(cur.fetchone()[0])
        assert after == before


def test_legacy_dedup_migration_has_bounded_scan_amplification(
    clean_runtime: str,
) -> None:
    """The real cleanup remains set-based at a CI-representative scale.

    Wall time alone is runner-dependent, so this test couples a loose timeout
    with the deterministic plan property that caught the production failure:
    no non-PK scan of nomos_decisions may execute once per candidate row.
    """
    migration = _dedup_migration_sql()
    statement = _dedup_delete_sql()
    reference_tables = {
        "federation_connection_requests",
        "federation_peer_trust_pins",
        "federation_route_events",
        "poros_incorporation_events",
        "nomos_external_claims",
        "nomos_policy_activations",
        "nomos_config_activations",
    }
    for table in reference_tables:
        assert f"FROM okg.{table}" in statement
    assert "IF NOT EXISTS" not in migration

    with psycopg.connect(clean_runtime) as conn:
        _seed_legacy_scale_fixture(conn, fixture_name="migration-scale")
        with conn.cursor() as cur:
            cur.execute("SET LOCAL statement_timeout = '15s'")
            started = perf_counter()
            cur.execute(migration)
            migration_elapsed_seconds = perf_counter() - started
            cur.execute("SELECT count(*) FROM okg.nomos_decisions")
            migration_survivor_count = int(cur.fetchone()[0])

        # Add a disjoint scale fixture, then capture the marked set-based
        # statement's execution plan. Timing the complete DO block first
        # ensures an expensive probe outside the marker cannot evade CI.
        _seed_legacy_scale_fixture(conn, fixture_name="plan-scale")
        with conn.cursor() as cur:
            cur.execute("SET LOCAL statement_timeout = '15s'")
            started = perf_counter()
            cur.execute(
                "EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) " + statement
            )
            elapsed_seconds = perf_counter() - started
            plan_document = cur.fetchone()[0][0]
            plan_nodes = list(_walk_plan(plan_document["Plan"]))

            cur.execute("SELECT count(*) FROM okg.nomos_decisions")
            survivor_count = int(cur.fetchone()[0])

    assert migration_survivor_count == 6_000
    assert survivor_count == 12_000
    assert any(node.get("Node Type") == "WindowAgg" for node in plan_nodes)

    repeated_wide_scans = [
        {
            "node_type": node.get("Node Type"),
            "index_name": node.get("Index Name"),
            "actual_loops": node.get("Actual Loops"),
            "actual_rows": node.get("Actual Rows"),
        }
        for node in plan_nodes
        if node.get("Relation Name") == "nomos_decisions"
        and int(node.get("Actual Loops", 0)) > 1
        and not (
            node.get("Index Name") == "nomos_decisions_pkey"
            and float(node.get("Actual Rows", 0)) <= 1
        )
    ]
    assert repeated_wide_scans == [], (
        "dedup plan reintroduced a per-candidate decision-table scan: "
        f"{repeated_wide_scans}"
    )
    assert elapsed_seconds < 15, (
        f"21k-row dedup exceeded its CI statement budget: {elapsed_seconds:.3f}s"
    )
    assert migration_elapsed_seconds < 15, (
        "complete 21k-row migration exceeded its CI statement budget: "
        f"{migration_elapsed_seconds:.3f}s"
    )


def test_retention_prunes_beyond_bound_and_respects_references(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Retention deletes aged unreferenced rows, keeps referenced ones,
    fails loudly on misconfiguration, and a pruned-but-recurring decision is
    re-inserted on its next occurrence (self-healing)."""
    _publish_lineage_graph(clean_runtime, include_restricted=False)
    _activate_source_policy(clean_runtime)
    _enable_enforcement(monkeypatch)

    with psycopg.connect(clean_runtime) as conn:
        session = open_session(conn, deployment_name="default")
        assert inspect_node(conn, session, "node:public-docs") is not None
        read_rows = _read_decision_rows(conn)
        assert read_rows

        # Age every read decision past the bound, and reference one aged row
        # from a non-FK activation reference: it must survive the prune.
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE okg.nomos_decisions
                   SET created_at = now() - make_interval(days => 400)
                 WHERE deployment_name = 'default'
                """
            )
            cur.execute(
                """
                SELECT activation_decision_id
                  FROM okg.nomos_policy_activations
                 WHERE deployment_name = 'default'
                 LIMIT 1
                """
            )
            protected_id = cur.fetchone()[0]
        conn.commit()

        monkeypatch.setenv("OKG_NOMOS_DECISION_RETENTION_DAYS", "not-a-number")
        with pytest.raises(NomosPolicyError):
            prune_nomos_decisions(conn, deployment_name="default")
        monkeypatch.setenv("OKG_NOMOS_DECISION_RETENTION_DAYS", "0")
        with pytest.raises(NomosPolicyError):
            prune_nomos_decisions(conn, deployment_name="default")
        monkeypatch.delenv("OKG_NOMOS_DECISION_RETENTION_DAYS")

        dry = prune_nomos_decisions(
            conn, deployment_name="default", dry_run=True,
        )
        assert dry > 0
        deleted = prune_nomos_decisions(conn, deployment_name="default")
        conn.commit()
        assert deleted == dry

        with conn.cursor() as cur:
            cur.execute(
                "SELECT count(*) FROM okg.nomos_decisions "
                "WHERE decision_id = %s",
                (protected_id,),
            )
            assert int(cur.fetchone()[0]) == 1, (
                "activation-referenced decision must survive retention"
            )
            cur.execute(
                "SELECT count(*) FROM okg.nomos_decisions "
                "WHERE action = 'graph.read.mcp'"
            )
            assert int(cur.fetchone()[0]) == 0

        # Self-healing: the same read re-records its decisions with a fresh
        # created_at (no unrecorded-decision window).
        session = open_session(conn, deployment_name="default")
        assert inspect_node(conn, session, "node:public-docs") is not None
        assert _read_decision_rows(conn)


def test_refinement_state_survives_dedup_and_prune(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The TLA+ refinement surface still builds after dedup + prune, and its
    activation-decision references stay resolvable in the retained scope."""
    _publish_lineage_graph(clean_runtime, include_restricted=False)
    _activate_source_policy(clean_runtime)
    _enable_enforcement(monkeypatch)

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        session = open_session(conn, deployment_name="default")
        assert inspect_node(conn, session, "node:public-docs") is not None
        _seed_legacy_duplicates(conn, generations=3)

        before = build_refinement_state_from_postgres(
            conn, deployment_name="default",
        )
        with conn.cursor() as cur:
            cur.execute(_dedup_migration_sql())
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE okg.nomos_decisions "
                "SET created_at = now() - make_interval(days => 400) "
                "WHERE action = 'graph.read.mcp'"
            )
        prune_nomos_decisions(conn, deployment_name="default")
        after = build_refinement_state_from_postgres(
            conn, deployment_name="default",
        )

    assert after["postgresPolicyState"] == before["postgresPolicyState"]
    decision_ids = {
        str(row.get("decision_id"))
        for row in after.get("decisions", [])
        if isinstance(row, dict)
    }
    for scope_state in after["postgresPolicyState"].values():
        for key in ("policyActivationDecision", "configActivationDecision"):
            if scope_state.get(key):
                assert scope_state[key] in decision_ids, (
                    "activation decision reference must survive dedup+prune"
                )
