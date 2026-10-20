"""okg#1114: the upgrade preflight must survive a PRE-0020 database.

``okg up`` runs ``preflight`` five phases before ``migrate``.  Preflight's
effective-attribute recovery classification fingerprints the snapshot and the
interval front over ``attrs_hash`` -- a column migration
``0020_interval_narrow_split`` CREATES -- so on any database older than 0020 the
preflight query died with ``UndefinedColumn`` and the check guarding the upgrade
required the upgrade to have already happened.  There is no ``--skip-preflight``
and no forward ``okg db migrate``, so the deployment could not be rescued.

The fix is NOT "fingerprint whichever columns happen to exist".  Two
fingerprints taken over DIFFERENT column sets compare equal and report a parity
match that was never performed -- a false witness in an integrity check.
``test_naive_drop_missing_columns_reports_a_false_parity_match`` executes that
naive implementation against a REAL payload divergence and shows it reporting a
match, which is why the shipped check refuses to answer instead.

The pre-0020 shape is reconstructed here by reversing 0020 on a live published
deployment (wide jsonb columns back, ``attrs_hash`` gone), then restored through
the checkpointed multi-phase driver -- the same forward path ``okg up`` runs.
``okg.attribute_versions`` is deliberately LEFT in place: a real pre-0020
database lacks it, but no query on the parity path reads it, and dropping it
would only exercise this module's table-existence cache.
"""
from __future__ import annotations

import contextlib
import uuid
from typing import Iterator

import pytest

psycopg = pytest.importorskip("psycopg")
from psycopg.types.json import Jsonb  # noqa: E402

from okg.substrate.attributes import (  # noqa: E402
    ATTRIBUTE_EFFECTIVE_INTERVALS_TABLE,
    ATTRIBUTE_EFFECTIVE_VIEW_TABLE,
    _INTERVAL_NARROW_SPLIT_MIGRATION,
    _INTERVAL_PAYLOAD_COLS,
    _effective_interval_payload_parity,
    _effective_interval_payload_parity_applicability,
    _relation_payload_columns,
    classify_effective_attribute_recovery,
    ensure_attribute_schema,
)
from okg.substrate.db.management import (  # noqa: E402
    current_known_migrations,
)
from okg.substrate.db.interval_narrow_split_migration import (  # noqa: E402
    build_spec,
)
from okg.substrate.db.multiphase import run_multiphase_migration  # noqa: E402
from okg.substrate.errors import SubstrateError  # noqa: E402
from okg.substrate.graph_store import ChronosGraphStore  # noqa: E402
from okg.substrate.graph_store_internal.generation_ordinal import (  # noqa: E402
    resolve_generation_ordinal_row,
)
from okg.substrate.library.sources.base import NodeFact  # noqa: E402
from okg.substrate.publisher import Publisher  # noqa: E402
from tests.pytest.catalog_fixture_builders import seed_catalog  # noqa: E402
from tests.substrate.test_deployment_upgrade_pre_narrow_split import (  # noqa: E402
    GRAPH_ANCHOR_FIELDS,
    MIGRATION_PENDING_CLASSIFICATION,
)
from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer  # noqa: E402

# ``serial_db`` is not optional here, and it is not about speed.  Every arm in
# this module reshapes the SESSION-SCOPED runtime database: ``DROP COLUMN
# attrs_hash`` and ``ALTER TABLE ... RENAME`` against the schema
# ``tests/substrate/conftest.py``'s ``okg_migrated`` fixture migrates ONCE per
# worker.  ``clean_runtime`` truncates rows between tests; it does not re-run
# migrations.  So an aborted teardown -- a killed process, a failure inside the
# revert block -- leaves every later test in that worker on a pre-0020 schema,
# and they fail for a reason that has nothing to do with them.  The parallel DB
# lanes select ``postgres and ... and not serial_db`` over a two-worker pool
# (tests/qa_lanes.yaml), so this marker is the mechanism that keeps these arms
# off a shared worker instead of a promise that they behave.
pytestmark = [pytest.mark.postgres, pytest.mark.serial_db]

_DEPLOYMENT = "default"

# The wide shape 0020 replaced: jsonb payloads inline on both relations, no
# ``attrs_hash``, and a GIN index over ``attrs`` on each.  Column ORDER differs
# from a real pre-0020 database (ALTER TABLE appends), which nothing here reads.
_REVERT_TO_WIDE_SHAPE = (
    """
    ALTER TABLE okg.attribute_effective_view
      ADD COLUMN IF NOT EXISTS raw_attrs JSONB,
      ADD COLUMN IF NOT EXISTS attrs JSONB
    """,
    """
    UPDATE okg.attribute_effective_view AS s
       SET attrs = v.attrs, raw_attrs = v.raw_attrs
      FROM okg.attribute_versions AS v
     WHERE v.target_kind = s.target_kind
       AND v.target_id = s.target_id
       AND v.attrs_hash = s.attrs_hash
    """,
    """
    ALTER TABLE okg.attribute_effective_view
      ALTER COLUMN attrs SET DEFAULT '{}'::jsonb,
      ALTER COLUMN attrs SET NOT NULL,
      DROP COLUMN attrs_hash
    """,
    """
    CREATE INDEX IF NOT EXISTS attribute_effective_view_attrs_gin_idx
      ON okg.attribute_effective_view USING GIN (attrs jsonb_path_ops)
    """,
    """
    ALTER TABLE okg.attribute_effective_view_intervals
      ADD COLUMN IF NOT EXISTS raw_attrs JSONB,
      ADD COLUMN IF NOT EXISTS attrs JSONB
    """,
    """
    UPDATE okg.attribute_effective_view_intervals AS i
       SET attrs = v.attrs, raw_attrs = v.raw_attrs
      FROM okg.attribute_versions AS v
     WHERE v.target_kind = i.target_kind
       AND v.target_id = i.target_id
       AND v.attrs_hash = i.attrs_hash
    """,
    """
    ALTER TABLE okg.attribute_effective_view_intervals
      ALTER COLUMN attrs SET DEFAULT '{}'::jsonb,
      ALTER COLUMN attrs SET NOT NULL,
      DROP COLUMN attrs_hash
    """,
    """
    CREATE INDEX IF NOT EXISTS attribute_effective_intervals_attrs_gin_idx
      ON okg.attribute_effective_view_intervals USING GIN (attrs jsonb_path_ops)
    """,
)


def _facts(suffix: str, ids, attrs: dict, *, rev: int = 1) -> list[NodeFact]:
    return [
        NodeFact(
            node_id=f"doc:{i}:{suffix}",
            subtype="doc",
            attrs=attrs,
            source_record_id={"id": f"doc:{i}:{suffix}"},
            source_revision={"v": rev},
        )
        for i in ids
    ]


def _publish_one_generation(dsn: str) -> str:
    """Seed a catalog, apply three node facts, publish, return the generation."""
    suffix = uuid.uuid4().hex[:8]
    with psycopg.connect(dsn, autocommit=True) as conn:
        seed_catalog(conn, subtypes=["doc"])
        ChronosGraphStore.from_connection(conn).apply_facts(
            _facts(suffix, range(3), {"k": "v"}),
            source="default",
            strict_endpoints=True,
        )
    published = Publisher(dsn, _DEPLOYMENT, catalog_identity=bind_test_catalog_writer(dsn, _DEPLOYMENT)).cycle()
    assert published["status"] == "published", published
    return str(published["generation_id"])


def _generation_anchor(conn: psycopg.Connection, generation: str) -> tuple[str, int]:
    store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
    return resolve_generation_ordinal_row(store.context.db, generation)


@contextlib.contextmanager
def _pre_narrow_split_schema(
    conn: psycopg.Connection, *, dsn: str
) -> Iterator[None]:
    """Put both effective-attribute relations back on the pre-0020 wide shape.

    Restores through the checkpointed multi-phase driver -- exactly what the
    lifecycle's migrate phase does -- so the session-scoped test database is
    handed back at the shape every other test expects, even on failure.
    """
    with conn.cursor() as cur:
        for statement in _REVERT_TO_WIDE_SHAPE:
            cur.execute(statement)
    try:
        yield
    finally:
        with conn.transaction(), conn.cursor() as cur:
            cur.execute(
                "DELETE FROM okg.schema_migrations WHERE migration_id = %s",
                (_INTERVAL_NARROW_SPLIT_MIGRATION,),
            )
            cur.execute(
                "DELETE FROM okg.schema_migration_phases "
                "WHERE migration_id = %s",
                (_INTERVAL_NARROW_SPLIT_MIGRATION,),
            )
        migration = next(
            migration
            for migration in current_known_migrations()
            if migration.migration_id == _INTERVAL_NARROW_SPLIT_MIGRATION
        )
        outcome = run_multiphase_migration(
            dsn,
            migration,
            build_spec(),
        )
        assert outcome["status"] == "applied", outcome
        # Replay the runtime ensure too: it re-grants the version store to
        # okg_mcp and reasserts the FKs, so the shared database is left exactly
        # as a migrated one, not merely column-compatible.
        ensure_attribute_schema(conn)
        for table in (
            ATTRIBUTE_EFFECTIVE_VIEW_TABLE,
            ATTRIBUTE_EFFECTIVE_INTERVALS_TABLE,
        ):
            assert "attrs_hash" in _relation_payload_columns(conn, table), table


def _naive_payload_signatures(
    conn: psycopg.Connection,
    *,
    generation: str,
    namespace: str,
    branch_id: str,
    ordinal: int,
) -> tuple[tuple, tuple]:
    """The REJECTED fix: fingerprint whichever payload columns still exist.

    A faithful transcription of ``_effective_interval_payload_signatures``'
    SQL with one change -- the column list is intersected with the relation's
    live columns instead of being taken as given.  It is here to be shown
    wrong, not to be used.
    """

    def fingerprint(alias: str, table: str, seed: int) -> str:
        live = _relation_payload_columns(conn, table)
        columns = (
            "target_kind",
            "target_id",
            "namespace",
            *(column for column in _INTERVAL_PAYLOAD_COLS if column in live),
        )
        payload = " || E'\\x1f' || ".join(
            f"COALESCE({alias}.{column}::text, E'\\x1e')" for column in columns
        )
        return f"COALESCE(sum(hashtextextended({payload}, {seed})::numeric), 0)"

    with conn.cursor() as cur:
        cur.execute(
            f"SELECT count(*)::bigint, "
            f"{fingerprint('s', ATTRIBUTE_EFFECTIVE_VIEW_TABLE, 0)}, "
            f"{fingerprint('s', ATTRIBUTE_EFFECTIVE_VIEW_TABLE, 1)} "
            f"FROM {ATTRIBUTE_EFFECTIVE_VIEW_TABLE} AS s "
            "WHERE s.generation_id = %s AND s.namespace = %s",
            (generation, namespace),
        )
        snapshot = tuple(cur.fetchone())
        cur.execute(
            f"SELECT count(*)::bigint, "
            f"{fingerprint('i', ATTRIBUTE_EFFECTIVE_INTERVALS_TABLE, 0)}, "
            f"{fingerprint('i', ATTRIBUTE_EFFECTIVE_INTERVALS_TABLE, 1)} "
            f"FROM {ATTRIBUTE_EFFECTIVE_INTERVALS_TABLE} AS i "
            "WHERE i.branch_id = %s AND i.namespace = %s "
            "AND i.from_ord <= %s AND %s < i.to_ord",
            (branch_id, namespace, ordinal, ordinal),
        )
        interval = tuple(cur.fetchone())
    return snapshot, interval


def test_preflight_classification_survives_a_pre_narrow_split_schema(
    clean_runtime: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The reproduction of okg#1114, and the whole round trip out of it.

    Before the fix this raised ``psycopg.errors.UndefinedColumn: column
    s.attrs_hash does not exist`` -- the crash that made ``okg up`` impossible
    on every pre-0020 deployment.
    """
    monkeypatch.setenv("OKG_IVM_INTERVAL_VIEW", "1")
    dsn = clean_runtime
    _publish_one_generation(dsn)

    with psycopg.connect(dsn, autocommit=True) as conn:
        # Control: on the migrated shape the parity check is applicable and the
        # classification carries a real boolean verdict.
        before = classify_effective_attribute_recovery(
            conn, deployment_name=_DEPLOYMENT
        )
        assert before["classification"] in {"no_repair", "ordinary_repair"}, before
        assert isinstance(before["payload_match"], bool)
        assert _effective_interval_payload_parity_applicability(conn)["applicable"]

        with _pre_narrow_split_schema(conn, dsn=dsn):
            pending = classify_effective_attribute_recovery(
                conn, deployment_name=_DEPLOYMENT
            )

        # The verdict is about the SCHEMA, and the parity field is unset --
        # never the True that a narrowed comparison would have produced.
        assert pending["classification"] == "schema_migration_pending", pending
        assert "payload_match" in pending
        assert pending["payload_match"] is None
        parity = pending["payload_parity"]
        assert parity["applicable"] is False
        assert parity["reason"] == "schema_predates_interval_narrow_split"
        assert parity["required_migration"] == _INTERVAL_NARROW_SPLIT_MIGRATION
        assert parity["absent_relations"] == []
        assert parity["missing_payload_columns"] == {
            ATTRIBUTE_EFFECTIVE_INTERVALS_TABLE: ["attrs_hash"],
            ATTRIBUTE_EFFECTIVE_VIEW_TABLE: ["attrs_hash"],
        }
        # Preflight's own contract: a bound, sha256-digested classification.
        assert pending["binding_digest"].startswith("sha256:")
        assert pending["deployment"] == _DEPLOYMENT

        # And the condition is self-clearing: the migrate phase the preflight
        # now reaches makes the very next classification answerable again.
        after = classify_effective_attribute_recovery(
            conn, deployment_name=_DEPLOYMENT
        )
        assert after["classification"] in {"no_repair", "ordinary_repair"}, after
        assert isinstance(after["payload_match"], bool)


def test_the_lifecycle_fixture_is_the_classifiers_real_placeholder(
    clean_runtime: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Pin the hermetic lifecycle fixture to what the classifier really emits.

    ``test_deployment_upgrade_pre_narrow_split.py`` drives ``_phase_preflight``
    and ``_phase_generation_recovery``, and the live-proof collector, against a
    hand-written classification.  A hand-written ``payload_match: None`` proves
    nothing about the classifier: flip the shipped code to False -- the single
    conflation this whole change exists to prevent -- and every one of those
    arms keeps passing.  So the fixture is compared HERE, field by field,
    against the real classifier's output on a real pre-0020 database, with the
    parity verdict compared by IDENTITY so False cannot pass as None.
    """
    monkeypatch.setenv("OKG_IVM_INTERVAL_VIEW", "1")
    dsn = clean_runtime
    _publish_one_generation(dsn)

    with psycopg.connect(dsn, autocommit=True) as conn:
        with _pre_narrow_split_schema(conn, dsn=dsn):
            real = classify_effective_attribute_recovery(
                conn, deployment_name=_DEPLOYMENT
            )

    fixture = MIGRATION_PENDING_CLASSIFICATION
    assert real["schema"] == fixture["schema"]
    assert real["classification"] == fixture["classification"]
    assert real["payload_match"] is fixture["payload_match"]
    assert real["payload_match"] is None
    for field in ("applicable", "reason", "required_migration"):
        assert real["payload_parity"][field] == fixture["payload_parity"][field], field
    # The premise the live proof rests on: a deferred preflight binds NO graph
    # anchor, so a consumer that reads it gets nothing to compare a successor
    # against.  Asserted on both sides so neither can drift alone.
    for field in GRAPH_ANCHOR_FIELDS:
        assert field not in real, field
        assert field not in fixture, field


def test_naive_drop_missing_columns_reports_a_false_parity_match(
    clean_runtime: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Why the check refuses instead of narrowing its column set.

    The interval front is made to disagree with the snapshot on the attribute
    payload and NOTHING else -- a genuine parity break.  Fingerprinting only the
    surviving columns drops the payload term from both sides and calls it a
    match.
    """
    monkeypatch.setenv("OKG_IVM_INTERVAL_VIEW", "1")
    dsn = clean_runtime
    generation = _publish_one_generation(dsn)

    with psycopg.connect(dsn, autocommit=True) as conn:
        branch_id, ordinal = _generation_anchor(conn, generation)
        with _pre_narrow_split_schema(conn, dsn=dsn):
            with conn.cursor() as cur:
                cur.execute(
                    f"UPDATE {ATTRIBUTE_EFFECTIVE_INTERVALS_TABLE} "
                    "SET attrs = attrs || %s::jsonb "
                    "WHERE branch_id = %s AND namespace = 'default' "
                    "AND from_ord <= %s AND %s < to_ord "
                    "AND target_id = ("
                    f"  SELECT min(target_id) FROM {ATTRIBUTE_EFFECTIVE_INTERVALS_TABLE}"
                    "   WHERE branch_id = %s AND namespace = 'default'"
                    "     AND from_ord <= %s AND %s < to_ord)",
                    (
                        Jsonb({"divergent": True}),
                        branch_id,
                        ordinal,
                        ordinal,
                        branch_id,
                        ordinal,
                        ordinal,
                    ),
                )
                assert cur.rowcount == 1

            # The rejected implementation, run against that real divergence.
            naive_snapshot, naive_interval = _naive_payload_signatures(
                conn,
                generation=generation,
                namespace="default",
                branch_id=branch_id,
                ordinal=ordinal,
            )
            assert naive_snapshot == naive_interval, (
                "fixture no longer models the false match this test exists for"
            )

            # The shipped implementation refuses to answer, and says why.
            with pytest.raises(SubstrateError) as excinfo:
                _effective_interval_payload_parity(
                    conn,
                    generation=generation,
                    namespace="default",
                    branch_id=branch_id,
                    ordinal=ordinal,
                )
            assert _INTERVAL_NARROW_SPLIT_MIGRATION in str(excinfo.value.fix_hint)

            pending = classify_effective_attribute_recovery(
                conn, deployment_name=_DEPLOYMENT
            )
            assert pending["classification"] == "schema_migration_pending"
            assert pending["payload_match"] is None

        # Migrate hashes the tampered payload into its own version row, so the
        # divergence the naive fingerprint erased is caught the moment the
        # check becomes applicable.
        assert _effective_interval_payload_parity(
            conn,
            generation=generation,
            namespace="default",
            branch_id=branch_id,
            ordinal=ordinal,
        ) is False
        repaired = classify_effective_attribute_recovery(
            conn, deployment_name=_DEPLOYMENT
        )
        assert repaired["payload_match"] is False
        assert repaired["classification"] == "ordinary_repair", repaired


def test_narrow_split_control_detects_the_same_divergence(
    clean_runtime: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The applicability gate must not neuter the check on a migrated database."""
    monkeypatch.setenv("OKG_IVM_INTERVAL_VIEW", "1")
    dsn = clean_runtime
    generation = _publish_one_generation(dsn)

    with psycopg.connect(dsn, autocommit=True) as conn:
        branch_id, ordinal = _generation_anchor(conn, generation)
        assert _effective_interval_payload_parity(
            conn,
            generation=generation,
            namespace="default",
            branch_id=branch_id,
            ordinal=ordinal,
        ) is True
        with conn.cursor() as cur:
            # Repoint one interval row at a DIFFERENT version of the same
            # target: the post-split spelling of "the payloads disagree".
            cur.execute(
                "SELECT target_kind, target_id FROM "
                f"{ATTRIBUTE_EFFECTIVE_INTERVALS_TABLE} "
                "WHERE branch_id = %s AND namespace = 'default' "
                "AND from_ord <= %s AND %s < to_ord "
                "ORDER BY target_id LIMIT 1",
                (branch_id, ordinal, ordinal),
            )
            target_kind, target_id = cur.fetchone()
            cur.execute(
                "SELECT okg.attribute_content_hash(%s::jsonb, NULL)",
                (Jsonb({"k": "divergent"}),),
            )
            divergent_hash = bytes(cur.fetchone()[0])
            cur.execute(
                "INSERT INTO okg.attribute_versions "
                "(target_kind, target_id, attrs_hash, attrs) "
                "VALUES (%s, %s, %s, %s::jsonb) "
                "ON CONFLICT (target_kind, target_id, attrs_hash) DO NOTHING",
                (target_kind, target_id, divergent_hash, Jsonb({"k": "divergent"})),
            )
            cur.execute(
                f"UPDATE {ATTRIBUTE_EFFECTIVE_INTERVALS_TABLE} "
                "SET attrs_hash = %s "
                "WHERE target_kind = %s AND target_id = %s AND branch_id = %s "
                "AND namespace = 'default' AND from_ord <= %s AND %s < to_ord",
                (
                    divergent_hash,
                    target_kind,
                    target_id,
                    branch_id,
                    ordinal,
                    ordinal,
                ),
            )
            assert cur.rowcount == 1
        assert _effective_interval_payload_parity(
            conn,
            generation=generation,
            namespace="default",
            branch_id=branch_id,
            ordinal=ordinal,
        ) is False


def test_applicability_reports_a_missing_relation_separately(
    clean_runtime: str,
) -> None:
    """An absent relation is its own reason, not conflated with the wide shape.

    "This database has no snapshot relation" and "this database predates the
    split" want different operator fixes, so they never share a reason string.
    The relation is renamed rather than dropped: reversible, and it keeps the
    session-shared database's grants and indexes intact.
    """
    dsn = clean_runtime
    with psycopg.connect(dsn, autocommit=True) as conn:
        assert _effective_interval_payload_parity_applicability(conn) == {
            "applicable": True,
            "reason": "applicable",
            "payload_columns": list(_INTERVAL_PAYLOAD_COLS),
        }
        with conn.cursor() as cur:
            cur.execute(
                f"ALTER TABLE {ATTRIBUTE_EFFECTIVE_VIEW_TABLE} "
                "RENAME TO attribute_effective_view_i1114_hidden"
            )
        try:
            absent = _effective_interval_payload_parity_applicability(conn)
        finally:
            with conn.cursor() as cur:
                cur.execute(
                    "ALTER TABLE okg.attribute_effective_view_i1114_hidden "
                    "RENAME TO attribute_effective_view"
                )
        assert absent["applicable"] is False
        assert absent["reason"] == "attribute_relations_absent"
        assert absent["absent_relations"] == [ATTRIBUTE_EFFECTIVE_VIEW_TABLE]
        assert absent["missing_payload_columns"] == {
            ATTRIBUTE_EFFECTIVE_VIEW_TABLE: list(_INTERVAL_PAYLOAD_COLS)
        }
        assert _effective_interval_payload_parity_applicability(conn)["applicable"]


def test_a_never_published_deployment_is_not_called_migration_pending(
    clean_runtime: str,
) -> None:
    """Schema applicability must not outrank "nothing has been published yet".

    The interval relation is created lazily by the FIRST publish, not by the
    migration schema, so a freshly provisioned deployment legitimately has no
    interval relation.  Classifying that as ``schema_migration_pending`` would
    demand a migration that changes nothing and then refuse the upgrade at
    generation_recovery when it changed nothing.
    """
    dsn = clean_runtime
    with psycopg.connect(dsn, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"ALTER TABLE {ATTRIBUTE_EFFECTIVE_INTERVALS_TABLE} "
                "RENAME TO attribute_effective_view_intervals_i1114_hidden"
            )
        try:
            # Non-vacuity: with the relation hidden the parity check really is
            # inapplicable, so the classification below is a genuine ordering
            # result and not a check that never fired.
            hidden = _effective_interval_payload_parity_applicability(conn)
            assert hidden["applicable"] is False
            assert hidden["absent_relations"] == [
                ATTRIBUTE_EFFECTIVE_INTERVALS_TABLE
            ]
            fresh = classify_effective_attribute_recovery(
                conn, deployment_name=_DEPLOYMENT
            )
        finally:
            with conn.cursor() as cur:
                cur.execute(
                    "ALTER TABLE "
                    "okg.attribute_effective_view_intervals_i1114_hidden "
                    "RENAME TO attribute_effective_view_intervals"
                )
    assert fresh["classification"] == "no_published_generation", fresh
    assert "payload_parity" not in fresh
