"""Generic explicit-interval storage tests for maintained derivations (#444)."""
from __future__ import annotations

import uuid

import pytest

psycopg = pytest.importorskip("psycopg")
from psycopg.types.json import Jsonb  # noqa: E402

import okg.substrate.derived_artifacts.maintained_derivation as md  # noqa: E402
from okg.substrate.derived_artifacts.maintained_derivation import (  # noqa: E402
    EXPLICIT_INTERVAL_OPEN_ORD,
    ExplicitIntervalTable,
    explicit_interval_stab_rows,
    sync_explicit_interval_rows_for_generation,
)
from okg.substrate.errors import SubstrateError  # noqa: E402
from okg.substrate.graph_store import ChronosGraphStore  # noqa: E402
from okg.substrate.graph_store_internal.generation_ordinal import (  # noqa: E402
    assign_generation_ordinal,
)

pytestmark = pytest.mark.postgres

_SNAPSHOT = "okg.test_md_interval_snapshot"
_INTERVALS = "okg.test_md_interval_rows"
_SPEC = ExplicitIntervalTable(
    table=_INTERVALS,
    snapshot_table=_SNAPSHOT,
    payload_columns=("payload", "attrs"),
    read_columns=("target_id", "payload", "attrs"),
    target_kinds=("item",),
)


def _setup(conn: psycopg.Connection) -> None:
    with conn.cursor() as cur:
        cur.execute(f"DROP TABLE IF EXISTS {_INTERVALS}")
        cur.execute(f"DROP TABLE IF EXISTS {_SNAPSHOT}")
        cur.execute(
            f"""
            CREATE TABLE {_SNAPSHOT} (
              target_kind TEXT NOT NULL,
              target_id TEXT NOT NULL,
              generation_id TEXT NOT NULL,
              namespace TEXT NOT NULL DEFAULT 'default',
              payload TEXT,
              attrs JSONB NOT NULL DEFAULT '{{}}'::jsonb,
              PRIMARY KEY (target_kind, target_id, generation_id, namespace)
            )
            """
        )
        cur.execute(
            f"""
            CREATE TABLE {_INTERVALS} (
              target_kind TEXT NOT NULL,
              target_id TEXT NOT NULL,
              namespace TEXT NOT NULL,
              branch_id TEXT NOT NULL,
              from_ord BIGINT NOT NULL,
              to_ord BIGINT NOT NULL DEFAULT {EXPLICIT_INTERVAL_OPEN_ORD},
              payload TEXT,
              attrs JSONB NOT NULL DEFAULT '{{}}'::jsonb,
              materialized_at TIMESTAMPTZ NOT NULL DEFAULT now(),
              PRIMARY KEY (
                target_kind, target_id, namespace, branch_id, from_ord
              ),
              CHECK (from_ord < to_ord)
            )
            """
        )


def _db(conn: psycopg.Connection):
    return ChronosGraphStore.from_connection(conn, ensure_schema=True).context.db


def _assign(
    conn: psycopg.Connection, generation_id: str, *, branch: str = "default"
) -> None:
    store = ChronosGraphStore.from_connection(conn, ensure_schema=True)
    assign_generation_ordinal(
        store.context.db,
        store.context,
        checkpoint_id=generation_id,
        branch=branch,
    )


def _insert_snapshot(
    conn: psycopg.Connection,
    generation_id: str,
    rows: list[tuple[str, str, dict[str, str]]],
) -> None:
    with conn.cursor() as cur:
        cur.executemany(
            f"""
            INSERT INTO {_SNAPSHOT}
              (target_kind, target_id, generation_id, namespace, payload, attrs)
            VALUES ('item', %s, %s, 'default', %s, %s)
            """,
            [
                (target_id, generation_id, payload, Jsonb(attrs))
                for target_id, payload, attrs in rows
            ],
        )


def _stab(
    conn: psycopg.Connection, db, generation_id: str
) -> dict[str, tuple[str, dict]]:
    rows = explicit_interval_stab_rows(
        conn,
        db,
        spec=_SPEC,
        generation_id=generation_id,
        target_kind="item",
    )
    return {str(row[0]): (str(row[1]), dict(row[2])) for row in rows}


def test_maintained_derivation_interval_storage_delta_time_travel_and_branch(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Force every sync through multiple keyset batches; parity and idempotence must
    # not depend on the default production batch size.
    monkeypatch.setattr(md, "_EXPLICIT_INTERVAL_INSERT_BATCH_SIZE", 1)
    suffix = uuid.uuid4().hex[:8]
    g1 = f"generation:test-md-interval:{suffix}:g1"
    g2 = f"generation:test-md-interval:{suffix}:g2"
    other = f"generation:test-md-interval:{suffix}:other"

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _setup(conn)
        db = _db(conn)

        _assign(conn, g1)
        _insert_snapshot(
            conn,
            g1,
            [
                ("a", "old-a", {"v": "1"}),
                ("b", "old-b", {"v": "1"}),
            ],
        )
        first = sync_explicit_interval_rows_for_generation(
            conn, db, spec=_SPEC, generation_id=g1
        )
        assert first["rows_written"] == 2
        assert _stab(conn, db, g1) == {
            "a": ("old-a", {"v": "1"}),
            "b": ("old-b", {"v": "1"}),
        }

        _assign(conn, other, branch="independent")
        _insert_snapshot(conn, other, [("a", "other-branch", {"v": "branch"})])
        other_sync = sync_explicit_interval_rows_for_generation(
            conn, db, spec=_SPEC, generation_id=other
        )
        assert other_sync["branch_id"] == "independent"
        assert _stab(conn, db, g1) == {
            "a": ("old-a", {"v": "1"}),
            "b": ("old-b", {"v": "1"}),
        }
        assert _stab(conn, db, other) == {"a": ("other-branch", {"v": "branch"})}

        _assign(conn, g2)
        _insert_snapshot(
            conn,
            g2,
            [
                ("a", "new-a", {"v": "2"}),
                ("c", "new-c", {"v": "1"}),
            ],
        )
        delta = sync_explicit_interval_rows_for_generation(
            conn,
            db,
            spec=_SPEC,
            generation_id=g2,
            dirty_target_ids_by_kind={"item": {"a", "b", "c"}},
        )
        assert delta["rows_written"] == 4
        assert delta["counts"]["item"] == {"closed": 2, "opened": 2}
        assert _stab(conn, db, g2) == {
            "a": ("new-a", {"v": "2"}),
            "c": ("new-c", {"v": "1"}),
        }
        assert _stab(conn, db, g1) == {
            "a": ("old-a", {"v": "1"}),
            "b": ("old-b", {"v": "1"}),
        }

        again = sync_explicit_interval_rows_for_generation(
            conn,
            db,
            spec=_SPEC,
            generation_id=g2,
            dirty_target_ids_by_kind={"item": {"a", "b", "c"}},
        )
        assert again["rows_written"] == 0


def test_maintained_derivation_interval_storage_missing_ordinal_fails_loud(
    clean_runtime: str,
) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _setup(conn)
        db = _db(conn)

        with pytest.raises(SubstrateError, match="no ordinal for generation"):
            explicit_interval_stab_rows(
                conn,
                db,
                spec=_SPEC,
                generation_id="generation:test-md-interval:missing",
                target_kind="item",
            )
