"""fix-projection-alias-identity #686: alias-seeding collisions are detected,
refused per value, deterministic, and surfaced.

Pre-fix, `seed_from_subtype` selected candidate nodes with no ORDER BY and
inserted with `ON CONFLICT DO NOTHING`, so two distinct nodes sharing a field
value collapsed onto whichever row scanned first; the winner could flip across
applies and the collision was tallied as a benign conflict.

The fix (ruling 2) is PER-VALUE REFUSAL: an alias value that maps to more than
one distinct canonical is seeded for NO node, a typed record is persisted in
`okg.alias_collisions`, and it is reported in the seed output. Non-colliding
values still seed. Exclusion is stable across applies (no winner to flip).
"""
from __future__ import annotations

import json
import uuid

import psycopg
import pytest

from okg.substrate.catalog.aliases import (
    list_alias_collisions,
    seed_from_subtype,
)
from okg.substrate.catalog.modules_apply import apply_composed_catalog
from okg.substrate.catalog.modules_compose import ComposedCatalog
from okg.substrate.cli.main import main
from okg.substrate.graph_store import ChronosGraphStore
from okg.substrate.library.sources.base import NodeFact

pytestmark = pytest.mark.postgres


def _seed_cv(conn: psycopg.Connection) -> int:
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO okg.catalog_versions (status, applied_at) "
            "VALUES ('applied', now()) RETURNING catalog_version_id"
        )
        cv = int(cur.fetchone()[0])
        cur.execute(
            "INSERT INTO okg.subtypes (name, catalog_version_id) "
            "VALUES ('author', %s)",
            (cv,),
        )
    return cv


def _add_authors(
    conn: psycopg.Connection, nodes: list[tuple[str, str]]
) -> None:
    # nodes: (node_id, display_name)
    store = ChronosGraphStore.from_connection(conn)
    try:
        store.apply_facts([
            NodeFact(
                node_id, "author", {"name": name}, {"id": node_id}, {"v": 1}
            )
            for node_id, name in nodes
        ])
    finally:
        store.close()


def _aliases_at(conn: psycopg.Connection, cv: int) -> dict[str, str]:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT alias, canonical FROM okg.aliases "
            " WHERE catalog_version_id = %s",
            (cv,),
        )
        return dict(cur.fetchall())


def test_collision_value_seeded_for_no_node_and_surfaced(
    clean_runtime: str,
) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        cv = _seed_cv(conn)
        # Two DISTINCT author nodes share the display name "J. Smith" -> a
        # collision. "K. Jones" is unique -> seeds normally.
        _add_authors(conn, [
            ("author:smith-a", "J. Smith"),
            ("author:smith-b", "J. Smith"),
            ("author:jones", "K. Jones"),
        ])

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        res = seed_from_subtype(
            conn, subtype="author", field="name", apply=True,
        )

    # The ambiguous value is refused; the unique one seeds.
    assert res.collision_count == 1
    assert res.inserted_count == 1
    assert [c.alias_value for c in res.collisions] == ["J. Smith"]
    assert res.collisions[0].distinct_canonical_count == 2
    assert res.collisions[0].canonicals == ["author:smith-a", "author:smith-b"]

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        aliases = _aliases_at(conn, cv)
        # "J. Smith" maps to NOTHING (not to an arbitrary winner).
        assert "J. Smith" not in aliases
        assert aliases == {"K. Jones": "author:jones"}
        # The collision is a standing, queryable record (doctor surface).
        standing = list_alias_collisions(conn)
        assert len(standing) == 1
        assert standing[0]["alias_value"] == "J. Smith"
        assert standing[0]["distinct_canonical_count"] == 2


def test_collision_refusal_is_stable_across_applies(clean_runtime: str) -> None:
    """No arbitrary winner means nothing to flip: repeated applies keep
    refusing the ambiguous value identically."""
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        cv = _seed_cv(conn)
        _add_authors(conn, [
            ("author:a", "J. Smith"),
            ("author:b", "J. Smith"),
        ])

    winners: set[str] = set()
    for _ in range(3):
        with psycopg.connect(clean_runtime, autocommit=True) as conn:
            res = seed_from_subtype(
                conn, subtype="author", field="name", apply=True,
            )
            assert res.collision_count == 1
            assert res.inserted_count == 0
            aliases = _aliases_at(conn, cv)
            # No alias row exists for the ambiguous value in ANY apply.
            winners.add(aliases.get("J. Smith", "__none__"))

    assert winners == {"__none__"}, winners


def test_late_collision_quarantines_existing_winner_and_clears_when_resolved(
    clean_runtime: str,
) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        cv = _seed_cv(conn)
        _add_authors(conn, [("author:a", "J. Smith")])
        first = seed_from_subtype(
            conn, subtype="author", field="name", apply=True,
        )
        assert first.inserted_count == 1
        assert _aliases_at(conn, cv)["J. Smith"] == "author:a"

        _add_authors(conn, [("author:b", "J. Smith")])
        collided = seed_from_subtype(
            conn, subtype="author", field="name", apply=True,
        )
        assert collided.collision_count == 1
        assert collided.quarantined_count == 1
        assert collided.sample_quarantined == [("J. Smith", "author:a")]
        assert "J. Smith" not in _aliases_at(conn, cv)

        repeated = seed_from_subtype(
            conn, subtype="author", field="name", apply=True,
        )
        assert repeated.collision_count == 1
        assert repeated.quarantined_count == 0
        assert "J. Smith" not in _aliases_at(conn, cv)

        # The former collision value is absent from the new candidate set;
        # the whole-field sweep must still clear its stale standing record.
        _add_authors(conn, [
            ("author:a", "Alice Smith"),
            ("author:b", "Bob Smith"),
        ])
        resolved = seed_from_subtype(
            conn, subtype="author", field="name", apply=True,
        )
        assert resolved.collision_count == 0
        assert list_alias_collisions(conn) == []


def test_catalog_apply_report_and_operator_surfaces_include_collision(
    clean_runtime: str,
    capsys,
) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_cv(conn)
        _add_authors(conn, [
            ("author:a", "J. Smith"),
            ("author:b", "J. Smith"),
        ])
        report: dict = {}
        apply_composed_catalog(
            conn,
            ComposedCatalog(
                deployment_name="test",
                modules=["test"],
                subtypes={"author"},
                subtype_origin={"author": "test"},
                subtype_metadata={"author": {"alias_fields": ["name"]}},
            ),
            apply_report=report,
        )

    assert report["alias_seeding"]["collision_count"] == 1
    assert report["alias_seeding"]["collision_samples"][0][
        "alias_value"
    ] == "J. Smith"

    rc = main([
        "--json", "alias", "collisions", "--limit", "10",
        "--dsn", clean_runtime,
    ])
    assert rc == 0
    alias_payload = json.loads(capsys.readouterr().out.strip().splitlines()[-1])
    assert alias_payload["collision_count"] == 1
    assert alias_payload["rows"][0]["alias_value"] == "J. Smith"

    rc = main([
        "--json", "doctor", "--check", "alias-collisions",
        "--dsn", clean_runtime,
    ])
    assert rc == 0
    doctor_payload = json.loads(capsys.readouterr().out.strip().splitlines()[-1])
    assert doctor_payload["status"] == "WARN"
    assert doctor_payload["warning_count"] == 1
    assert "okg alias collisions" in doctor_payload["remediation"]


def test_noncolliding_values_unaffected(clean_runtime: str) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        cv = _seed_cv(conn)
        _add_authors(conn, [
            (f"author:{uuid.uuid4().hex[:8]}", name)
            for name in ("Alice", "Bob", "Carol")
        ])

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        res = seed_from_subtype(
            conn, subtype="author", field="name", apply=True,
        )
    assert res.collision_count == 0
    assert res.inserted_count == 3
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        assert set(_aliases_at(conn, cv).keys()) == {"Alice", "Bob", "Carol"}
        assert list_alias_collisions(conn) == []
