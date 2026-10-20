"""scale-operator-attr-reads / req.effective-attrs-view + req.parity (Layer 2).

The indexed effective-attrs snapshot (``okg.attribute_effective_view``, built by
``materialize_effective_attribute_view_for_generation`` at publish) MUST return
EXACTLY what the Python overlay read path (``apply_effective_attrs_to_rows`` ->
``_current_overlays``) returns, key-for-key, for both nodes and edges:

  * accepted overlay OVERRIDES raw  (``raw || overlay``);
  * reserved effective keys are excluded from the OVERLAY (never from raw);
  * a multiple-current-accepted conflict is SURFACED (AttributeConflictError) on
    BOTH the materialize/refresh guard and the Python read path -- no silent
    last-wins.

These pin the three highest parity risks the design spec flagged: the
raw-vs-overlay reserved-key asymmetry, the conflict-surfaced failure case, and
edge-kind parity. Operator-level happy-path + Layer-1 fallback coverage lives in
``tests/substrate/mcp/test_operator_attr_pushdown.py``; this file pins the SQL
materialized representation itself against the Python merge.
"""

from __future__ import annotations

import uuid

import pytest

psycopg = pytest.importorskip("psycopg")

from okg.substrate.attributes import (  # noqa: E402
    ATTRIBUTE_EFFECTIVE_VIEW_TABLE,
    AttributeConflictError,
    apply_effective_attrs_to_rows,
    materialize_effective_attribute_view_for_generation,
    materialize_effective_attributes_for_generation,
    record_attribute_observation,
    refresh_effective_attribute_view_overlay_for_generation,
)
from okg.substrate.graph_store import ChronosGraphStore  # noqa: E402
from okg.substrate.library.sources.base import EdgeFact, NodeFact  # noqa: E402
from okg.substrate.mcp import expand_neighbors, open_session  # noqa: E402
from okg.substrate.nomos.source_lineage import LINEAGE_ATTR  # noqa: E402
from okg.substrate.publisher import Publisher  # noqa: E402
from tests.pytest.catalog_fixture_builders import seed_catalog  # noqa: E402
from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer  # noqa: E402


pytestmark = pytest.mark.postgres


def _seed(clean_runtime: str) -> dict[str, str | int]:
    """Publish a tiny node+edge graph (so both kinds materialize), then accept
    overlays that (a) override a raw value, (b) carry a reserved effective key
    (which the overlay path drops), and (c) add a fresh effective key. Returns
    the published generation + ids."""
    suffix = uuid.uuid4().hex[:8]
    src = f"node:src:{suffix}"
    dst = f"node:dst:{suffix}"
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog(
            conn,
            subtypes=["case", "person"],
            narrowings=[("case", "reported_by", "person")],
        )
        ChronosGraphStore.from_connection(conn).apply_facts(
            [
                NodeFact(
                    src,
                    "case",
                    {"raw_status": "source", "raw_only": True, "shared": "raw"},
                    {"id": src},
                    {"v": 1},
                ),
                NodeFact(dst, "person", {"text": "reporter"}, {"id": dst}, {"v": 1}),
                EdgeFact(
                    src,
                    dst,
                    "reported_by",
                    attrs={"edge_raw": "source", "shared": "raw"},
                    source_record_id={"id": f"edge:{suffix}"},
                    source_revision={"v": 1},
                ),
            ],
            source="default",
            strict_endpoints=True,
        )
    published = Publisher(clean_runtime, "default", catalog_identity=bind_test_catalog_writer(clean_runtime, "default")).cycle()
    assert published["status"] == "published", published
    gen = str(published["generation_id"])
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        session = open_session(conn, deployment_name="default")
        edge_id = expand_neighbors(conn, session, src)["neighbors"][0]["edge_id"]
    return {"src": src, "dst": dst, "edge_id": edge_id, "generation_id": gen}


def _view_attrs(conn, *, generation_id: str, target_kind: str, target_id: str) -> dict:
    # schema-interval-narrow-split: the payload materializes through the
    # version-store join.
    with conn.cursor() as cur:
        cur.execute(
            f"SELECT av.attrs FROM {ATTRIBUTE_EFFECTIVE_VIEW_TABLE} v "
            "JOIN okg.attribute_versions av "
            "  ON av.target_kind = v.target_kind "
            " AND av.target_id = v.target_id "
            " AND av.attrs_hash = v.attrs_hash "
            "WHERE v.target_kind = %s AND v.target_id = %s "
            "AND v.generation_id = %s AND v.namespace = 'default'",
            (target_kind, target_id, generation_id),
        )
        row = cur.fetchone()
    assert row is not None, (target_kind, target_id, generation_id)
    return dict(row[0] or {})


def _python_attrs(conn, *, generation_id: str, target_kind: str, target_id: str) -> dict:
    """The reference: raw chronos attrs for this generation with the Python
    overlay merge applied -- the exact path the Layer-1 operator fallback uses."""
    graph = ChronosGraphStore.from_connection(
        conn, ensure_schema=False
    ).checkout_generation(generation_id)
    table = "okg.graph_nodes" if target_kind == "node" else "okg.graph_edges"
    id_col = "node_id" if target_kind == "node" else "edge_id"
    where_id = target_id if target_kind == "node" else int(target_id)
    raw = graph.query(
        f"SELECT {id_col} AS tid, attrs FROM {table} WHERE {id_col} = :tid",
        {"tid": where_id},
    )[0]
    rows = apply_effective_attrs_to_rows(
        conn,
        [{"tid": str(raw["tid"]), "attrs": raw["attrs"]}],
        target_kind=target_kind,
        target_id_field="tid",
        generation_id=generation_id,
    )
    return dict(rows[0]["attrs"])


def test_view_node_attrs_match_python_overlay_keyforkey(clean_runtime: str) -> None:
    """The materialized view's node ``attrs`` equals the Python overlay output
    key-for-key: overlay overrides raw, a NEW overlay key is present, an
    untouched raw key passes through, and a RESERVED key submitted AS AN OVERLAY
    is excluded (does NOT override the raw lineage value -- the empirically
    confirmed raw-vs-overlay asymmetry: raw carries ``nomos_source_lineage``,
    neither path strips it from raw, so the FORGED overlay value must lose).

    This pins the spec's #1 parity risk: raw DOES carry the reserved key, so the
    materialize must NOT strip raw (it does ``raw || overlay`` with no raw-strip);
    the reserved exclusion applies to the OVERLAY only. view == py proves it.
    """
    fx = _seed(clean_runtime)
    gen, src = str(fx["generation_id"]), str(fx["src"])
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        # The raw lineage value the publisher stamped (the value the forged
        # overlay must NOT be able to override).
        raw_lineage = _python_attrs(
            conn, generation_id=gen, target_kind="node", target_id=src
        )[LINEAGE_ATTR]
        # (a) override a raw value, (b) add a new effective key,
        # (c) a RESERVED key the overlay path must DROP (never override raw).
        record_attribute_observation(
            conn, target_kind="node", target_id=src, generation_id=gen,
            attr_key="raw_status", value="overridden", producer="reviewer",
        )
        record_attribute_observation(
            conn, target_kind="node", target_id=src, generation_id=gen,
            attr_key="derived_key", value="new", producer="reviewer",
        )
        record_attribute_observation(
            conn, target_kind="node", target_id=src, generation_id=gen,
            attr_key=LINEAGE_ATTR, value={"forged": True}, producer="reviewer",
        )
        materialize_effective_attributes_for_generation(conn, generation_id=gen)

        view = _view_attrs(conn, generation_id=gen, target_kind="node", target_id=src)
        py = _python_attrs(conn, generation_id=gen, target_kind="node", target_id=src)

    assert view == py, (view, py)
    assert view["raw_status"] == "overridden"  # overlay overrides raw
    assert view["derived_key"] == "new"        # new overlay key present
    assert view["shared"] == "raw"             # untouched raw passes through
    # Reserved key submitted as an overlay is EXCLUDED -> the forged value never
    # wins; the raw lineage value survives unchanged on BOTH paths.
    assert view[LINEAGE_ATTR] == raw_lineage
    assert view[LINEAGE_ATTR] != {"forged": True}


def test_view_edge_attrs_match_python_overlay_keyforkey(clean_runtime: str) -> None:
    """Edge-kind parity: the materialized view's edge ``attrs`` equals the
    Python overlay output (override + reserved exclusion + raw passthrough)."""
    fx = _seed(clean_runtime)
    gen, edge_id = str(fx["generation_id"]), str(fx["edge_id"])
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        record_attribute_observation(
            conn, target_kind="edge", target_id=edge_id, generation_id=gen,
            attr_key="edge_raw", value="overridden", producer="reviewer",
        )
        record_attribute_observation(
            conn, target_kind="edge", target_id=edge_id, generation_id=gen,
            attr_key=LINEAGE_ATTR, value={"forged": True}, producer="reviewer",
        )
        materialize_effective_attributes_for_generation(conn, generation_id=gen)

        view = _view_attrs(conn, generation_id=gen, target_kind="edge", target_id=edge_id)
        py = _python_attrs(conn, generation_id=gen, target_kind="edge", target_id=edge_id)

    assert view == py, (view, py)
    assert view["edge_raw"] == "overridden"
    assert view["shared"] == "raw"
    # Reserved key submitted as an overlay never overrides whatever raw carried;
    # the forged value loses on both paths (parity proven by view == py).
    assert view.get(LINEAGE_ATTR) != {"forged": True}


def test_view_raw_passthrough_when_no_overlay(clean_runtime: str) -> None:
    """With NO overlay for the generation, the view's ``attrs`` equals raw attrs
    exactly (no reserved-key strip of raw, no drift). Raw carries the
    publisher-stamped ``nomos_source_lineage`` key; the view keeps it verbatim,
    exactly as the Python path does."""
    fx = _seed(clean_runtime)
    gen, src = str(fx["generation_id"]), str(fx["src"])
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        view = _view_attrs(conn, generation_id=gen, target_kind="node", target_id=src)
        py = _python_attrs(conn, generation_id=gen, target_kind="node", target_id=src)
    assert view == py
    # The three source-authored raw keys are present untouched; lineage (a
    # reserved key) is present in raw and NOT stripped from raw (parity).
    assert view["raw_status"] == "source"
    assert view["raw_only"] is True
    assert view["shared"] == "raw"
    assert LINEAGE_ATTR in view


def test_conflict_surfaced_on_materialize_and_python(clean_runtime: str) -> None:
    """A multiple-current-accepted collision MUST surface an
    AttributeConflictError on BOTH the SQL materialize/refresh guard (reads
    ``attribute_observations``) and the Python read path (reads
    ``attribute_effective_attrs``) -- never a silent last-wins. Each table's own
    uniqueness guard makes the collision structurally impossible, so we inject it
    by temporarily dropping that guard, exactly the case the explicit guards
    exist to catch (the spec's no-default-fallback contract)."""
    fx = _seed(clean_runtime)
    gen, src = str(fx["generation_id"]), str(fx["src"])
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        a = record_attribute_observation(
            conn, target_kind="node", target_id=src, generation_id=gen,
            attr_key="conflicted", value="first", producer="reviewer",
        )
        b = record_attribute_observation(
            conn, target_kind="node", target_id=src, generation_id=gen,
            attr_key="conflicted", value="second", producer="reviewer",
        )
        materialize_effective_attributes_for_generation(conn, generation_id=gen)

    # The conflict injection drops uniqueness guards + inserts a duplicate; run
    # it in a transaction we ROLL BACK so the shared per-test DB schema is never
    # corrupted (clean_runtime truncates rows but does not recreate dropped
    # constraints). A SAVEPOINT lets the expected AttributeConflictError aborts be
    # caught without poisoning the outer transaction.
    with psycopg.connect(clean_runtime, autocommit=False) as conn:
        with conn.cursor() as cur:
            cur.execute("DROP INDEX okg.idx_attribute_observations_current_unique")
            cur.execute(
                "UPDATE okg.attribute_observations "
                "SET status = 'accepted', superseded_by = NULL "
                "WHERE observation_id = ANY(%s)",
                ([a.observation_id, b.observation_id],),
            )
            # Mirror the collision into the overlay snapshot the Python path reads.
            cur.execute(
                "ALTER TABLE okg.attribute_effective_attrs "
                "DROP CONSTRAINT attribute_effective_attrs_pkey"
            )
            cur.execute(
                "INSERT INTO okg.attribute_effective_attrs "
                "(target_kind, target_id, generation_id, namespace, attr_key, "
                " observation_id, value, value_hash, producer) "
                "SELECT target_kind, target_id, generation_id, namespace, "
                " attr_key, %s, value, value_hash, producer "
                "FROM okg.attribute_effective_attrs "
                "WHERE target_id = %s AND generation_id = %s "
                "AND attr_key = 'conflicted'",
                (a.observation_id, src, gen),
            )

        # Python read path raises on the multiple-current-accepted overlay.
        with conn.transaction(savepoint_name="py_conflict"):
            with pytest.raises(AttributeConflictError):
                apply_effective_attrs_to_rows(
                    conn,
                    [{"tid": src, "attrs": {}}],
                    target_kind="node",
                    target_id_field="tid",
                    generation_id=gen,
                )

        # The materialize/refresh guard (_assert_no_overlay_conflict over
        # attribute_observations) raises on the SAME collision -> parity on the
        # failure case, no silent last-wins.
        with conn.transaction(savepoint_name="sql_conflict"):
            with pytest.raises(AttributeConflictError):
                refresh_effective_attribute_view_overlay_for_generation(
                    conn, generation_id=gen
                )

        conn.rollback()  # discard the dropped guards + duplicate rows


def test_view_idempotent_and_overlay_refresh_propagates(clean_runtime: str) -> None:
    """A re-materialize for an already-built generation is a no-op; a POST-publish
    overlay write + re-materialize propagates the new effective value into the
    snapshot so the SQL fast path stays in parity with the Python read path."""
    fx = _seed(clean_runtime)
    gen, src = str(fx["generation_id"]), str(fx["src"])
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        # Re-materialize: already built at publish -> no-op.
        again = materialize_effective_attribute_view_for_generation(
            conn, generation_id=gen
        )
        assert again["materialized"] is False
        assert again["reason"] == "already_materialized"

        # POST-publish overlay write, then re-materialize the overlay snapshot.
        record_attribute_observation(
            conn, target_kind="node", target_id=src, generation_id=gen,
            attr_key="raw_status", value="late", producer="reviewer",
        )
        materialize_effective_attributes_for_generation(conn, generation_id=gen)

        view = _view_attrs(conn, generation_id=gen, target_kind="node", target_id=src)
        py = _python_attrs(conn, generation_id=gen, target_kind="node", target_id=src)
    assert view == py
    assert view["raw_status"] == "late"
