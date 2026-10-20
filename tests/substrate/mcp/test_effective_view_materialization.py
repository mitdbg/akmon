"""materialize-effective-attrs-scan-once.

The Layer-2 effective-attrs scan (`_effective_view_fast_rows`) used to fetch each
row's attribute blob TWICE from the database -- once as `attrs`, once as
`COALESCE(raw_attrs, attrs)`, which for the no-overlay majority is the same blob
-- buffer the whole result with `fetchall()`, and then build a second full copy
of both in Python. Four copies of every row's attributes existed at the peak.

These tests prove the three claims of the change:

  (a) each blob crosses the wire once and is decoded once, with raw/effective
      still carrying the values they carried before (including for a row that
      genuinely HAS an overlay, where raw != effective);
  (b) the result is streamed rather than buffered, and truncation still fires at
      the ceiling with the same row count as before; and
  (c) the aliasing this introduces -- when `raw_attrs` is NULL the filter input
      and the effective attrs are ONE object -- is not observable.

The row that has an overlay is written straight into
`okg.attribute_effective_view` rather than through
`record_attribute_observation`, so the test proves the read path's handling of
`raw_attrs != attrs` without depending on the overlay materializer.
"""

from __future__ import annotations

import dataclasses
import uuid

import pytest

psycopg = pytest.importorskip("psycopg")

from okg.substrate.errors import SubstrateError  # noqa: E402
from okg.substrate.graph_store import ChronosGraphStore  # noqa: E402
from okg.substrate.library.sources.base import NodeFact  # noqa: E402
from okg.substrate.mcp import open_session  # noqa: E402
# Imported as a MODULE, not `from ... import build_server`: the truncation test
# monkeypatches `_EFFECTIVE_VIEW_SCAN_CEILING` on it, and importing the same
# module both ways trips CodeQL's py/import-and-import-from.
from okg.substrate.mcp import server as server_mod  # noqa: E402
from okg.substrate.publisher import Publisher  # noqa: E402
from tests.pytest.catalog_fixture_builders import seed_catalog  # noqa: E402
from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer  # noqa: E402


pytestmark = pytest.mark.postgres


_NODE_COUNT = 12


@pytest.fixture(autouse=True)
def _mcp_role_can_log_in(okg_mcp_can_log_in: str) -> None:
    """Every test here needs a login for the `okg_mcp` role.

    These tests reach the scan through `build_server`, whose read pool connects
    as `okg_mcp`. Migration creates that role NOLOGIN
    (src/okg/substrate/db/views.sql). The shared `okg_mcp_can_log_in` fixture
    sets LOGIN under the cluster role lock and never reverts it.

    This fixture used to run its own `ALTER ROLE okg_mcp` from the worker
    database, before and after every test, without that lock. The role is one
    row shared by every database on the server, so it collided with whatever
    another worker was doing to the role: `tuple concurrently updated`, in
    this file's setup or in the other worker's test (issue #2554).
    """


def _seed(clean_runtime: str) -> dict[str, str]:
    """Publish a small graph and materialize its effective-attrs snapshot."""
    suffix = uuid.uuid4().hex[:8]
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog(conn, subtypes=["ci_workflow"])
        store = ChronosGraphStore.from_connection(conn)
        store.apply_facts(
            [
                NodeFact(
                    f"wf:{i}:{suffix}",
                    "ci_workflow",
                    {"conclusion": "success", "idx": i},
                    {"id": f"wf:{i}:{suffix}"},
                    {"v": 1},
                )
                for i in range(_NODE_COUNT)
            ],
            source="default",
            strict_endpoints=True,
        )
    published = Publisher(clean_runtime, "default", catalog_identity=bind_test_catalog_writer(clean_runtime, "default")).cycle()
    assert published["status"] == "published", published
    gen = str(published["generation_id"])
    from okg.substrate.attributes import (
        materialize_effective_attributes_for_generation,
    )

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        materialize_effective_attributes_for_generation(conn, generation_id=gen)
    return {"generation_id": gen, "suffix": suffix}


def _attr_rows(clean_runtime: str, gen: str):
    """Call the operator attr scan through the server's test hook."""
    mcp = server_mod.build_server(dsn=clean_runtime)
    attr_rows = mcp._okg_operator_attr_rows  # type: ignore[attr-defined]
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        session = dataclasses.replace(
            open_session(conn, deployment_name="default"), generation_id=gen
        )
        return attr_rows(
            conn, session, kind="nodes", limit=10_000, scan_predicate=None
        )


def _patch_effective_row(
    clean_runtime: str, gen: str, target_id: str, *, column: str, value: str
) -> None:
    """Write a value straight into the effective-attrs store for one target.

    BOTH backing tables are written. `_effective_view_source` reads the SHARED
    interval table when `OKG_IVM_INTERVAL_VIEW` is on (its default) and the
    per-generation snapshot when it is off; writing only one makes these tests
    pass vacuously under the other setting, which is exactly what happened the
    first time this file was written.
    """
    assert column in {"attrs", "raw_attrs"}, column
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        with conn.cursor() as cur:
            # schema-interval-narrow-split moved the payload out of both view
            # tables into the content-addressed okg.attribute_versions; the view
            # rows now carry only an `attrs_hash` reference. So write the version
            # row, which is what BOTH read sources resolve through -- one write
            # instead of the two this helper used to need. The version PK
            # includes target_id, so this cannot disturb another target that
            # happens to share content.
            cur.execute(
                f"UPDATE okg.attribute_versions SET {column} = %s::jsonb "
                "WHERE target_kind = 'node' AND target_id = %s",
                (value, target_id),
            )
            version_rows = cur.rowcount
    # No version row means the fixture never reached the target and every
    # assertion after this would be vacuous.
    assert version_rows, (target_id, gen)


# --------------------------------------------------------------------------- #
# (a) one blob per row on the wire
# --------------------------------------------------------------------------- #


def test_the_select_fetches_each_attribute_blob_once(clean_runtime: str) -> None:
    """The scan's SELECT must not carry COALESCE(raw_attrs, attrs).

    Postgres does not deduplicate two target-list entries that evaluate equal, so
    that wrapper sent the SAME blob a second time for every row without an
    overlay -- the majority. Asserted on the statement actually issued, not on the
    payload: a payload assertion would pass just as well if the column were
    fetched and discarded, which is the behaviour being fixed.
    """
    seeded = _seed(clean_runtime)
    gen = seeded["generation_id"]
    statements: list[str] = []

    real_stream = psycopg.Cursor.stream

    def capturing_stream(self, query, params=None, **kw):
        statements.append(str(query))
        return real_stream(self, query, params, **kw)

    mcp = server_mod.build_server(dsn=clean_runtime)
    attr_rows = mcp._okg_operator_attr_rows  # type: ignore[attr-defined]
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        session = dataclasses.replace(
            open_session(conn, deployment_name="default"), generation_id=gen
        )
        psycopg.Cursor.stream = capturing_stream
        try:
            rows, truncated = attr_rows(
                conn, session, kind="nodes", limit=10_000, scan_predicate=None
            )
        finally:
            psycopg.Cursor.stream = real_stream

    scan_sql = [s for s in statements if "attribute_effective_view" in s]
    assert scan_sql, statements
    for sql in scan_sql:
        assert "COALESCE" not in sql.upper(), sql
        assert "raw_attrs" in sql, sql
    assert truncated is False
    assert len(rows) == _NODE_COUNT, len(rows)
    # A row with no overlay: raw and effective are both the stored effective.
    row = next(r for r in rows if r["node_id"].startswith("wf:0:"))
    assert row["attrs"]["conclusion"] == "success"


def test_an_overlay_row_is_served_from_the_effective_value(
    clean_runtime: str,
) -> None:
    """A row whose stored raw_attrs DIFFERS from attrs keeps both values.

    This is the case the removed COALESCE existed to serve, so it is the one that
    would break if `raw_attrs` were read as though a non-NULL value could be
    ignored. Nomos is off in this fixture, so the scan swaps the effective attrs
    in as the row's `attrs` -- the assertion is that the EFFECTIVE value wins,
    not the raw one.
    """
    seeded = _seed(clean_runtime)
    gen = seeded["generation_id"]
    target = f"wf:1:{seeded['suffix']}"
    # Make raw and effective genuinely differ, and make the EFFECTIVE value
    # distinguishable from the seeded default so the assertion below cannot pass
    # by accident if the overlay column were ignored.
    _patch_effective_row(
        clean_runtime, gen, target, column="raw_attrs",
        value='{"conclusion": "RAW_ONLY"}',
    )
    _patch_effective_row(
        clean_runtime, gen, target, column="attrs",
        value='{"conclusion": "EFFECTIVE_ONLY"}',
    )

    rows, _ = _attr_rows(clean_runtime, gen)
    row = next(r for r in rows if r["node_id"] == target)
    # Enforcement off: the scan swaps the EFFECTIVE attrs in as the row's attrs.
    # The raw value must not be what surfaces.
    assert row["attrs"]["conclusion"] == "EFFECTIVE_ONLY", row["attrs"]
    # And the untouched rows still carry their own values.
    other = next(r for r in rows if r["node_id"] == f"wf:2:{seeded['suffix']}")
    assert other["attrs"]["conclusion"] == "success", other["attrs"]


def test_rows_do_not_share_attribute_objects_with_each_other(
    clean_runtime: str,
) -> None:
    """Dropping the per-row copies must not make two ROWS share one dict.

    Scope note, because the obvious assertion here is a vacuous one. Within a
    single row, `attrs` and `_effective_attrs` DO alias when `raw_attrs` is NULL
    -- that aliasing is the memory saving, and it is deliberate. It is also not
    observable through this seam: `_effective_attrs` is popped or swapped in
    before the rows are returned (server.py, end of `_effective_view_fast_rows`),
    so a caller never holds both. Asserting "mutate `attrs`, then re-read the
    database and see the old value" would prove only that a Python dict mutation
    does not reach Postgres -- true of any implementation, including the one this
    change replaced.

    What IS observable, and what this asserts, is that the rows are mutually
    independent: mutating one row's attrs must not change another's. That would
    break if the loop ever hoisted a shared dict out of the per-row decode, which
    is a plausible next "optimisation" on top of this one.

    The within-row aliasing is safe because no read-path step mutates attrs in
    place -- `filter_node_rows_for_session` and `apply_effective_attrs_to_rows`
    both copy before touching and rebind the result. That was verified by audit,
    not by this test; if it ever stops being true, reinstate the copy in
    `_effective_view_fast_rows` rather than deleting this note.
    """
    seeded = _seed(clean_runtime)
    rows, _ = _attr_rows(clean_runtime, seeded["generation_id"])
    assert len(rows) >= 2
    # No two rows may be backed by the same object.
    identities = [id(r["attrs"]) for r in rows]
    assert len(set(identities)) == len(identities), "rows share an attrs dict"
    rows[0]["attrs"]["conclusion"] = "MUTATED"
    assert all(
        r["attrs"]["conclusion"] == "success" for r in rows[1:]
    ), [r["attrs"] for r in rows[1:]]


def test_a_non_object_attrs_value_is_refused_by_name(clean_runtime: str) -> None:
    """Dropping `dict(...)` removed an incidental type check.

    A JSONB column holding an array used to raise inside `dict()`. With the copy
    gone it would flow downstream as a list and fail somewhere less obvious, so
    the scan refuses it explicitly and names the target id.
    """
    seeded = _seed(clean_runtime)
    gen = seeded["generation_id"]
    target = f"wf:3:{seeded['suffix']}"
    _patch_effective_row(
        clean_runtime, gen, target, column="attrs",
        value='["not", "an", "object"]',
    )

    with pytest.raises(SubstrateError) as excinfo:
        _attr_rows(clean_runtime, gen)
    assert target in str(excinfo.value.data), excinfo.value.data


# --------------------------------------------------------------------------- #
# (b) streamed, not buffered -- with identical results
# --------------------------------------------------------------------------- #


def test_streamed_rows_equal_buffered_rows(clean_runtime: str) -> None:
    """Streaming must change no result.

    The rows built, and their order, are compared against the same query consumed
    the old way (`execute` + `fetchall`) over the same snapshot. Equality here is
    what licenses the claim that this change is a pure memory reduction.
    """
    seeded = _seed(clean_runtime)
    gen = seeded["generation_id"]
    streamed, truncated = _attr_rows(clean_runtime, gen)

    # The same rows, read the buffered way straight from the snapshot.
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        with conn.cursor() as cur:
            # The payload lives in the version store since
            # schema-interval-narrow-split; join it the way production does.
            cur.execute(
                "SELECT v.target_id, v.attrs "
                "FROM okg.attribute_effective_view e "
                "JOIN okg.attribute_versions v "
                "  USING (target_kind, target_id, attrs_hash) "
                "WHERE e.target_kind = 'node' AND e.generation_id = %s "
                "AND e.namespace = 'default' ORDER BY v.target_id",
                (gen,),
            )
            expected = {t: a for t, a in cur.fetchall()}

    assert truncated is False
    assert {r["node_id"] for r in streamed} == set(expected)
    for row in streamed:
        assert row["attrs"] == expected[row["node_id"]], row["node_id"]


def test_truncation_still_fires_at_the_ceiling(
    clean_runtime: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """`truncated` keeps its exact meaning across the streaming rewrite.

    The ceiling is lowered for the test rather than seeding 200K rows. The
    contract is unchanged: a scan that yields MORE rows than the ceiling returns
    exactly `ceiling` rows with `truncated` True -- the ceiling-plus-first row is
    counted but never built -- and a scan that yields fewer returns them all with
    `truncated` False.
    """
    seeded = _seed(clean_runtime)
    gen = seeded["generation_id"]

    # Below the ceiling: every row, not truncated.
    rows, truncated = _attr_rows(clean_runtime, gen)
    assert truncated is False
    assert len(rows) == _NODE_COUNT, len(rows)

    # Above it: exactly `ceiling` rows built, truncated True. The
    # ceiling-plus-first row is COUNTED (that is what sets the flag) but never
    # built, matching the `raw[:ceiling]` slice this rewrite replaced.
    ceiling = _NODE_COUNT - 4
    monkeypatch.setattr(server_mod, "_EFFECTIVE_VIEW_SCAN_CEILING", ceiling)
    rows, truncated = _attr_rows(clean_runtime, gen)
    assert truncated is True
    assert len(rows) == ceiling, len(rows)
