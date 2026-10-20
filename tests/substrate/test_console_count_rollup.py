"""Step-2 gate for console-count-rollup (#444 slice 2): the count-rollup module's
full-recount + oracle, exercised in isolation from the publish pipeline.

We seed graph_nodes / graph_edges DIRECTLY at the Chronos branch head (bypassing
apply_facts + the narrowing catalog) so the test isolates the rollup mechanism,
then force a full recount and assert the kept rows are byte-identical to the
COUNT_RECOMPUTE_SQL oracle via the kit's ``parity_check`` -- across all three
dimensions, with >= 2 buckets each.
"""
from __future__ import annotations

import psycopg
import pytest

from okg.substrate.derived_artifacts.maintained_derivation import (
    FULL_REASON_DIMENSION_SHAPE_UPGRADE,
    is_sentinel_row,
    parity_check,
)
from okg.substrate.derived_artifacts.maintained_derivation_constants import (
    UNATTRIBUTED_DERIVED_PRODUCER,
    encode_source_subtype,
)
from okg.substrate.graph_store import (
    CONSOLE_COUNT_ROLLUP_TABLE,
    DEFAULT_BRANCH,
    ChronosGraphStore,
)
from okg.substrate.publisher.console_count_rollup import (
    KIND_EDGE_TYPE,
    KIND_SOURCE,
    KIND_SOURCE_SUBTYPE,
    KIND_SUBTYPE,
    _COUNT_ORACLE,
    _COUNT_SENTINEL,
    maintain_console_count_rollup,
)

pytestmark = pytest.mark.postgres


# 4 nodes: subtype alpha x3 (a1,a2,c1), beta x1 (b1); source srcA x3 (a1,a2,b1),
# srcB x1 (c1). 2 edges: rel_one x1, rel_two x1. >= 2 buckets in every dimension.
_NODES = [
    {"node_id": "n:a1", "subtype": "alpha", "source": "srcA"},
    {"node_id": "n:a2", "subtype": "alpha", "source": "srcA"},
    {"node_id": "n:b1", "subtype": "beta", "source": "srcA"},
    {"node_id": "n:c1", "subtype": "alpha", "source": "srcB"},
]
_EDGES = [
    {"edge_id": 1, "src": "n:a1", "dst": "n:a2", "edge_type": "rel_one"},
    {"edge_id": 2, "src": "n:a1", "dst": "n:b1", "edge_type": "rel_two"},
]


def _seed_graph(store: ChronosGraphStore) -> None:
    session = store.checkout(DEFAULT_BRANCH)
    with session.transaction():
        session.upsert_rows("okg.graph_nodes", _NODES)
        session.upsert_rows("okg.graph_edges", _EDGES)


def _seed_n_nodes(store: ChronosGraphStore, n: int, *, n_subtypes: int = 12) -> None:
    nodes = [
        {"node_id": f"bn{i}", "subtype": f"st{i % n_subtypes:02d}",
         "source": f"src{i % 6}"}
        for i in range(n)
    ]
    session = store.checkout(DEFAULT_BRANCH)
    with session.transaction():
        for k in range(0, len(nodes), 5000):
            session.upsert_rows("okg.graph_nodes", nodes[k:k + 5000])


def _data_bucket_count(checkout) -> int:
    return len(_read_kept(checkout))


def _read_kept(checkout) -> dict[tuple[str, str], tuple[int]]:
    out: dict[tuple[str, str], tuple[int]] = {}
    for row in checkout.query(
        f"SELECT dimension_kind, dimension_value, item_count "
        f"FROM {CONSOLE_COUNT_ROLLUP_TABLE}"
    ):
        if is_sentinel_row(row, _COUNT_SENTINEL):
            continue
        out[(str(row["dimension_kind"]), str(row["dimension_value"]))] = (
            int(row["item_count"]),
        )
    return out


def test_console_count_rollup_full_recount_matches_oracle(clean_runtime: str) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn, ensure_schema=True)
        _seed_graph(store)

        summary = maintain_console_count_rollup(
            conn,
            generation_id=1,
            changed_node_ids=[],
            changed_edge_ids=[],
            prior_generation_checkpoint=None,
            force_full=True,
        )
        assert summary["mode"] == "full_recount"
        assert summary["reason"] == "forced"

        checkout = store.checkout(DEFAULT_BRANCH)
        kept = _read_kept(checkout)

        # Byte-identical to the from-scratch recompute (the kit's CORRECTNESS oracle).
        result = parity_check(_COUNT_ORACLE, checkout, kept)
        assert result.ok, (result, kept)

        # Multi-bucket in every dimension (>= 2 each), with the expected counts.
        assert {KIND_SUBTYPE, KIND_EDGE_TYPE, KIND_SOURCE} <= {k[0] for k in kept}
        assert kept[(KIND_SUBTYPE, "alpha")] == (3,)
        assert kept[(KIND_SUBTYPE, "beta")] == (1,)
        assert kept[(KIND_SOURCE, "srcA")] == (3,)
        assert kept[(KIND_SOURCE, "srcB")] == (1,)
        assert kept[(KIND_EDGE_TYPE, "rel_one")] == (1,)
        assert kept[(KIND_EDGE_TYPE, "rel_two")] == (1,)
        # source_subtype: per-(producer, subtype) node emission (issue #446).
        assert kept[(KIND_SOURCE_SUBTYPE, encode_source_subtype("srcA", "alpha"))] == (2,)
        assert kept[(KIND_SOURCE_SUBTYPE, encode_source_subtype("srcA", "beta"))] == (1,)
        assert kept[(KIND_SOURCE_SUBTYPE, encode_source_subtype("srcB", "alpha"))] == (1,)


def test_console_count_rollup_incremental_add_folds_onto_prior(
    clean_runtime: str,
) -> None:
    """After a full recount, an incremental cycle that ADDS a node folds +1 onto
    the prior buckets (running sum), not a recount, and stays byte-identical to the
    oracle. A pure add needs no prior checkpoint (the -1 leg is empty)."""
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn, ensure_schema=True)
        _seed_graph(store)
        maintain_console_count_rollup(
            conn,
            generation_id=1,
            changed_node_ids=[],
            changed_edge_ids=[],
            prior_generation_checkpoint=None,
            force_full=True,
        )

        # Add one alpha/srcB node directly, then fold it in incrementally.
        session = store.checkout(DEFAULT_BRANCH)
        with session.transaction():
            session.upsert_rows(
                "okg.graph_nodes",
                [{"node_id": "n:d1", "subtype": "alpha", "source": "srcB"}],
            )
        summary = maintain_console_count_rollup(
            conn,
            generation_id=2,
            changed_node_ids=["n:d1"],
            changed_edge_ids=[],
            prior_generation_checkpoint=None,
        )
        assert summary["mode"] == "incremental"
        assert "reason" not in summary  # incremental cycles carry no forcing reason
        # (subtype,alpha) + (source,srcB) + (source_subtype,srcB/alpha)
        assert summary["dirty_bucket_count"] == 3

        checkout = store.checkout(DEFAULT_BRANCH)
        kept = _read_kept(checkout)
        result = parity_check(_COUNT_ORACLE, checkout, kept)
        assert result.ok, (result, kept)
        assert kept[(KIND_SUBTYPE, "alpha")] == (4,)  # was 3, +1
        assert kept[(KIND_SOURCE, "srcB")] == (2,)  # was 1, +1
        assert kept[(KIND_SOURCE_SUBTYPE, encode_source_subtype("srcB", "alpha"))] == (2,)


def test_row5_top15_tiebreak_python_sort_matches_sql(clean_runtime: str) -> None:
    """ROW 5 cutover tiebreak: knowledge.py's flag-ON Python sort
    ``sorted(rows, key=(-count, subtype))[:15]`` must agree with the flag-OFF SQL
    ``ORDER BY n DESC, subtype COLLATE "C" LIMIT 15`` at the 15/16 boundary. Seed
    exactly 16 subtypes: 14 with distinct high counts + 2 TIED at count 1 (the 15th
    and 16th), so the secondary subtype tiebreak alone decides which survives."""
    nodes = []
    for i in range(14):  # counts 30,29,...,17 -- unambiguously the top 14
        for j in range(30 - i):
            nodes.append(
                {"node_id": f"hi{i:02d}_{j}", "subtype": f"hi{i:02d}", "source": "s"}
            )
    for name in ("sub_a", "sub_b"):  # count 1 each -- the boundary tie
        nodes.append({"node_id": f"{name}_0", "subtype": name, "source": "s"})

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn, ensure_schema=True)
        session = store.checkout(DEFAULT_BRANCH)
        with session.transaction():
            session.upsert_rows("okg.graph_nodes", nodes)
        checkout = store.checkout(DEFAULT_BRANCH)

        # flag-OFF path: the exact SQL knowledge.py runs (with the COLLATE "C" pin).
        flag_off = [
            (str(r["subtype"]), int(r["n"]))
            for r in checkout.query(
                'SELECT subtype, count(*) AS n FROM okg.graph_nodes '
                'GROUP BY subtype ORDER BY n DESC, subtype COLLATE "C" LIMIT 15'
            )
        ]
        # flag-ON path: all (subtype, count) sorted in Python, sliced to 15.
        all_rows = [
            (str(r["subtype"]), int(r["n"]))
            for r in checkout.query(
                "SELECT subtype, count(*) AS n FROM okg.graph_nodes GROUP BY subtype"
            )
        ]
        all_rows.sort(key=lambda r: (-r[1], r[0]))
        flag_on = all_rows[:15]

    assert flag_on == flag_off, f"tiebreak diverged:\nON ={flag_on}\nOFF={flag_off}"
    assert len(all_rows) == 16, all_rows  # exactly 16 subtypes (clean graph)
    assert all_rows[14][1] == all_rows[15][1] == 1, all_rows[13:]  # real boundary tie
    kept = {s for s, _ in flag_on}
    assert "sub_a" in kept and "sub_b" not in kept, kept  # subtype ASC kept the earlier


def test_console_count_rollup_read_path_import_light() -> None:
    """The read path must not import graph_store at MODULE level -- it redefines the
    table name as a literal and shares the kind constants via the import-light module,
    deferring its graph_store import (inside the kit's read_maintained_rows) into a
    function body. Parsed from source so a new top-level graph_store import fails loud."""
    import ast
    import inspect

    from okg.substrate.console_views import console_count_rollup_read

    tree = ast.parse(inspect.getsource(console_count_rollup_read))
    offenders: list[str] = []
    for node in tree.body:  # module-level statements only
        if isinstance(node, ast.Import):
            offenders += [n.name for n in node.names if "graph_store" in n.name]
        elif (
            isinstance(node, ast.ImportFrom)
            and node.module
            and "graph_store" in node.module
        ):
            offenders.append(node.module)
    assert not offenders, f"read path has a module-level graph_store import: {offenders}"


def test_console_count_rollup_drift_is_caught_by_oracle(clean_runtime: str) -> None:
    """A present-but-DRIFTED kept bucket fails the CORRECTNESS oracle (drift is the
    oracle's job; absence is the read's). We corrupt one bucket directly and assert
    parity_check flags exactly that key as mismatched."""
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn, ensure_schema=True)
        _seed_graph(store)
        maintain_console_count_rollup(
            conn,
            generation_id=1,
            changed_node_ids=[],
            changed_edge_ids=[],
            prior_generation_checkpoint=None,
            force_full=True,
        )
        # Corrupt the (subtype, alpha) bucket: kept says 99, the graph has 3.
        session = store.checkout(DEFAULT_BRANCH)
        with session.transaction():
            session.upsert_rows(
                CONSOLE_COUNT_ROLLUP_TABLE,
                [
                    {
                        "dimension_kind": KIND_SUBTYPE,
                        "dimension_value": "alpha",
                        "item_count": 99,
                    }
                ],
            )

        checkout = store.checkout(DEFAULT_BRANCH)
        kept = _read_kept(checkout)
        result = parity_check(_COUNT_ORACLE, checkout, kept)
        assert not result.ok
        assert (KIND_SUBTYPE, "alpha") in result.mismatched, result


def test_console_count_rollup_full_recount_empty_graph_writes_sentinel(
    clean_runtime: str,
) -> None:
    """An empty graph still maintains the rollup: zero data rows but a sentinel,
    so a later read can distinguish 'maintained, empty' from 'never maintained'."""
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn, ensure_schema=True)
        summary = maintain_console_count_rollup(
            conn,
            generation_id=1,
            changed_node_ids=[],
            changed_edge_ids=[],
            prior_generation_checkpoint=None,
            force_full=True,
        )
        assert summary["mode"] == "full_recount"

        checkout = store.checkout(DEFAULT_BRANCH)
        rows = checkout.query(
            f"SELECT dimension_kind, dimension_value, item_count "
            f"FROM {CONSOLE_COUNT_ROLLUP_TABLE}"
        )
        sentinels = [r for r in rows if is_sentinel_row(r, _COUNT_SENTINEL)]
        data = [r for r in rows if not is_sentinel_row(r, _COUNT_SENTINEL)]
        assert len(sentinels) == 1, "the maintain hook must stamp a sentinel"
        assert data == [], "empty graph => zero data buckets"


def test_marginal_cost_rollup_read_is_bounded_independent_of_graph_size(
    clean_runtime: str,
) -> None:
    """READ-WIN marginal cost (deterministic count proof): the maintained rollup the
    console read scans is a COMPACT table whose size is O(#buckets), NOT O(N). A 20x
    larger graph yields the SAME bucket count -- so the read is FLAT in graph size,
    while the raw count(*) GROUP BY it replaces scans O(N) rows. (Wall-clock headline,
    measured separately: raw 0.6->2.6->25ms at 1k/10k/100k vs rollup ~flat 3->5ms.)"""
    from tests.pytest.chronos_reset import reset_chronos_graph

    bucket_counts: dict[int, int] = {}
    for n in (1000, 20000):
        reset_chronos_graph(clean_runtime)
        with psycopg.connect(clean_runtime, autocommit=True) as conn:
            store = ChronosGraphStore.from_connection(conn, ensure_schema=True)
            _seed_n_nodes(store, n)
            maintain_console_count_rollup(
                conn, generation_id=1, changed_node_ids=[], changed_edge_ids=[],
                prior_generation_checkpoint=None, force_full=True,
            )
            checkout = store.checkout(DEFAULT_BRANCH)
            kept = _read_kept(checkout)
            bucket_counts[n] = len(kept)
            assert len({k[0] for k in kept}) >= 2  # multi-bucket (subtype + source)
    # 12 subtype + 6 source + 12 (producer, subtype) pair buckets (i%6, i%12 has
    # period lcm(6,12)=12), constant across a 20x graph -> read is O(buckets).
    assert bucket_counts[1000] == bucket_counts[20000] == 12 + 6 + 12, bucket_counts


def test_marginal_cost_rows_touched_tracks_changeset_not_graph(
    clean_runtime: str,
) -> None:
    """O(changed) marginal cost (deterministic count proof): the incremental fold's
    rows-touched (dirty_bucket_count -- the maintained_derivation_scope O(changed)
    numerator the bench records) tracks the CHANGESET, not the graph. The SAME 10-node
    changeset touches the SAME number of buckets at a 20x larger graph. (Wall-clock
    headline, measured separately: incremental maintain FLAT ~56ms across 1k/10k/100k,
    full recount SCALES 26->60ms.)"""
    from tests.pytest.chronos_reset import reset_chronos_graph

    rows_touched: dict[int, int] = {}
    for n in (1000, 20000):
        reset_chronos_graph(clean_runtime)
        with psycopg.connect(clean_runtime, autocommit=True) as conn:
            store = ChronosGraphStore.from_connection(conn, ensure_schema=True)
            _seed_n_nodes(store, n)
            maintain_console_count_rollup(
                conn, generation_id=1, changed_node_ids=[], changed_edge_ids=[],
                prior_generation_checkpoint=None, force_full=True,
            )
            gen1 = store.checkpoint_generation(
                branch=DEFAULT_BRANCH, deployment_name="default",
                status="published", catalog_version_id=1, metadata={"node_count": n},
            )
            new = [
                {"node_id": f"chg{j}", "subtype": f"st{j % 12:02d}",
                 "source": f"src{j % 6}"}
                for j in range(10)
            ]
            session = store.checkout(DEFAULT_BRANCH)
            with session.transaction():
                session.upsert_rows("okg.graph_nodes", new)
            summary = maintain_console_count_rollup(
                conn, generation_id=2,
                changed_node_ids=[d["node_id"] for d in new],
                changed_edge_ids=[], prior_generation_checkpoint=gen1,
            )
            assert summary["mode"] == "incremental"
            rows_touched[n] = summary["dirty_bucket_count"]
    assert rows_touched[1000] == rows_touched[20000], rows_touched
    assert rows_touched[1000] > 0


def test_source_subtype_separator_guard_raises_loud() -> None:
    """A subtype containing the 0x1f separator must raise at encode time, and
    a malformed or ambiguous dimension_value must raise at parse time — a
    silent mis-parse rendered as truth would be a P0 fallback."""
    from okg.substrate.derived_artifacts.maintained_derivation_constants import (
        parse_source_subtype,
    )

    with pytest.raises(ValueError, match="separator"):
        encode_source_subtype("srcA", "bad\x1fsubtype")
    with pytest.raises(ValueError, match="separator"):
        parse_source_subtype("no-separator-at-all")
    with pytest.raises(ValueError, match="separator"):
        parse_source_subtype("a\x1fb\x1fc")
    assert parse_source_subtype(encode_source_subtype("srcA", "alpha")) == (
        "srcA", "alpha",
    )


def test_source_subtype_producer_key_parity_with_ontology_rollup(
    clean_runtime: str,
) -> None:
    """One producer, ONE key, across both artifacts the Graph view joins: the
    source_subtype (node) producer expression and the ontology rollup's edge
    producer expression must key the SAME real producer identically — enricher
    linker recovery included, empty-string linker routed to the unattributed
    bucket in BOTH (the identity-injectivity failure class)."""
    from okg.substrate.publisher.ontology_rollup import (
        ONTOLOGY_ROLLUP_FULL_SQL,
    )

    nodes = [
        {"node_id": "n:git1", "subtype": "alpha", "source": "git"},
        {"node_id": "n:git2", "subtype": "beta", "source": "git"},
        {"node_id": "n:enr1", "subtype": "alpha", "source": "_publisher_derived",
         "source_record_id": {"linker": "semantic-linker", "rule_id": "r7"}},
        {"node_id": "n:dir1", "subtype": "beta", "source": "_enricher:direct-linker"},
        {"node_id": "n:un1", "subtype": "alpha", "source": "_publisher_derived"},
        {"node_id": "n:un2", "subtype": "alpha", "source": "_publisher_derived",
         "source_record_id": {"linker": ""}},
    ]
    edges = [
        {"edge_id": 1, "src": "n:git1", "dst": "n:git2", "edge_type": "rel_one",
         "source": "git"},
        {"edge_id": 2, "src": "n:git1", "dst": "n:enr1", "edge_type": "rel_two",
         "source": "_publisher_derived",
         "source_record_id": {"linker": "semantic-linker", "rule_id": "r7"}},
        {"edge_id": 3, "src": "n:git2", "dst": "n:dir1", "edge_type": "rel_one",
         "source": "_enricher:direct-linker"},
        {"edge_id": 4, "src": "n:un1", "dst": "n:un2", "edge_type": "rel_two",
         "source": "_publisher_derived"},
        {"edge_id": 5, "src": "n:un1", "dst": "n:git1", "edge_type": "rel_one",
         "source": "_publisher_derived", "source_record_id": {"linker": ""}},
    ]
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn, ensure_schema=True)
        session = store.checkout(DEFAULT_BRANCH)
        with session.transaction():
            session.upsert_rows("okg.graph_nodes", nodes)
            session.upsert_rows("okg.graph_edges", edges)
        maintain_console_count_rollup(
            conn, generation_id=1, changed_node_ids=[], changed_edge_ids=[],
            prior_generation_checkpoint=None, force_full=True,
        )
        checkout = store.checkout(DEFAULT_BRANCH)
        kept = _read_kept(checkout)
        node_producers = {
            value.split("\x1f")[0]
            for (kind, value) in kept
            if kind == KIND_SOURCE_SUBTYPE
        }
        edge_producers = {
            str(row["producer"])
            for row in checkout.query(ONTOLOGY_ROLLUP_FULL_SQL)
        }

    # The SAME producer key set on both sides — rule_id NOT baked into the key,
    # direct '_enricher:%' sources verbatim, sources verbatim.
    assert node_producers == edge_producers == {
        "git",
        "_enricher:semantic-linker",
        "_enricher:direct-linker",
        UNATTRIBUTED_DERIVED_PRODUCER,
    }
    # Empty-string linker routed to the unattributed bucket on the NODE side too
    # (n:un1 + n:un2 both land there): unattributed alpha emission == 2.
    assert kept[
        (KIND_SOURCE_SUBTYPE,
         encode_source_subtype(UNATTRIBUTED_DERIVED_PRODUCER, "alpha"))
    ] == (2,)


def test_source_subtype_linker_reattribution_stays_byte_equal(
    clean_runtime: str,
) -> None:
    """A source_record_id-only change that MOVES a node's recovered producer
    (linker rename) re-keys its (producer, subtype) bucket through the fold's
    -1-prior / +1-current legs and stays byte-equal to the oracle."""
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn, ensure_schema=True)
        session = store.checkout(DEFAULT_BRANCH)
        with session.transaction():
            session.upsert_rows("okg.graph_nodes", [
                {"node_id": "n:m1", "subtype": "alpha",
                 "source": "_publisher_derived",
                 "source_record_id": {"linker": "old-linker"}},
            ])
        maintain_console_count_rollup(
            conn, generation_id=1, changed_node_ids=[], changed_edge_ids=[],
            prior_generation_checkpoint=None, force_full=True,
        )
        gen1 = store.checkpoint_generation(
            branch=DEFAULT_BRANCH, deployment_name="default",
            status="published", catalog_version_id=1, metadata={},
        )
        session = store.checkout(DEFAULT_BRANCH)
        with session.transaction():
            session.upsert_rows("okg.graph_nodes", [
                {"node_id": "n:m1", "subtype": "alpha",
                 "source": "_publisher_derived",
                 "source_record_id": {"linker": "new-linker"}},
            ])
        summary = maintain_console_count_rollup(
            conn, generation_id=2, changed_node_ids=["n:m1"],
            changed_edge_ids=[], prior_generation_checkpoint=gen1,
        )
        assert summary["mode"] == "incremental"

        checkout = store.checkout(DEFAULT_BRANCH)
        kept = _read_kept(checkout)
        result = parity_check(_COUNT_ORACLE, checkout, kept)
        assert result.ok, (result, kept)
        assert kept[
            (KIND_SOURCE_SUBTYPE,
             encode_source_subtype("_enricher:new-linker", "alpha"))
        ] == (1,)
        assert (
            KIND_SOURCE_SUBTYPE,
            encode_source_subtype("_enricher:old-linker", "alpha"),
        ) not in kept


def test_read_producer_emission_detects_predates_dimension_on_real_rows(
    clean_runtime: str,
) -> None:
    """The read-side predates-dimension detection runs against REAL rollup
    state (not a monkeypatched read): a generation whose maintained rollup has
    source rows but no source_subtype rows reports dimension_available=False
    (an explicit unavailable, never an honest-looking zero), and a same-shape
    generation reports the rows with dimension_available=True."""
    from okg.substrate.console_views.console_count_rollup_read import (
        read_producer_emission,
    )

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn, ensure_schema=True)
        _seed_graph(store)
        maintain_console_count_rollup(
            conn, generation_id=1, changed_node_ids=[], changed_edge_ids=[],
            prior_generation_checkpoint=None, force_full=True,
        )

        # Same-shape generation: rows present, available.
        gen_ok = store.checkpoint_generation(
            branch=DEFAULT_BRANCH, deployment_name="default",
            status="published", catalog_version_id=1, metadata={},
        )
        emission = read_producer_emission(conn, generation_id=gen_ok)
        assert emission.dimension_available
        assert ("srcA", "alpha", 2) in emission.rows

        # Pre-upgrade shape: delete ONLY the source_subtype rows, checkpoint,
        # read as-of that generation.
        session = store.checkout(DEFAULT_BRANCH)
        stale_keys = [
            {"dimension_kind": kind, "dimension_value": value}
            for (kind, value) in _read_kept(session)
            if kind == KIND_SOURCE_SUBTYPE
        ]
        with session.transaction():
            session.delete_keys(CONSOLE_COUNT_ROLLUP_TABLE, stale_keys)
        gen_predates = store.checkpoint_generation(
            branch=DEFAULT_BRANCH, deployment_name="default",
            status="published", catalog_version_id=1, metadata={},
        )
        emission = read_producer_emission(conn, generation_id=gen_predates)
        assert not emission.dimension_available
        assert emission.rows == []


def test_source_subtype_shape_upgrade_backfills_once_then_quiescent(
    clean_runtime: str,
) -> None:
    """A branch-head rollup carried forward from a release that predates the
    source_subtype dimension (source rows, no source_subtype rows) triggers ONE
    callback full recount recorded as full_reason=dimension_shape_upgrade, which
    backfills the dimension; subsequent incremental cycles record no full_reason
    (quiescence — the fallback can never become an every-cycle recount)."""
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn, ensure_schema=True)
        _seed_graph(store)
        maintain_console_count_rollup(
            conn, generation_id=1, changed_node_ids=[], changed_edge_ids=[],
            prior_generation_checkpoint=None, force_full=True,
        )
        # Simulate the pre-upgrade shape: delete ONLY the source_subtype rows at
        # the branch head (what a pre-upgrade release's rows look like when
        # carried forward into the upgraded worker's next cycle).
        session = store.checkout(DEFAULT_BRANCH)
        stale_keys = [
            {"dimension_kind": kind, "dimension_value": value}
            for (kind, value) in _read_kept(session)
            if kind == KIND_SOURCE_SUBTYPE
        ]
        assert stale_keys, "full recount must have written the dimension"
        with session.transaction():
            session.delete_keys(CONSOLE_COUNT_ROLLUP_TABLE, stale_keys)

        summary = maintain_console_count_rollup(
            conn, generation_id=2, changed_node_ids=[], changed_edge_ids=[],
            prior_generation_checkpoint=None,
        )
        # Callback fallback under incremental dispatch: cause recorded, once.
        assert summary["mode"] == "incremental"
        assert summary["full_reason"] == FULL_REASON_DIMENSION_SHAPE_UPGRADE
        assert "reason" not in summary

        checkout = store.checkout(DEFAULT_BRANCH)
        kept = _read_kept(checkout)
        assert any(kind == KIND_SOURCE_SUBTYPE for (kind, _v) in kept), (
            "the backfill must repopulate the dimension"
        )
        result = parity_check(_COUNT_ORACLE, checkout, kept)
        assert result.ok, (result, kept)

        # Quiescence: the next ordinary cycles fold incrementally, no fallback.
        for gen in (3, 4):
            summary = maintain_console_count_rollup(
                conn, generation_id=gen, changed_node_ids=[], changed_edge_ids=[],
                prior_generation_checkpoint=None,
            )
            assert summary["mode"] == "incremental"
            assert "full_reason" not in summary, summary
