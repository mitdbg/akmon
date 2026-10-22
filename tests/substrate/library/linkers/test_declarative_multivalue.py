from __future__ import annotations

import psycopg
import pytest

from okg.substrate.enrichers.base import IncrementalContext
from okg.substrate.graph_store import ChronosGraphStore
from okg.substrate.library.linkers.declarative import DeclarativeLinker
from okg.substrate.library.linkers.rules import (
    CrossLinkRule,
    DeriveFromPath,
    RuleEndpoint,
    ViaEdge,
)
from okg.substrate.library.sources.base import EdgeFact, NodeFact
from tests.substrate._db_helpers import ensure_published_generation


pytestmark = pytest.mark.postgres


def _linker() -> DeclarativeLinker:
    return DeclarativeLinker(rules=[CrossLinkRule(
        id="multivalue_exact",
        src=RuleEndpoint(subtypes=("src_type",), field="key"),
        dst=RuleEndpoint(subtypes=("dst_type",), field="key"),
        matcher="exact",
        edge="references",
        bidirectional=False,
    )])


def _seed(
    conn: psycopg.Connection,
    facts: list[NodeFact],
    *,
    extra_subtypes: tuple[str, ...] = (),
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO okg.catalog_versions (status, applied_at) "
            "VALUES ('applied', now()) RETURNING catalog_version_id"
        )
        catalog_version = int(cur.fetchone()[0])
        ensure_published_generation(
            conn,
            1,
            deployment_name="declarative-multivalue-test",
            catalog_version_id=catalog_version,
        )
        for subtype in ("src_type", "dst_type", *extra_subtypes):
            cur.execute(
                "INSERT INTO okg.subtypes (name, catalog_version_id) "
                "VALUES (%s, %s)",
                (subtype, catalog_version),
            )
    ChronosGraphStore.from_connection(conn).apply_facts(
        facts, source="default", strict_endpoints=True,
    )


def test_exact_match_expands_string_arrays_on_either_endpoint(
    clean_runtime: str,
) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed(conn, [
            NodeFact(
                "src:list", "src_type", {"key": ["a", "b"]},
                {"id": "src:list"}, {"v": 1},
            ),
            NodeFact(
                "dst:scalar", "dst_type", {"key": "b"},
                {"id": "dst:scalar"}, {"v": 1},
            ),
            NodeFact(
                "src:scalar", "src_type", {"key": "c"},
                {"id": "src:scalar"}, {"v": 1},
            ),
            NodeFact(
                "dst:list", "dst_type", {"key": ["c", "d"]},
                {"id": "dst:list"}, {"v": 1},
            ),
        ])
        result = _linker().enrich(conn, generation_id=2)

    assert result.n_edges_emitted == 2


def test_incremental_exact_lookup_matches_array_without_full_scan(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate.library.linkers import declarative as module

    original_query = module._chronos.query
    bounded_queries = 0

    def guarded_query(cur, sql, params=None):
        nonlocal bounded_queries
        params = params or {}
        if (
            params.get("subtypes") == ["dst_type"]
            and "node_ids" not in params
            and "FROM okg.graph_nodes" in sql
        ):
            if sql.strip().endswith("LIMIT 1") and "attrs" not in sql:
                pass  # the one-row existence probe (issue #2941), not a scan
            elif any(key.startswith("field_value_") for key in params):
                bounded_queries += 1
            else:
                raise AssertionError("incremental exact match scanned all destinations")
        return original_query(cur, sql, params)

    monkeypatch.setattr(module._chronos, "query", guarded_query)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed(conn, [
            NodeFact(
                "src:dirty", "src_type", {"key": "paper:Graham_2021"},
                {"id": "src:dirty"}, {"v": 1},
            ),
            NodeFact(
                "dst:paper", "dst_type",
                {"key": ["paper:Graham:2021ggy", "paper:Graham_2021"]},
                {"id": "dst:paper"}, {"v": 1},
            ),
        ])
        result = _linker().enrich(
            conn,
            generation_id=2,
            incremental=IncrementalContext(
                prev_fact_watermark=1,
                prev_edge_watermark=1,
                dirty_node_ids=frozenset({"src:dirty"}),
                dirty_node_attr_changes_by_subtype={
                    "src_type": {
                        "src:dirty": frozenset({"__new__", "key"}),
                    },
                },
                is_first_run=False,
                last_status="success",
            ),
        )

    assert result.n_edges_emitted == 1
    assert bounded_queries == 1


# --- derive_from_path over list fields (issue #2921) -----------------------
#
# example-kb published 8 physics-group hubs and 0 edges into them: its rule
# captured a title root and looked it up in the hub's `title_roots` list, and
# derive_from_path compared the capture to the whole list.


def _derive_rule(
    *,
    src_field: str = "title",
    regex: str = r"^([A-Za-z]+)POG\b",
) -> CrossLinkRule:
    return CrossLinkRule(
        id="page_names_group_root",
        src=RuleEndpoint(subtypes=("src_type",), field=src_field),
        dst=RuleEndpoint(
            subtypes=("dst_type",),
            field="roots",
            derive_from_path=DeriveFromPath(
                attr=src_field,
                regex=regex,
                capture_field="roots",
            ),
        ),
        matcher="derive_from_path",
        edge="references",
        bidirectional=False,
    )


def _via_derive_rule(*, regex: str = r"^([A-Za-z]+)POG\b") -> CrossLinkRule:
    return CrossLinkRule(
        id="holder_contains_page_naming_group_root",
        src=RuleEndpoint(
            subtypes=("src_type",),
            via_edge=ViaEdge(edge_type="contains", dst_subtype="via_type"),
        ),
        dst=RuleEndpoint(
            subtypes=("dst_type",),
            field="roots",
            derive_from_path=DeriveFromPath(
                attr="title",
                regex=regex,
                capture_field="roots",
            ),
        ),
        matcher="derive_from_path",
        edge="references",
        bidirectional=False,
    )


def _node(node_id: str, subtype: str, attrs: dict) -> NodeFact:
    return NodeFact(node_id, subtype, attrs, {"id": node_id}, {"v": 1})


def _edges(conn: psycopg.Connection) -> set[tuple[str, str]]:
    """Edges the linker wrote; they land in `edge_facts` until a publish."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT src, dst FROM okg.edge_facts "
            " WHERE edge_type = 'references' AND op = 'I'"
        )
        return {(str(src), str(dst)) for src, dst in cur.fetchall()}


@pytest.mark.parametrize(
    "roots",
    [["MUO", "Muon"], "Muon"],
    ids=["list_field", "scalar_field"],
)
def test_derive_from_path_matches_a_list_or_scalar_destination_field(
    clean_runtime: str,
    roots: object,
) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed(conn, [
            _node("page:muon", "src_type", {"title": "MuonPOG Twiki"}),
            _node("page:other", "src_type", {"title": "Unrelated page"}),
            _node("hub:muon", "dst_type", {"roots": roots}),
            _node("hub:egamma", "dst_type", {"roots": ["EGM", "Egamma"]}),
        ])
        result = DeclarativeLinker(rules=[_derive_rule()]).enrich(
            conn, generation_id=2,
        )
        edges = _edges(conn)

    assert result.n_edges_emitted == 1
    assert ("page:muon", "hub:muon") in edges


def test_derive_from_path_expands_a_list_source_field(
    clean_runtime: str,
) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed(conn, [
            _node("page:two", "src_type", {
                "titles": ["Unrelated", "EgammaPOG Twiki", "MuonPOG Twiki"],
            }),
            _node("hub:muon", "dst_type", {"roots": "Muon"}),
            _node("hub:egamma", "dst_type", {"roots": ["EGM", "Egamma"]}),
        ])
        result = DeclarativeLinker(
            rules=[_derive_rule(src_field="titles")],
        ).enrich(conn, generation_id=2)
        edges = _edges(conn)

    assert result.n_edges_emitted == 2
    assert {("page:two", "hub:muon"), ("page:two", "hub:egamma")} <= edges


def test_incremental_derive_from_path_matches_lists_on_dirty_destination(
    clean_runtime: str,
) -> None:
    """A changed hub looks sources up by value; both ends are lists."""
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed(conn, [
            _node("page:two", "src_type", {
                "titles": ["Unrelated", "MuonPOG Twiki"],
            }),
            _node("hub:muon", "dst_type", {"roots": ["MUO", "Muon"]}),
        ])
        result = DeclarativeLinker(
            rules=[_derive_rule(src_field="titles")],
        ).enrich(
            conn,
            generation_id=2,
            incremental=IncrementalContext(
                prev_fact_watermark=1,
                prev_edge_watermark=1,
                dirty_node_ids=frozenset({"hub:muon"}),
                dirty_node_attr_changes_by_subtype={
                    "dst_type": {"hub:muon": frozenset({"__new__", "roots"})},
                },
                is_first_run=False,
                last_status="success",
            ),
        )

    assert result.n_edges_emitted == 1


def test_via_edge_derive_from_path_matches_a_list_destination_field(
    clean_runtime: str,
) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed(conn, [
            _node("holder:1", "src_type", {}),
            _node("page:muon", "via_type", {"title": "MuonPOG Twiki"}),
            _node("hub:muon", "dst_type", {"roots": ["MUO", "Muon"]}),
        ], extra_subtypes=("via_type",))
        ChronosGraphStore.from_connection(conn).apply_facts(
            [EdgeFact("holder:1", "page:muon", "contains", {})],
            source="default",
            strict_endpoints=True,
        )
        result = DeclarativeLinker(rules=[_via_derive_rule()]).enrich(
            conn, generation_id=2,
        )
        edges = _edges(conn)

    assert result.n_edges_emitted == 1
    assert ("holder:1", "hub:muon") in edges

def _dirty(*node_ids: str, attrs: dict[str, dict[str, frozenset[str]]]) -> IncrementalContext:
    return IncrementalContext(
        prev_fact_watermark=1,
        prev_edge_watermark=1,
        dirty_node_ids=frozenset(node_ids),
        dirty_node_attr_changes_by_subtype=attrs,
        is_first_run=False,
        last_status="success",
    )


def _seed_via_list_graph(conn: psycopg.Connection) -> None:
    """One holder contains one page whose title is a two-item list; each
    item names a different hub."""
    _seed(conn, [
        _node("holder:1", "src_type", {}),
        _node("page:two", "via_type", {
            "title": ["MuonPOG Twiki", "EgammaPOG Twiki"],
        }),
        _node("hub:muon", "dst_type", {"roots": ["MUO", "Muon"]}),
        _node("hub:egamma", "dst_type", {"roots": "Egamma"}),
    ], extra_subtypes=("via_type",))
    ChronosGraphStore.from_connection(conn).apply_facts(
        [EdgeFact("holder:1", "page:two", "contains", {})],
        source="default",
        strict_endpoints=True,
    )


_VIA_LIST_EDGES = {("holder:1", "hub:muon"), ("holder:1", "hub:egamma")}


def test_via_edge_derive_from_path_reads_every_item_of_a_via_list(
    clean_runtime: str,
) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_via_list_graph(conn)
        result = DeclarativeLinker(rules=[_via_derive_rule()]).enrich(
            conn, generation_id=2,
        )
        edges = _edges(conn)

    assert result.n_edges_emitted == 2
    assert edges == _VIA_LIST_EDGES


def test_incremental_via_list_dirty_destinations_backfill_every_item(
    clean_runtime: str,
) -> None:
    """Changed hubs look up via nodes by value. The page arrives once per
    list item; keeping only the last item's captures loses the muon link."""
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_via_list_graph(conn)
        result = DeclarativeLinker(rules=[_via_derive_rule()]).enrich(
            conn,
            generation_id=2,
            incremental=_dirty("hub:muon", "hub:egamma", attrs={
                "dst_type": {
                    "hub:muon": frozenset({"__new__", "roots"}),
                    "hub:egamma": frozenset({"__new__", "roots"}),
                },
            }),
        )
        edges = _edges(conn)

    assert result.n_edges_emitted == 2
    assert edges == _VIA_LIST_EDGES


def test_incremental_via_list_dirty_via_node_reads_every_item(
    clean_runtime: str,
) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_via_list_graph(conn)
        result = DeclarativeLinker(rules=[_via_derive_rule()]).enrich(
            conn,
            generation_id=2,
            incremental=_dirty("page:two", attrs={
                "via_type": {"page:two": frozenset({"__new__", "title"})},
            }),
        )
        edges = _edges(conn)

    assert result.n_edges_emitted == 2
    assert edges == _VIA_LIST_EDGES


def test_derive_from_path_reads_numbers_inside_a_list(
    clean_runtime: str,
) -> None:
    """A number in a list reads as its text, as a scalar number does."""
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed(conn, [
            _node("page:fix", "src_type", {"title": "#2921 fix"}),
            _node("hub:issues", "dst_type", {"roots": [17, 2921]}),
            _node("hub:other", "dst_type", {"roots": [18]}),
        ])
        result = DeclarativeLinker(
            rules=[_derive_rule(regex=r"^#(\d+)")],
        ).enrich(conn, generation_id=2)
        edges = _edges(conn)

    assert result.n_edges_emitted == 1
    assert edges == {("page:fix", "hub:issues")}
    assert result.incomplete is False


def test_derive_from_path_links_every_destination_sharing_a_value(
    clean_runtime: str,
) -> None:
    """Two hubs carry the capture; both link, as `exact` would do, instead
    of whichever row the database returned last."""
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed(conn, [
            _node("page:muon", "src_type", {"title": "MuonPOG Twiki"}),
            _node("hub:a", "dst_type", {"roots": ["Muon"]}),
            _node("hub:b", "dst_type", {"roots": "Muon"}),
        ])
        result = DeclarativeLinker(rules=[_derive_rule()]).enrich(
            conn, generation_id=2,
        )
        edges = _edges(conn)

    assert result.n_edges_emitted == 2
    assert edges == {("page:muon", "hub:a"), ("page:muon", "hub:b")}


@pytest.mark.parametrize(
    ("roots", "detail"),
    [
        (["Egamma", {"name": "Egamma"}], "a list holding a dict"),
        (["Egamma", ["Egamma"]], "a list holding a list"),
        ({"name": "Egamma"}, "is an object"),
    ],
    ids=["list_of_objects", "nested_list", "object"],
)
def test_derive_from_path_skips_and_records_a_field_it_cannot_read(
    clean_runtime: str,
    caplog: pytest.LogCaptureFixture,
    roots: object,
    detail: str,
) -> None:
    """The bad node is skipped for that rule only. The rule's good links and
    an unrelated rule in the same linker still land, and the run is marked
    incomplete with the registered code, the rule and the skip count."""
    unrelated = CrossLinkRule(
        id="unrelated_exact",
        src=RuleEndpoint(subtypes=("src_type",), field="key"),
        dst=RuleEndpoint(subtypes=("dst_type",), field="key"),
        matcher="exact",
        edge="references",
        bidirectional=False,
    )
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed(conn, [
            _node("page:muon", "src_type", {"title": "MuonPOG Twiki"}),
            _node("page:egamma", "src_type", {"title": "EgammaPOG Twiki"}),
            _node("hub:muon", "dst_type", {"roots": ["MUO", "Muon"]}),
            _node("hub:bad", "dst_type", {"roots": roots}),
            _node("src:k", "src_type", {"key": "k"}),
            _node("dst:k", "dst_type", {"key": "k"}),
        ])
        with caplog.at_level("WARNING"):
            result = DeclarativeLinker(
                rules=[_derive_rule(), unrelated],
            ).enrich(conn, generation_id=2)
        edges = _edges(conn)

    assert edges == {("page:muon", "hub:muon"), ("src:k", "dst:k")}
    assert result.n_edges_emitted == 2
    assert result.incomplete is True
    assert result.error is not None
    assert result.error.startswith("cross_link_field_shape_unsupported:")
    assert "'page_names_group_root' skipped 1 node(s)" in result.error
    assert "'hub:bad'" in result.error
    assert "unrelated_exact" not in result.error
    warnings = [
        rec.getMessage() for rec in caplog.records
        if "cross_link_field_shape_unsupported" in rec.getMessage()
    ]
    assert len(warnings) == 1
    assert "'page_names_group_root'" in warnings[0]
    assert "node 'hub:bad' field 'roots'" in warnings[0]
    assert detail in warnings[0]


# --- quarantine under the publisher (review of #2927) ----------------------
#
# A bad shape is deterministic, so holding the watermark would rescan and
# re-warn every cycle. Under the publisher the node is quarantined for that
# rule instead: the run stays complete and doctor reports the ledger row.

_DEPLOYMENT = "declarative-multivalue-test"


def _published_linker(*rules: CrossLinkRule) -> DeclarativeLinker:
    linker = DeclarativeLinker(rules=list(rules))
    linker._set_publisher_context(deployment_name=_DEPLOYMENT, branch_name="main")
    return linker


def _quarantine_rows(conn: psycopg.Connection) -> list[tuple[str, str, str]]:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT enricher_name, item_ref, status "
            "  FROM okg.enricher_input_quarantines "
            " WHERE deployment_name = %s ORDER BY item_ref, status",
            (_DEPLOYMENT,),
        )
        return [(str(a), str(b), str(c)) for a, b, c in cur.fetchall()]


def test_publisher_run_quarantines_a_bad_node_and_resolves_it_when_fixed(
    clean_runtime: str,
    caplog: pytest.LogCaptureFixture,
) -> None:
    from okg.substrate.cli.doctor import _enricher_quarantine_findings

    item_ref = '["derive_from_path","page_names_group_root","roots","hub:bad"]'
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed(conn, [
            _node("page:muon", "src_type", {"title": "MuonPOG Twiki"}),
            _node("page:egamma", "src_type", {"title": "EgammaPOG Twiki"}),
            _node("hub:muon", "dst_type", {"roots": ["MUO", "Muon"]}),
            _node("hub:bad", "dst_type", {"roots": ["Egamma", {"x": 1}]}),
        ])
        linker = _published_linker(_derive_rule())

        with caplog.at_level("WARNING"):
            first = linker.enrich(conn, generation_id=2)
        first_warnings = [
            rec.getMessage() for rec in caplog.records
            if "cross_link_field_shape_unsupported" in rec.getMessage()
        ]
        rows_after_first = _quarantine_rows(conn)
        findings = _enricher_quarantine_findings(conn, _DEPLOYMENT)

        caplog.clear()
        with caplog.at_level("WARNING"):
            second = linker.enrich(conn, generation_id=3)
        second_warnings = [
            rec.getMessage() for rec in caplog.records
            if "cross_link_field_shape_unsupported" in rec.getMessage()
        ]

        ChronosGraphStore.from_connection(conn).apply_facts(
            [NodeFact(
                "hub:bad", "dst_type", {"roots": ["Egamma"]},
                {"id": "hub:bad"}, {"v": 2},
            )],
            source="default",
            strict_endpoints=True,
        )
        third = linker.enrich(
            conn,
            generation_id=4,
            incremental=_dirty("hub:bad", attrs={
                "dst_type": {"hub:bad": frozenset({"roots"})},
            }),
        )
        rows_after_fix = _quarantine_rows(conn)
        findings_after_fix = _enricher_quarantine_findings(conn, _DEPLOYMENT)
        edges = _edges(conn)

    # The run is complete, so the publisher advances the watermark.
    assert first.incomplete is False
    assert first.error is None
    assert first.n_quarantined == 1
    assert first.n_edges_emitted == 1
    assert len(first_warnings) == 1
    assert rows_after_first == [("declarative", item_ref, "active")]
    assert len(findings) == 1
    assert findings[0]["enricher_name"] == "declarative"
    assert findings[0]["item_ref"] == item_ref
    assert findings[0]["failure_class"] == "cross_link_field_shape_unsupported"
    assert "'page_names_group_root'" in str(findings[0]["error"])
    assert "node 'hub:bad' field 'roots'" in str(findings[0]["error"])
    # A rescan of the same bad value adds no quarantine and no warning.
    assert second.incomplete is False
    assert second.n_quarantined == 0
    assert second_warnings == []
    # Fixing the node links it and clears the finding.
    assert third.incomplete is False
    assert third.n_quarantined == 0
    assert ("page:egamma", "hub:bad") in edges
    assert findings_after_fix == []
    assert {status for _e, ref, status in rows_after_fix if ref == item_ref} == {
        "resolved", "superseded",
    }


def test_shape_warnings_are_capped_per_rule(
    clean_runtime: str,
    caplog: pytest.LogCaptureFixture,
) -> None:
    bad = [
        _node(f"hub:bad{i:02d}", "dst_type", {"roots": {"n": i}})
        for i in range(23)
    ]
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed(conn, [
            _node("page:muon", "src_type", {"title": "MuonPOG Twiki"}),
            *bad,
        ])
        with caplog.at_level("WARNING"):
            result = DeclarativeLinker(rules=[_derive_rule()]).enrich(
                conn, generation_id=2,
            )
    warnings = [
        rec.getMessage() for rec in caplog.records
        if "cross_link_field_shape_unsupported" in rec.getMessage()
    ]

    assert len(warnings) == 21
    assert "23 node(s) skipped in total; 3 more suppressed" in warnings[-1]
    assert "skipped 23 node(s)" in str(result.error)


def test_incremental_via_numeric_list_skips_a_bad_destination(
    clean_runtime: str,
) -> None:
    """L1: a changed via node holds numbers; the value lookup finds a
    destination whose list holds a number and an object. That destination
    is skipped and recorded, not linked."""
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed(conn, [
            _node("holder:1", "src_type", {}),
            _node("page:nums", "via_type", {"title": [17, 2921]}),
            _node("hub:good", "dst_type", {"roots": [17]}),
            _node("hub:bad", "dst_type", {"roots": [2921, {"x": 1}]}),
        ], extra_subtypes=("via_type",))
        ChronosGraphStore.from_connection(conn).apply_facts(
            [EdgeFact("holder:1", "page:nums", "contains", {})],
            source="default",
            strict_endpoints=True,
        )
        result = DeclarativeLinker(
            rules=[_via_derive_rule(regex=r"^(\d+)$")],
        ).enrich(
            conn,
            generation_id=2,
            incremental=_dirty("page:nums", attrs={
                "via_type": {"page:nums": frozenset({"__new__", "title"})},
            }),
        )
        edges = _edges(conn)

    assert edges == {("holder:1", "hub:good")}
    assert result.incomplete is True
    assert "skipped 1 node(s)" in str(result.error)
    assert "'hub:bad'" in str(result.error)


def test_two_rules_reading_one_field_both_report_the_skip(
    clean_runtime: str,
) -> None:
    """L2: the second rule reads the destination field from the endpoint
    cache and must still report the bad node."""
    second = CrossLinkRule(
        id="page_names_group_root_again",
        src=RuleEndpoint(subtypes=("src_type",), field="title"),
        dst=_derive_rule().dst,
        matcher="derive_from_path",
        edge="references",
        bidirectional=False,
    )
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed(conn, [
            _node("page:muon", "src_type", {"title": "MuonPOG Twiki"}),
            _node("hub:muon", "dst_type", {"roots": "Muon"}),
            _node("hub:bad", "dst_type", {"roots": {"x": 1}}),
        ])
        result = DeclarativeLinker(rules=[_derive_rule(), second]).enrich(
            conn, generation_id=2,
        )

    assert "'page_names_group_root' skipped 1 node(s)" in str(result.error)
    assert "'page_names_group_root_again' skipped 1 node(s)" in str(result.error)


def test_a_node_read_twice_by_one_rule_counts_once(
    clean_runtime: str,
) -> None:
    """L3: with the source and the bad destination both changed, the rule
    reads the destination once per direction; it is one skipped node."""
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed(conn, [
            _node("page:muon", "src_type", {"title": "MuonPOG Twiki"}),
            _node("hub:muon", "dst_type", {"roots": "Muon"}),
            _node("hub:bad", "dst_type", {"roots": [{"x": "Muon"}]}),
        ])
        result = DeclarativeLinker(rules=[_derive_rule()]).enrich(
            conn,
            generation_id=2,
            incremental=_dirty("page:muon", "hub:bad", attrs={
                "src_type": {"page:muon": frozenset({"__new__", "title"})},
                "dst_type": {"hub:bad": frozenset({"__new__", "roots"})},
            }),
        )

    assert "skipped 1 node(s)" in str(result.error)
    assert "'hub:bad'" in str(result.error)


_RESOLVE_READ = (
    "SELECT node_id, attrs FROM okg.graph_nodes WHERE node_id = ANY(:node_ids)"
)


def test_failed_graph_read_leaves_quarantines_active(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """F1: the resolve pass reads the quarantined nodes. A failed read must
    not look like "every node is gone": the run fails and the row stays
    active."""
    from okg.substrate.graph_store import ChronosGraphStore as Store

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed(conn, [
            _node("page:muon", "src_type", {"title": "MuonPOG Twiki"}),
            _node("hub:bad", "dst_type", {"roots": {"x": 1}}),
        ])
        linker = _published_linker(_derive_rule())
        first = linker.enrich(conn, generation_id=2)

        original_checkout = Store.checkout

        def failing_checkout(self, *args, **kwargs):
            checkout = original_checkout(self, *args, **kwargs)

            class _Failing:
                def query(self, sql, params=None):
                    if " ".join(sql.split()) == _RESOLVE_READ:
                        raise RuntimeError("graph read failed")
                    return checkout.query(sql, params)

                def __getattr__(self, name):
                    return getattr(checkout, name)

            return _Failing()

        monkeypatch.setattr(Store, "checkout", failing_checkout)
        with pytest.raises(RuntimeError, match="graph read failed"):
            linker.enrich(conn, generation_id=3)
        monkeypatch.undo()
        rows = _quarantine_rows(conn)

    assert first.n_quarantined == 1
    assert rows == [(
        "declarative",
        '["derive_from_path","page_names_group_root","roots","hub:bad"]',
        "active",
    )]


def test_quarantine_reference_round_trips_slashes(
    clean_runtime: str,
) -> None:
    """F2: a '/' in the rule id and in the field must not change which
    node the resolve pass re-reads; the row stays active until the node is
    fixed, then resolves."""
    rule = CrossLinkRule(
        id="pog/rule",
        src=RuleEndpoint(subtypes=("src_type",), field="title"),
        dst=RuleEndpoint(
            subtypes=("dst_type",),
            field="ro/ots",
            derive_from_path=DeriveFromPath(
                attr="title",
                regex=r"^([A-Za-z]+)POG\b",
                capture_field="ro/ots",
            ),
        ),
        matcher="derive_from_path",
        edge="references",
        bidirectional=False,
    )
    item_ref = '["derive_from_path","pog/rule","ro/ots","hub/bad"]'
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed(conn, [
            _node("page:muon", "src_type", {"title": "MuonPOG Twiki"}),
            _node("hub/bad", "dst_type", {"ro/ots": [{"x": "Muon"}]}),
        ])
        linker = _published_linker(rule)
        first = linker.enrich(conn, generation_id=2)
        after_first = _quarantine_rows(conn)
        second = linker.enrich(conn, generation_id=3)
        after_second = _quarantine_rows(conn)
        ChronosGraphStore.from_connection(conn).apply_facts(
            [NodeFact(
                "hub/bad", "dst_type", {"ro/ots": ["Muon"]},
                {"id": "hub/bad"}, {"v": 2},
            )],
            source="default",
            strict_endpoints=True,
        )
        linker.enrich(conn, generation_id=4)
        after_fix = _quarantine_rows(conn)
        edges = _edges(conn)

    assert first.n_quarantined == 1
    assert after_first == [("declarative", item_ref, "active")]
    assert second.n_quarantined == 0
    assert after_second == [("declarative", item_ref, "active")]
    assert {status for _e, ref, status in after_fix if ref == item_ref} == {
        "resolved", "superseded",
    }
    assert ("page:muon", "hub/bad") in edges
