"""Catalog-alias promotion: an exact alias ranks its canonical first (#797).

Scope note: this reorders, it does not merge duplicates. Alias-like *nodes* still
rank on their own text, and resolution reads `okg.aliases` only — node attributes
named `canonical` / `alias_of` are never consulted.

Every test here publishes a competing match, so a test cannot pass merely because
the promoted row was the only row.
"""
from __future__ import annotations

import psycopg
import pytest

from okg.substrate.mcp import open_session, search
from tests.substrate.mcp.search_fixture import (
    make_search_deployment,
    node,
    publish_nodes,
)


pytestmark = pytest.mark.postgres


def test_catalog_alias_promotes_canonical_over_competing_matches(
    clean_runtime: str,
    tmp_path,
) -> None:
    deployment = make_search_deployment(tmp_path, dsn=clean_runtime)
    publish_nodes(
        clean_runtime,
        deployment,
        subtypes=["concept"],
        aliases=[("JES", "concept:jes", None)],
        nodes=[
            # The canonical. Its own text also matches the query, so it is a
            # genuine BM25 candidate and promotion must not duplicate it.
            node("concept:jes", "concept", "JES jet energy scale systematic"),
            # Competitors that BM25 scores for the same query.
            node("concept:decoy-a", "concept", "JES appears here too"),
            node("concept:decoy-b", "concept", "another JES mention entirely"),
        ],
    )
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        session = open_session(conn, deployment_name=deployment)
        result = search(conn, session, "JES", method="lexical", completeness="exact")

    rows = result["results"]
    node_ids = [row["node_id"] for row in rows]
    # The competitors really were matched, so first place is contested.
    assert "concept:decoy-a" in node_ids
    assert "concept:decoy-b" in node_ids
    assert rows[0]["node_id"] == "concept:jes"
    assert rows[0]["canonical_node_id"] == "concept:jes"
    assert rows[0]["match_kind"] == "catalog_alias"
    assert rows[0]["method"] == "lexical"
    # Promoted exactly once, despite also being a lexical candidate.
    assert node_ids.count("concept:jes") == 1
    assert result["total_count"] == len(node_ids)


def test_scoped_alias_does_not_promote_into_unscoped_search(
    clean_runtime: str,
    tmp_path,
) -> None:
    """A subtype-scoped alias is invisible to an unscoped query.

    `method="alias"` already enforces this; lexical promotion must agree, or a
    scoped alias silently outranks everything in an unrelated search.
    """
    deployment = make_search_deployment(tmp_path, dsn=clean_runtime)
    publish_nodes(
        clean_runtime,
        deployment,
        subtypes=["concept", "library"],
        aliases=[("ORM", "concept:sqlalchemy", "library")],
        nodes=[
            node("concept:sqlalchemy", "concept", "sqlalchemy toolkit"),
            node("concept:orm-notes", "concept", "ORM mapping notes"),
        ],
    )
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        session = open_session(conn, deployment_name=deployment)
        result = search(conn, session, "ORM", method="lexical")

    rows = result["results"]
    assert rows, "the unscoped query should still return its lexical match"
    assert rows[0]["node_id"] == "concept:orm-notes"
    assert rows[0]["match_kind"] == "lexical"
    assert all(row["match_kind"] != "catalog_alias" for row in rows)


def test_attribute_named_canonical_never_removes_a_row(
    clean_runtime: str,
    tmp_path,
) -> None:
    """Resolution reads the alias catalog, never node attributes.

    The attribute target is published as a real node, so an implementation that
    folded on attribute names would successfully hydrate it and drop the original
    row — this test fails in that case rather than passing vacuously.
    """
    deployment = make_search_deployment(tmp_path, dsn=clean_runtime)
    publish_nodes(
        clean_runtime,
        deployment,
        subtypes=["concept"],
        nodes=[
            node(
                "concept:calibration-notes",
                "concept",
                "hash-like canonical calibration object",
                canonical="concept:other-target",
            ),
            node("concept:other-target", "concept", "unrelated target text"),
        ],
    )
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        session = open_session(conn, deployment_name=deployment)
        result = search(
            conn, session, "hash-like canonical calibration", method="lexical",
        )

    rows = result["results"]
    assert [row["node_id"] for row in rows] == ["concept:calibration-notes"]
    # No promotion happened: the row is reported as an ordinary lexical match.
    assert rows[0]["match_kind"] == "lexical"
    assert rows[0]["canonical_node_id"] == "concept:calibration-notes"


def test_promotion_does_not_flip_rank_capped(
    clean_runtime: str,
    tmp_path,
) -> None:
    """Injecting a row must not claim BM25 truncated relevance when it did not."""
    deployment = make_search_deployment(tmp_path, dsn=clean_runtime)
    publish_nodes(
        clean_runtime,
        deployment,
        subtypes=["concept"],
        aliases=[("JES", "concept:jes", None)],
        nodes=[
            node("concept:jes", "concept", "jet energy scale systematic"),
            node("concept:decoy", "concept", "JES mention"),
        ],
    )
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        session = open_session(conn, deployment_name=deployment)
        result = search(conn, session, "JES", method="lexical")

    assert result["rank_capped"] is False


def _code_symbol_alias_fields(tmp_path) -> list[str]:
    """The CodeSymbol alias fields the shipped `code_graph` schema declares,
    parsed the way catalog composition parses them."""
    from pathlib import Path

    import yaml

    import okg.substrate.library.ontologies as ontologies
    from okg.substrate.catalog.modules_compose import _split_subtypes_field

    schema = yaml.safe_load(
        (Path(ontologies.__file__).parent / "code_graph" / "schema.yaml").read_text(),
    )
    annotations = schema["classes"]["CodeSymbol"]["annotations"]
    return list(_split_subtypes_field(annotations["okg_alias_fields"]))


def _code_symbol_facts(root, repo: str, files: dict[str, str]):
    from okg.substrate.library.sources.base import NodeFact
    from okg.substrate.library.sources.code_graph import CodeStructureSource

    for rel, text in files.items():
        target = root / rel
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(text)
    return [
        f for f in CodeStructureSource(repo=repo, repo_root=root).run("extract").facts
        if isinstance(f, NodeFact) and f.subtype == "code_symbol"
    ]


def test_same_module_path_files_seed_distinct_scoped_aliases(clean_runtime: str, tmp_path) -> None:
    """Issue #1747: two files with one module path no longer collide.

    `pkg/util.ts` and `pkg/util.js` both define `helper` and both have the
    module name `pkg.util`. Keyed by bare `qualified_name` those values collide
    and seed for no node; keyed by `scoped_qualified_name` every node gets one
    alias of its own. A genuine collision on the scoped key still quarantines.
    """
    from dataclasses import replace

    from okg.substrate.catalog.aliases import seed_from_subtype
    from okg.substrate.graph_store import ChronosGraphStore
    from tests.pytest.catalog_fixture_builders import seed_catalog

    fields = _code_symbol_alias_fields(tmp_path)
    assert fields == ["scoped_qualified_name", "symbol_id", "path"]
    facts = _code_symbol_facts(tmp_path / "corpus", "demo", {
        "pkg/util.ts": "export function helper() { return 1; }\n",
        "pkg/util.js": "function helper() { return 2; }\n",
    })
    assert len(facts) == 4
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        cv = seed_catalog(conn, subtypes=["code_symbol"])
        ChronosGraphStore.from_connection(conn).apply_facts(facts, source="fixture.scoped-aliases")
        bare = seed_from_subtype(conn, subtype="code_symbol", field="qualified_name", apply=False)
        # The old key: `helper` and `pkg.util` each map to two nodes.
        assert bare.collision_count == 2
        scoped = seed_from_subtype(conn, subtype="code_symbol", field=fields[0], apply=True)
        assert scoped.collision_count == 0
        assert scoped.quarantined_count == 0
        assert scoped.inserted_count == 4
        with conn.cursor() as cur:
            cur.execute(
                "SELECT canonical, alias FROM okg.aliases WHERE catalog_version_id = %s",
                (cv,),
            )
            aliases = cur.fetchall()
        assert sorted(aliases) == sorted(
            (f.node_id, f.attrs["scoped_qualified_name"]) for f in facts
        )
        # A genuine collision: a second node claims an existing scoped name.
        helper = next(f for f in facts if f.attrs["scoped_qualified_name"] == "demo:pkg/util.ts::helper")
        impostor = replace(helper, node_id=helper.node_id + ":impostor", source_record_id={"id": "impostor"})
        ChronosGraphStore.from_connection(conn).apply_facts([impostor], source="fixture.scoped-aliases")
        collided = seed_from_subtype(conn, subtype="code_symbol", field=fields[0], apply=True)
        assert collided.collision_count == 1
        assert collided.quarantined_count == 1
        assert collided.sample_quarantined == [("demo:pkg/util.ts::helper", helper.node_id)]


def _mention_link_graph(clean_runtime: str, tmp_path, *, extra_symbols=(), extra_mentions=()):
    """Publish code symbols and document mentions; seed the scoped aliases."""
    from okg.substrate.catalog.aliases import seed_from_subtype
    from okg.substrate.graph_store import ChronosGraphStore
    from okg.substrate.library.sources.base import NodeFact
    from tests.pytest.catalog_fixture_builders import SubtypeSpec, seed_catalog
    from tests.substrate._db_helpers import ensure_published_generation

    fields = _code_symbol_alias_fields(tmp_path)
    facts = [
        *_code_symbol_facts(tmp_path / "corpus", "demo", {
            "a.py": "def unique_helper(): pass\n",
            "b.py": "def shared_helper(): pass\n",
            "c.py": "def shared_helper(): pass\n",
        }),
        *extra_symbols,
    ]
    mentions = [
        NodeFact(f"mention:{value}", "entity_mention", {"entity_value": value},
                 {"id": f"mention:{value}"}, {"v": 1})
        for value in ("unique_helper", "shared_helper", "demo:b.py::shared_helper", *extra_mentions)
    ]
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        cv = seed_catalog(
            conn,
            subtypes=[
                SubtypeSpec("code_symbol", metadata={"alias_fields": fields}),
                "entity_mention",
            ],
            narrowings=[
                ("entity_mention", "mentions", "code_symbol"),
                ("code_symbol", "mentions", "entity_mention"),
            ],
        )
        ensure_published_generation(conn, 1, catalog_version_id=cv)
        ChronosGraphStore.from_connection(conn).apply_facts(
            [*facts, *mentions], source="default", strict_endpoints=True,
        )
        seeded = seed_from_subtype(conn, subtype="code_symbol", field=fields[0], apply=True)
        assert seeded.collision_count == 0
    return {f.attrs["scoped_qualified_name"]: f.node_id for f in facts}


def _mention_links(conn) -> set[tuple[str, str]]:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT src, dst FROM okg.edge_facts "
            "WHERE edge_type = 'mentions' AND src LIKE 'mention:%%'"
        )
        return {(str(src), str(dst)) for src, dst in cur.fetchall()}


def _shipped_mention_linker():
    from pathlib import Path

    from okg.substrate.library.linkers.declarative import DeclarativeLinker
    from okg.substrate.library.linkers.rules import load_rules_file

    import okg.substrate.library.ontologies as ontologies

    rule_file = Path(ontologies.__file__).parent / "extraction" / "cross_links" / "mentions_to_canonical.yaml"
    return DeclarativeLinker(rules=load_rules_file(rule_file))


def test_bare_symbol_mention_links_only_when_unique(clean_runtime: str, tmp_path) -> None:
    """Operator decision on #1747 (option 2): never pick among several symbols."""
    ids = _mention_link_graph(clean_runtime, tmp_path)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        result = _shipped_mention_linker().enrich(conn, generation_id=2)
        links = _mention_links(conn)

    assert links == {
        # Unique bare name: exactly one code symbol is `unique_helper`.
        ("mention:unique_helper", ids["demo:a.py::unique_helper"]),
        # A scoped name resolves through the alias table.
        ("mention:demo:b.py::shared_helper", ids["demo:b.py::shared_helper"]),
    }
    # `shared_helper` names two symbols, so that mention links to neither.
    assert result.n_edges_emitted == 4  # two links, each with its reverse edge


def test_dirty_symbol_links_existing_unique_mention_but_not_ambiguous_one(
    clean_runtime: str, tmp_path,
) -> None:
    from okg.substrate.enrichers.base import IncrementalContext
    from okg.substrate.graph_store import ChronosGraphStore
    from okg.substrate.library.sources.base import NodeFact

    ids = _mention_link_graph(clean_runtime, tmp_path, extra_mentions=("late_helper", "twin"))
    late = [
        NodeFact(node_id, "code_symbol", {
            "qualified_name": name, "scoped_qualified_name": f"late:{node_id}",
        }, {"id": node_id}, {"v": 1})
        for node_id, name in (("symbol:late", "late_helper"), ("symbol:twin1", "twin"), ("symbol:twin2", "twin"))
    ]
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        ChronosGraphStore.from_connection(conn).apply_facts(late, source="default", strict_endpoints=True)
        _shipped_mention_linker().enrich(
            conn,
            generation_id=2,
            incremental=IncrementalContext(
                prev_fact_watermark=1,
                prev_edge_watermark=1,
                dirty_node_ids=frozenset(f.node_id for f in late),
                is_first_run=False,
                last_status="success",
            ),
        )
        links = _mention_links(conn)

    assert ("mention:late_helper", "symbol:late") in links
    assert not {src for src, _dst in links} & {"mention:twin", "mention:shared_helper"}
    assert ids  # the base graph was built


def test_publish_of_only_a_new_unique_symbol_links_existing_mention(
    clean_runtime: str, tmp_path,
) -> None:
    """The okg-workspace linker, driven by the publisher's own change scoping.

    `declarative_alias` runs the mentions rule alone and may skip a clean
    publish. The second publish adds one code symbol and nothing else; the
    publisher must still hand it to the linker, which links the mention of
    its unique name that was published a cycle earlier.
    """
    from okg.substrate.graph_store import ChronosGraphStore
    from okg.substrate.library.linkers.declarative import DeclarativeLinker
    from okg.substrate.library.sources.base import NodeFact
    from okg.substrate.publisher import Publisher
    from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer

    ids = _mention_link_graph(clean_runtime, tmp_path, extra_mentions=("late_helper",))
    # The okg-workspace deployment.yaml configuration of this linker.
    linker = DeclarativeLinker(
        name="declarative_alias",
        rules=_shipped_mention_linker().rules,
        include_rule_ids=["entity_mention_resolves_via_alias_table"],
        publisher_skip_when_clean=True,
        publisher_skip_alias_table_when_clean=True,
    )
    publisher = Publisher(
        clean_runtime,
        "mention-link",
        enrichers=[linker],
        catalog_identity=bind_test_catalog_writer(clean_runtime, "mention-link"),
    )
    first = publisher.cycle()
    assert first["status"] == "published", first
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        before = _mention_links(conn)
    assert ("mention:unique_helper", ids["demo:a.py::unique_helper"]) in before
    assert not {src for src, _dst in before} & {"mention:late_helper"}

    late = NodeFact("symbol:late", "code_symbol", {
        "qualified_name": "late_helper", "scoped_qualified_name": "late:late.py::late_helper",
    }, {"id": "symbol:late"}, {"v": 1})
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        ChronosGraphStore.from_connection(conn).apply_facts([late], source="default", strict_endpoints=True)
    second = publisher.cycle()
    assert second["status"] == "published", second
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        after = _mention_links(conn)

    assert after - before == {("mention:late_helper", "symbol:late")}


def test_fallback_lookup_binds_more_values_than_postgres_parameters(
    clean_runtime: str,
) -> None:
    """Postgres takes at most 65,535 bind parameters per statement.

    The lookup used to bind one parameter per value; past that limit the
    query failed, the failure read as "no node matches", and the fallback
    silently linked nothing. The values now bind as one array.
    """
    from okg.substrate.graph_store import ChronosGraphStore
    from okg.substrate.library.linkers.declarative import DeclarativeLinker
    from okg.substrate.library.sources.base import NodeFact
    from tests.pytest.catalog_fixture_builders import seed_catalog
    from tests.substrate._db_helpers import ensure_published_generation

    symbol = NodeFact("symbol:needle", "code_symbol", {"qualified_name": "needle"},
                      {"id": "symbol:needle"}, {"v": 1})
    values = {f"unresolved_{i:05d}" for i in range(70_000)} | {"needle"}
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        cv = seed_catalog(conn, subtypes=["code_symbol"])
        ensure_published_generation(conn, 1, deployment_name="bind-limit", catalog_version_id=cv)
        ChronosGraphStore.from_connection(conn).apply_facts([symbol], source="default")
        with conn.cursor() as cur:
            rows = DeclarativeLinker._fetch_subtype_field_matching_values(
                cur,
                subtypes=("code_symbol",),
                field="qualified_name",
                values=values,
            )

    assert rows == [("symbol:needle", "needle")]


def test_fallback_lookup_error_raises_instead_of_linking_nothing(
    clean_runtime: str, monkeypatch,
) -> None:
    """A failed fallback lookup must fail the linker, not read as "no match".

    The shared graph-read wrapper returns no rows on any error. The fallback
    uses the strict reader instead; the stand-in below replaces only that
    reader, so only the fallback's request can surface.
    """
    from okg.substrate.library.linkers import _chronos
    from okg.substrate.library.linkers.declarative import DeclarativeLinker
    from tests.pytest.catalog_fixture_builders import seed_catalog

    class LookupFailed(RuntimeError):
        pass

    def failing_query_strict(cur, sql, params=None):
        raise LookupFailed("graph read failed")

    rule = _shipped_mention_linker().rules[0]
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        cv = seed_catalog(conn, subtypes=["code_symbol", "entity_mention"])
        # The wrapper itself: an error stays silent by default and raises
        # when asked.
        with conn.cursor() as cur:
            assert _chronos.query(cur, "SELECT no_such_column FROM okg.graph_nodes") == []
            with pytest.raises(psycopg.Error):
                _chronos.query_strict(cur, "SELECT no_such_column FROM okg.graph_nodes")
        monkeypatch.setattr(_chronos, "query_strict", failing_query_strict)
        with conn.cursor() as cur, pytest.raises(LookupFailed):
            DeclarativeLinker._add_unique_fallback_targets(
                cur,
                rule,
                rule.dst.unique_field_fallback,
                cv=cv,
                srcs=[("mention:needle", "needle")],
                alias_to_canon={},
                dirty_node_ids=None,
                endpoint_cache={},
            )


def test_scoped_code_names_restore_12604_aliases(clean_runtime: str, tmp_path) -> None:
    """Scale fixture for #1362 and #1747; not a new production measurement.

    The same 6,302 function names appear in a live file and in two archives.
    Keyed by bare `qualified_name` (the pre-#1747 alias field) they collide and
    the 276 earlier winners are quarantined. Keyed by `scoped_qualified_name`,
    every one of the 12,604 symbols seeds its own alias and none collide.
    """
    from dataclasses import replace

    from okg.substrate.catalog.aliases import seed_from_subtype
    from okg.substrate.graph_store import ChronosGraphStore
    from okg.substrate.library.sources.base import NodeFact
    from okg.substrate.library.sources.code_graph import CodeStructureSource
    from okg.substrate.library.sources._code_graph_common import _lexical_symbol_name
    from okg.substrate.publisher import Publisher
    from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer, seed_catalog

    alias_field = _code_symbol_alias_fields(tmp_path)[0]
    assert alias_field == "scoped_qualified_name"
    names = [f"ArchivedAlias{i:04d}" for i in range(6302)]
    paths = {
        "src/live.py": names,
        "archive/ui/legacy.py": names[:3151],
        "pact/archive/proposal/mock.py": names[3151:],
    }
    for path, symbols in paths.items():
        target = tmp_path / "corpus" / path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text("\n".join(f"def {name}(): pass" for name in symbols))
    source = CodeStructureSource(repo="alias-proof", repo_root=tmp_path / "corpus")
    facts = [
        f for f in source.run("extract").facts
        if isinstance(f, NodeFact) and f.subtype == "code_symbol" and f.attrs["kind"] != "module"
    ]
    assert len(facts) == 12604
    live = {f.attrs["qualified_name"]: f.node_id for f in facts if f.attrs["path"] == "src/live.py"}
    assert set(live) == set(names)
    scoped = {f.attrs[alias_field]: f.node_id for f in facts}
    assert len(scoped) == 12604
    # Reconstruct the pre-#1362/#1747 emission on the same stable node
    # identities: bare names everywhere and no scoped name.
    original = [replace(f, attrs={
        **{k: v for k, v in f.attrs.items() if k != alias_field},
        "qualified_name": _lexical_symbol_name("alias-proof", f.attrs["path"], f.attrs["qualified_name"]),
    }) for f in facts]
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog(conn, subtypes=["code_symbol"], aliases=[
            (name, live[name], None) for name in names[:276]
        ])
        store = ChronosGraphStore.from_connection(conn)
        store.apply_facts(original, source="fixture.archive-aliases")
    publisher = Publisher(clean_runtime, "archive-alias-proof", catalog_identity=bind_test_catalog_writer(clean_runtime, "archive-alias-proof"))
    before_publish = publisher.cycle()
    assert before_publish["status"] == "published", before_publish
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        before = seed_from_subtype(conn, subtype="code_symbol", field="qualified_name", apply=True)
        assert before.collision_count == 6302
        assert before.quarantined_count == 276
        store = ChronosGraphStore.from_connection(conn)
        store.apply_facts(facts, source="fixture.archive-aliases")
    after_publish = publisher.cycle()
    assert after_publish["status"] == "published", after_publish
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        after = seed_from_subtype(conn, subtype="code_symbol", field=alias_field, apply=True)
        assert after.collision_count == 0
        assert after.quarantined_count == 0
        assert after.inserted_count == 12604
        with conn.cursor() as cur:
            cur.execute("SELECT alias, canonical FROM okg.aliases WHERE catalog_version_id = %s AND alias = ANY(%s)", (after.catalog_version_id, list(scoped)))
            assert dict(cur.fetchall()) == scoped
            cur.execute("SELECT count(*) FROM okg.alias_collisions WHERE catalog_version_id = %s AND subtype = 'code_symbol' AND field = %s", (after.catalog_version_id, alias_field))
            assert cur.fetchone()[0] == 0
        session = open_session(conn, deployment_name="archive-alias-proof")
        live_key = f"alias-proof:src/live.py::{names[0]}"
        result = search(conn, session, live_key, method="alias")
        assert [row["canonical"] for row in result["results"]] == [live[names[0]]]
        # The bare name is no alias: it names three symbols, and the alias
        # table never picks one of them.
        bare = search(conn, session, names[0], method="alias")
        assert bare["results"] == []
