"""Tests for ``af_pubs.supersedure_health`` — Stage 3.

Two layers:

* **Unit / planner tests** (no DB) — synthesize the corpus by
  hand from ``Anchor.from_content`` calls so the planner's rule
  logic is exercised in isolation. Fast, hermetic, no fixture
  setup.
* **Integration test** (Postgres-backed) — seed a tiny fixture
  dataset into the Chronos graph (``okg.graph_nodes`` /
  ``okg.graph_edges`` via ``ChronosGraphStore.apply_facts``) and
  checkpoint the published generation, build the corpus through
  ``build_corpus`` (which goes through ``corpus_from_okg`` +
  ``edges_as_anchors`` over a generation checkout), run the planner,
  and assert end-to-end behavior.

The exact-count gate against the live af_pubs snapshot
(R1=627, R2=620, R3=7, R4=12, dangling=0) is Stage 5 of
add-incremental-views, not this file.
"""
from __future__ import annotations

import json

import psycopg
import pytest

pytest.importorskip("views.builders")
pytest.importorskip("views.supersedure_health")
# Imports from the af_pubs deployment use top-level package
# names because tests/views/conftest.py adds
# deployments/af_pubs/ to sys.path (matching the deployment
# convention documented in history/openspec/project.md).
from views.builders import (
    parse_catalog_attrs,
)
from views.supersedure_health import (
    VIEW_DEF_HASH,
    VIEW_ID,
    _edge_finding_id,
    _pub_finding_id,
    build_corpus,
    supersedure_health_assembler,
    supersedure_health_planner,
    supersedure_health_view_def,
)
from okg.substrate.graph_store import ChronosGraphStore, _edge_id
from okg.substrate.library.sources.base import EdgeFact, NodeFact
from okg.views.execution.llm import DeterministicStubLLM
from okg.views.refresh import fresh, refresh
from okg.views.types import Anchor


# ---------------------------------------------------------------------------
# Synthetic-corpus helpers (no DB)
# ---------------------------------------------------------------------------


def _pub_anchor(
    pub_id: str,
    *,
    pub_number: str,
    status: str,
    certified_date: str = "",
    rescinded_date: str = "",
    title: str = "",
    opr: str = "AF/A3",
) -> Anchor:
    """Build a catalog anchor in the SAME format as
    catalog_anchor_builder, so the planner's parser sees identical
    text in both unit and integration tests.
    """
    fields = (
        ("pub_number", pub_number),
        ("title", title),
        ("status", status),
        ("certified_date", certified_date),
        ("rescinded_date", rescinded_date),
        ("opr", opr),
        ("pdf_url", ""),
    )
    content = "\n".join(f"{k}: {v}" for k, v in fields)
    return Anchor.from_content(id=f"{pub_id}:catalog", content=content)


def _supersedes_edge_anchor(
    edge_id: int,
    *,
    src: str,
    dst: str,
    attrs: dict | None = None,
) -> Anchor:
    content = json.dumps({
        "edge_id": edge_id,
        "src": src,
        "dst": dst,
        "attrs": attrs or {},
    }, sort_keys=True)
    return Anchor.from_content(
        id=f"edge:supersedes:{edge_id}", content=content,
    )


def _corpus(*anchors: Anchor) -> dict[str, Anchor]:
    return {a.id: a for a in anchors}


# ---------------------------------------------------------------------------
# parse_catalog_attrs
# ---------------------------------------------------------------------------


class TestParseCatalogAttrs:
    def test_round_trip_via_builder_format(self):
        a = _pub_anchor(
            "pub:afi10-1001",
            pub_number="AFI10-1001",
            status="current",
            certified_date="2020-01-15",
            title="Air Operations",
        )
        attrs = parse_catalog_attrs(a.content)
        assert attrs["pub_number"] == "AFI10-1001"
        assert attrs["status"] == "current"
        assert attrs["certified_date"] == "2020-01-15"
        assert attrs["title"] == "Air Operations"

    def test_lines_without_colon_are_ignored(self):
        out = parse_catalog_attrs("pub_number: A\n\nrandom\nstatus: current")
        assert out == {"pub_number": "A", "status": "current"}


# ---------------------------------------------------------------------------
# Finding-id helpers
# ---------------------------------------------------------------------------


class TestFindingIds:
    def test_edge_finding_id_is_deterministic(self):
        a = _edge_finding_id("R1", "pub:a", "pub:b")
        b = _edge_finding_id("R1", "pub:a", "pub:b")
        assert a == b
        assert len(a) == 16

    def test_edge_finding_id_distinguishes_rule_and_endpoints(self):
        ids = {
            _edge_finding_id("R1", "pub:a", "pub:b"),
            _edge_finding_id("R2", "pub:a", "pub:b"),
            _edge_finding_id("R1", "pub:a", "pub:c"),
            _edge_finding_id("R1", "pub:c", "pub:b"),
        }
        assert len(ids) == 4

    def test_pub_finding_id_distinguishes_pubs(self):
        assert (
            _pub_finding_id("pub:a") != _pub_finding_id("pub:b")
        )


# ---------------------------------------------------------------------------
# Planner — rule logic
# ---------------------------------------------------------------------------


class TestPlannerRules:
    def test_R1_superseded_but_current(self):
        # supersedes edge dst is still current → R1 fires.
        corpus = _corpus(
            _pub_anchor("pub:a", pub_number="A", status="rescinded"),
            _pub_anchor("pub:b", pub_number="B", status="current"),
            _supersedes_edge_anchor(1, src="pub:a", dst="pub:b"),
        )
        specs = supersedure_health_planner(corpus, supersedure_health_view_def())
        rule_ids = {s.id for s in specs}
        fid = _edge_finding_id("R1", "pub:a", "pub:b")
        # 3 fragments per edge-based finding.
        assert f"finding:R1:{fid}:src" in rule_ids
        assert f"finding:R1:{fid}:dst" in rule_ids
        assert f"finding:R1:{fid}:analysis" in rule_ids
        # R2 / R3 should NOT fire — src is rescinded, dst current
        # ⇒ R2 also fires (it's a stricter form of R1).
        # R3 would need both current.
        # Wait: R2 = (src rescinded AND dst current). That MATCHES.
        # So R2 should fire. R3 needs both current — does not.
        fid_r2 = _edge_finding_id("R2", "pub:a", "pub:b")
        assert f"finding:R2:{fid_r2}:src" in rule_ids
        # R3 must not appear.
        assert not any(s.id.startswith("finding:R3:") for s in specs)

    def test_R2_rescinded_supersedes_current(self):
        # Same as R1 above — R2 fires when src=rescinded, dst=current.
        corpus = _corpus(
            _pub_anchor("pub:a", pub_number="A", status="rescinded"),
            _pub_anchor("pub:b", pub_number="B", status="current"),
            _supersedes_edge_anchor(1, src="pub:a", dst="pub:b"),
        )
        specs = supersedure_health_planner(corpus, supersedure_health_view_def())
        fid = _edge_finding_id("R2", "pub:a", "pub:b")
        assert any(s.id == f"finding:R2:{fid}:analysis" for s in specs)

    def test_R3_two_currents_on_edge(self):
        corpus = _corpus(
            _pub_anchor("pub:a", pub_number="A", status="current"),
            _pub_anchor("pub:b", pub_number="B", status="current"),
            _supersedes_edge_anchor(1, src="pub:a", dst="pub:b"),
        )
        specs = supersedure_health_planner(corpus, supersedure_health_view_def())
        rule_ids = {s.id for s in specs}
        fid_r1 = _edge_finding_id("R1", "pub:a", "pub:b")
        fid_r3 = _edge_finding_id("R3", "pub:a", "pub:b")
        assert f"finding:R1:{fid_r1}:analysis" in rule_ids  # also R1
        assert f"finding:R3:{fid_r3}:analysis" in rule_ids
        # R2 needs src rescinded — not the case here.
        assert not any(s.id.startswith("finding:R2:") for s in specs)

    def test_R4_date_inversion(self):
        corpus = _corpus(
            _pub_anchor(
                "pub:a", pub_number="A", status="rescinded",
                certified_date="2020-05-01",
                rescinded_date="2019-12-01",  # before certified
            ),
            _pub_anchor("pub:b", pub_number="B", status="current"),
        )
        specs = supersedure_health_planner(corpus, supersedure_health_view_def())
        fid = _pub_finding_id("pub:a")
        rule_ids = {s.id for s in specs}
        # 2 fragments per single-pub finding (pub + analysis).
        assert f"finding:R4:{fid}:pub" in rule_ids
        assert f"finding:R4:{fid}:analysis" in rule_ids
        # Pub:b has consistent dates → no R4.
        assert not any(
            s.id == f"finding:R4:{_pub_finding_id('pub:b')}:pub"
            for s in specs
        )

    def test_R4_skipped_when_dates_consistent(self):
        corpus = _corpus(
            _pub_anchor(
                "pub:a", pub_number="A", status="rescinded",
                certified_date="2019-01-01",
                rescinded_date="2020-12-01",  # after certified
            ),
        )
        specs = supersedure_health_planner(corpus, supersedure_health_view_def())
        assert specs == []  # no R4

    def test_R4_skipped_when_dates_missing(self):
        corpus = _corpus(
            _pub_anchor(
                "pub:a", pub_number="A", status="current",
                certified_date="", rescinded_date="",
            ),
        )
        specs = supersedure_health_planner(corpus, supersedure_health_view_def())
        assert specs == []

    def test_dangling_edge_skipped(self):
        # supersedes edge whose endpoints aren't in the corpus
        # (e.g. dst pub got retracted). Planner skips silently;
        # dangling-count is a separate concern.
        corpus = _corpus(
            _pub_anchor("pub:a", pub_number="A", status="rescinded"),
            # No pub:b in corpus.
            _supersedes_edge_anchor(1, src="pub:a", dst="pub:b"),
        )
        specs = supersedure_health_planner(corpus, supersedure_health_view_def())
        assert specs == []

    def test_no_findings_on_well_formed_corpus(self):
        # Healthy supersedure: src current → src rescinded after
        # rewrite; superseded dst rescinded.
        corpus = _corpus(
            _pub_anchor(
                "pub:a", pub_number="A", status="current",
                certified_date="2020-01-15",
            ),
            _pub_anchor(
                "pub:b", pub_number="B", status="rescinded",
                certified_date="2018-01-15",
                rescinded_date="2020-01-15",
            ),
            _supersedes_edge_anchor(1, src="pub:a", dst="pub:b"),
        )
        specs = supersedure_health_planner(corpus, supersedure_health_view_def())
        assert specs == []


# ---------------------------------------------------------------------------
# Determinism — ordering must be stable
# ---------------------------------------------------------------------------


class TestPlannerDeterminism:
    def test_spec_ordering_is_stable_across_runs(self):
        # Build a corpus where dict insertion order would differ
        # between runs if the planner relied on it. Sorted iteration
        # in the planner keeps spec output stable.
        corpus = _corpus(
            _pub_anchor("pub:c", pub_number="C", status="current"),
            _pub_anchor("pub:a", pub_number="A", status="rescinded"),
            _pub_anchor("pub:b", pub_number="B", status="current"),
            _supersedes_edge_anchor(2, src="pub:a", dst="pub:c"),
            _supersedes_edge_anchor(1, src="pub:a", dst="pub:b"),
        )
        specs1 = supersedure_health_planner(corpus, supersedure_health_view_def())
        # Re-shuffle the corpus and re-run — output must be byte-identical.
        shuffled = dict(reversed(list(corpus.items())))
        specs2 = supersedure_health_planner(shuffled, supersedure_health_view_def())
        assert [s.id for s in specs1] == [s.id for s in specs2]


# ---------------------------------------------------------------------------
# Synthesize + refresh end-to-end with the stub LLM
# ---------------------------------------------------------------------------


class TestEndToEndWithStub:
    def test_fresh_generates_full_view(self):
        corpus = _corpus(
            _pub_anchor("pub:a", pub_number="A", status="rescinded"),
            _pub_anchor("pub:b", pub_number="B", status="current"),
            _supersedes_edge_anchor(1, src="pub:a", dst="pub:b"),
        )
        view, stats = fresh(
            new_corpus=corpus,
            view_def=supersedure_health_view_def(),
            llm_client=DeterministicStubLLM(),
        )
        # Six fragments expected: R1 src/dst/analysis + R2
        # src/dst/analysis (both rules fire on this edge).
        assert stats.fragments_total == 6
        assert stats.added == 6
        # Two analysis fragments are GeneratedSpec → two LLM calls.
        assert stats.llm_calls == 2

    def test_refresh_unchanged_corpus_reuses_everything(self):
        corpus = _corpus(
            _pub_anchor("pub:a", pub_number="A", status="rescinded"),
            _pub_anchor("pub:b", pub_number="B", status="current"),
            _supersedes_edge_anchor(1, src="pub:a", dst="pub:b"),
        )
        view_def = supersedure_health_view_def()
        view, _ = fresh(
            new_corpus=corpus, view_def=view_def,
            llm_client=DeterministicStubLLM(),
        )

        view2, stats2 = refresh(
            cached_view=view, new_corpus=corpus, view_def=view_def,
            llm_client=DeterministicStubLLM(),
        )
        assert stats2.reused == 6
        assert stats2.refreshed == 0
        assert stats2.llm_calls == 0

    def test_status_flip_refreshes_only_affected_findings(self):
        # Flip pub:b status current → rescinded. R1/R2 stop firing
        # on this edge; six fragments DROPPED, none added (no new
        # findings).
        corpus_before = _corpus(
            _pub_anchor("pub:a", pub_number="A", status="rescinded"),
            _pub_anchor("pub:b", pub_number="B", status="current"),
            _supersedes_edge_anchor(1, src="pub:a", dst="pub:b"),
        )
        view_def = supersedure_health_view_def()
        view, _ = fresh(
            new_corpus=corpus_before, view_def=view_def,
            llm_client=DeterministicStubLLM(),
        )

        corpus_after = _corpus(
            _pub_anchor("pub:a", pub_number="A", status="rescinded"),
            _pub_anchor("pub:b", pub_number="B", status="rescinded"),
            _supersedes_edge_anchor(1, src="pub:a", dst="pub:b"),
        )
        _, stats = refresh(
            cached_view=view, new_corpus=corpus_after, view_def=view_def,
            llm_client=DeterministicStubLLM(),
        )
        # No findings now → all 6 prior fragments dropped.
        assert stats.dropped == 6
        assert stats.added == 0
        assert stats.refreshed == 0
        assert stats.fragments_total == 0


# ---------------------------------------------------------------------------
# Assembler
# ---------------------------------------------------------------------------


class TestAssembler:
    def test_assembled_markdown_groups_by_rule(self):
        corpus = _corpus(
            _pub_anchor("pub:a", pub_number="A", status="rescinded"),
            _pub_anchor("pub:b", pub_number="B", status="current"),
            _supersedes_edge_anchor(1, src="pub:a", dst="pub:b"),
        )
        view, _ = fresh(
            new_corpus=corpus,
            view_def=supersedure_health_view_def(),
            llm_client=DeterministicStubLLM(),
        )
        rendered = supersedure_health_assembler(view).markdown
        assert "# `af_pubs.supersedure_health`" in rendered
        assert "## R1:" in rendered
        assert "## R2:" in rendered
        # No-finding rules don't get a section.
        assert "## R3:" not in rendered
        assert "## R4:" not in rendered
        # severity annotation present.
        assert "high" in rendered

    def test_empty_view_renders_only_header(self):
        from okg.views.types import View

        v = View(view_id=VIEW_ID, view_def_hash=VIEW_DEF_HASH)
        rendered = supersedure_health_assembler(v).markdown
        assert rendered.strip() == "# `af_pubs.supersedure_health`"


# ---------------------------------------------------------------------------
# Postgres-backed integration test — build_corpus end-to-end
# ---------------------------------------------------------------------------


def _insert_generation(
    conn: psycopg.Connection, *, deployment_name: str = "test",
) -> int:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO okg.graph_generations
              (deployment_name, status, catalog_version_id,
               correlation_id, published_at)
            VALUES (%s, 'published', 1, %s, now())
            RETURNING generation_id
            """,
            (deployment_name, f"test-{deployment_name}"),
        )
        return int(cur.fetchone()[0])


def _insert_pub(
    conn: psycopg.Connection,
    *,
    node_id: str,
    pub_number: str,
    status: str,
    certified_date: str = "",
    rescinded_date: str = "",
    valid_from_gen: int,
    valid_to_gen: int | None = None,
) -> None:
    # Chronos cutover: the publication node lives in the Chronos graph
    # (okg.graph_nodes), read by build_corpus via a generation checkout.
    # ``valid_from_gen``/``valid_to_gen`` no longer map to columns — the
    # caller checkpoints the published generation after seeding so the
    # checkout pins these nodes. (No caller currently retracts a node.)
    store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
    store.apply_facts(
        [
            NodeFact(
                node_id=node_id,
                subtype="publication",
                attrs={
                    "pub_number": pub_number,
                    "title": "Test pub",
                    "status": status,
                    "certified_date": certified_date,
                    "rescinded_date": rescinded_date,
                    "opr": "AF/A3",
                    "pdf_url": "",
                },
                source_record_id={"id": node_id},
                source_revision={"v": 1},
            )
        ],
        source="default",
        strict_endpoints=True,
    )


def _ensure_narrowing(
    conn: psycopg.Connection,
    parent_subtype: str,
    edge_type: str,
    child_subtype: str,
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO okg.narrowings
              (parent_subtype, edge_type, child_subtype, catalog_version_id)
            VALUES (
              %s, %s, %s,
              COALESCE((
                SELECT max(catalog_version_id)
                  FROM okg.catalog_versions
                 WHERE status = 'applied'
              ), 1)
            )
            ON CONFLICT DO NOTHING
            """,
            (parent_subtype, edge_type, child_subtype),
        )


def _insert_supersedes(
    conn: psycopg.Connection,
    *,
    edge_id: int,
    src: str,
    dst: str,
    valid_from_gen: int,
) -> int:
    # Chronos cutover: the edge lives in okg.graph_edges. The Chronos
    # store derives the edge_id from (src, edge_type, dst); the legacy
    # serial ``edge_id`` argument is ignored. Return the derived edge_id
    # so callers can assert on the `edge:<type>:<id>` anchor.
    store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
    store.apply_facts(
        [
            EdgeFact(
                src=src,
                dst=dst,
                edge_type="supersedes",
                attrs={},
                source_record_id={"id": f"{src}->{dst}"},
                source_revision={"v": 1},
            )
        ],
        source="default",
        strict_endpoints=True,
    )
    return _edge_id(src, "supersedes", dst)


def _checkpoint(conn: psycopg.Connection, generation_id: int) -> None:
    """Checkpoint the current Chronos branch head as ``generation_id``
    so ``build_corpus`` (which pins via ``checkout_generation(gen)``)
    resolves the seeded graph."""
    store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
    store.checkpoint_generation(generation_id, deployment_name="test")


@pytest.fixture
def conn(clean_runtime):
    with psycopg.connect(clean_runtime, autocommit=True) as c:
        # Ensure the Chronos schema + default branch exist before the
        # seed helpers (which use ensure_schema=False) run. On a fresh
        # DB the views clean_runtime reset is a no-op, so create them here.
        ChronosGraphStore.from_connection(c, ensure_schema=True)
        yield c


class TestBuildCorpusFromSubstrate:
    @pytest.mark.postgres
    def test_corpus_has_catalog_and_edge_anchors(self, conn):
        from okg.views.backbone import clear_corpus_cache
        clear_corpus_cache()

        gen = _insert_generation(conn)
        _ensure_narrowing(conn, "publication", "supersedes", "publication")
        _insert_pub(
            conn, node_id="pub:a", pub_number="A",
            status="rescinded", valid_from_gen=gen,
        )
        _insert_pub(
            conn, node_id="pub:b", pub_number="B",
            status="current", valid_from_gen=gen,
        )
        edge_id = _insert_supersedes(
            conn, edge_id=1, src="pub:a", dst="pub:b",
            valid_from_gen=gen,
        )
        _checkpoint(conn, gen)

        corpus = build_corpus(conn, generation_id=gen)
        # Both publication anchors and one edge anchor. The Chronos edge_id
        # is derived from (src, edge_type, dst), not the legacy serial 1.
        assert "pub:a:catalog" in corpus
        assert "pub:b:catalog" in corpus
        assert f"edge:supersedes:{edge_id}" in corpus

    @pytest.mark.postgres
    def test_planner_against_substrate_corpus_finds_R1_and_R2(self, conn):
        from okg.views.backbone import clear_corpus_cache
        clear_corpus_cache()

        gen = _insert_generation(conn)
        _ensure_narrowing(conn, "publication", "supersedes", "publication")
        _insert_pub(
            conn, node_id="pub:a", pub_number="A",
            status="rescinded", valid_from_gen=gen,
        )
        _insert_pub(
            conn, node_id="pub:b", pub_number="B",
            status="current", valid_from_gen=gen,
        )
        _insert_supersedes(
            conn, edge_id=1, src="pub:a", dst="pub:b",
            valid_from_gen=gen,
        )
        _checkpoint(conn, gen)

        corpus = build_corpus(conn, generation_id=gen)
        specs = supersedure_health_planner(
            corpus, supersedure_health_view_def(),
        )
        # 7 specs: 1 summary QuoteSpec (per
        # add-deployment-sql-views) + R1 + R2 each with 3
        # fragments. The summary QuoteSpec id is "summary"; the
        # finding specs are the rest. Filter so the count
        # assertion is robust to the deployment views.sql
        # loader being absent (the synthetic-only case below).
        finding_specs = [s for s in specs if s.id.startswith("finding:")]
        assert len(finding_specs) == 6
        # Summary spec present iff the corpus has the SQL-view
        # anchor (which it does in this Postgres-backed test
        # because the deployment views.sql is loaded by the
        # session-scoped fixture).
        assert any(s.id == "summary" for s in specs)
        rule_ids = {s.id.split(":", 2)[1] for s in finding_specs}
        assert rule_ids == {"R1", "R2"}

    def test_view_def_factory_returns_consistent_definition(self):
        vd1 = supersedure_health_view_def()
        vd2 = supersedure_health_view_def()
        assert vd1.view_id == VIEW_ID
        assert vd1.view_def_hash == VIEW_DEF_HASH
        # Each call returns a fresh ViewDefinition (independent
        # params dict) so refresh runs don't share state.
        assert vd1 is not vd2
        assert vd1.params is not vd2.params


class TestAnchorResolver:
    """`anchor_resolver` for the view_cites_node edge projection.

    Per `replace-portal-findings-with-views`. Catalog anchors
    resolve to their publication node; edge / sql_view anchors
    return None.
    """

    def test_catalog_anchor_strips_suffix(self):
        from views.supersedure_health import anchor_resolver
        assert (
            anchor_resolver("publication:afi36-2903:catalog")
            == "publication:afi36-2903"
        )

    def test_edge_anchor_returns_none(self):
        from views.supersedure_health import anchor_resolver
        assert anchor_resolver("edge:supersedes:42") is None

    def test_sql_view_anchor_returns_none(self):
        from views.supersedure_health import anchor_resolver
        assert (
            anchor_resolver("sql_view:supersedure_summary") is None
        )

    def test_unknown_anchor_returns_none(self):
        from views.supersedure_health import anchor_resolver
        assert anchor_resolver("nonsense") is None
        assert anchor_resolver("invariant:finding:foo") is None
