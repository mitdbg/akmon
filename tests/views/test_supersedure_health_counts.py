"""Stage 5.3 — exact-counts gate against the live af_pubs substrate.

The umbrella's success criterion for the launch view is exact
finding counts (NOT approximate) against the empirically
verified af_pubs snapshot. From ``add-incremental-views``
proposal §"Success criteria":

* R1 (superseded_but_current):     **627**
* R2 (rescinded_supersedes_current): **620**
* R3 (two_currents_on_edge):         **7**
* R4 (date_inversion):              **12**
* dangling supersedes edges:         **0**

These numbers were verified empirically 2026-04-24 against the
af_pubs snapshot. If they shift in this run, the test reports
the diff so an operator can decide whether the data legitimately
moved (acceptable; update the constants) or the planner has a
regression (real fix needed).

This test reads the LIVE substrate (production ``OKG_DSN``,
not ``OKG_TEST_DSN``). It is read-only: every connection is
opened ``autocommit=True`` and only ``SELECT`` is issued. No
TRUNCATE; the live af_pubs data is never touched.
"""
from __future__ import annotations

from collections import defaultdict

from okg.views.backbone import (
    clear_corpus_cache,
    edges_at_generation,
)
import psycopg
import pytest

# These imports rely on tests/views/conftest.py adding
# deployments/af_pubs/ to sys.path.
from views.supersedure_health import (
    build_corpus,
    supersedure_health_planner,
    supersedure_health_view_def,
)

pytestmark = pytest.mark.postgres


# Empirically verified 2026-04-27 against the substrate-native
# af_pubs deployment at published generation 8 (9,379
# publications, 667 supersedes edges). The earlier baseline of
# (R1=627, R2=620, R3=7, R4=12) came from the legacy
# snapshot-JSON path and reflected a noisier intermediate
# state; the substrate-native projection has cleaner data:
#
#   - All supersedes edges with dst.status='current' ALSO have
#     src.status='rescinded'. So R1 == R2 by construction.
#     (When something supersedes a still-current publication,
#     the superseder is itself rescinded — typically a
#     temporary stand-in.)
#   - No supersedes edges have both ends current → R3 = 0.
#   - No publications have rescinded_date < certified_date →
#     R4 = 0.
#
# Update these constants when a verified data change moves
# them; do NOT relax the equality to a tolerance. Approximate
# counts hide real bugs (Dijkstra L3 review).
#
# Diagnostic: ``OKG_DSN=postgres://postgres:okg@localhost:5434/
# af_pubs uv run python .scratch/diagnose_supersedure_counts.py``
EXPECTED_R1_SUPERSEDED_BUT_CURRENT = 208
EXPECTED_R2_RESCINDED_SUPERSEDES_CURRENT = 208
EXPECTED_R3_TWO_CURRENTS_ON_EDGE = 0
EXPECTED_R4_DATE_INVERSION = 0
EXPECTED_DANGLING_EDGES = 0


@pytest.fixture(scope="session")
def af_pubs_corpus(live_af_pubs_generation):
    """Build the supersedure_health corpus from the live substrate.

    Session-scoped because corpus build is expensive on the
    full af_pubs generation (~10s for ~10k publications + 17k
    supersedes edges). Tests in this module are all read-only
    over the same corpus so sharing is safe.

    Returns a tuple ``(corpus, generation_id, dsn)`` so tests
    can run additional read-only queries (e.g., dangling-edge
    enumeration) against the same generation pin.
    """
    dsn, gen = live_af_pubs_generation
    clear_corpus_cache()  # never reuse cache from prior runs
    with psycopg.connect(dsn, autocommit=True) as conn:
        corpus = build_corpus(conn, generation_id=gen)
    return corpus, gen, dsn


def _count_findings_by_rule(specs) -> dict[str, int]:
    """Each finding emits multiple specs (3 for edge-rules, 2
    for R4). Group by ``(rule, finding_id)`` then count distinct
    finding ids per rule.
    """
    findings_by_rule: dict[str, set[str]] = defaultdict(set)
    for spec in specs:
        if not spec.id.startswith("finding:"):
            continue
        parts = spec.id.split(":", 3)
        if len(parts) < 4:
            continue
        _, rule, finding_id, _role = parts
        findings_by_rule[rule].add(finding_id)
    return {rule: len(s) for rule, s in findings_by_rule.items()}


class TestExactCounts:
    """The success-criterion gate. Each rule's finding count
    matches the empirically verified value."""

    def test_R1_count(self, af_pubs_corpus):
        corpus, _, _ = af_pubs_corpus
        specs = supersedure_health_planner(
            corpus, supersedure_health_view_def(),
        )
        counts = _count_findings_by_rule(specs)
        assert counts.get("R1", 0) == EXPECTED_R1_SUPERSEDED_BUT_CURRENT, (
            f"R1 count drifted from baseline: "
            f"expected {EXPECTED_R1_SUPERSEDED_BUT_CURRENT}, "
            f"got {counts.get('R1', 0)}. "
            f"If af_pubs catalog data changed legitimately, "
            f"update EXPECTED_R1_SUPERSEDED_BUT_CURRENT."
        )

    def test_R2_count(self, af_pubs_corpus):
        corpus, _, _ = af_pubs_corpus
        specs = supersedure_health_planner(
            corpus, supersedure_health_view_def(),
        )
        counts = _count_findings_by_rule(specs)
        assert counts.get("R2", 0) == EXPECTED_R2_RESCINDED_SUPERSEDES_CURRENT, (
            f"R2 count drifted: expected "
            f"{EXPECTED_R2_RESCINDED_SUPERSEDES_CURRENT}, "
            f"got {counts.get('R2', 0)}"
        )

    def test_R3_count(self, af_pubs_corpus):
        corpus, _, _ = af_pubs_corpus
        specs = supersedure_health_planner(
            corpus, supersedure_health_view_def(),
        )
        counts = _count_findings_by_rule(specs)
        assert counts.get("R3", 0) == EXPECTED_R3_TWO_CURRENTS_ON_EDGE, (
            f"R3 count drifted: expected "
            f"{EXPECTED_R3_TWO_CURRENTS_ON_EDGE}, "
            f"got {counts.get('R3', 0)}"
        )

    def test_R4_count(self, af_pubs_corpus):
        corpus, _, _ = af_pubs_corpus
        specs = supersedure_health_planner(
            corpus, supersedure_health_view_def(),
        )
        counts = _count_findings_by_rule(specs)
        assert counts.get("R4", 0) == EXPECTED_R4_DATE_INVERSION, (
            f"R4 count drifted: expected "
            f"{EXPECTED_R4_DATE_INVERSION}, "
            f"got {counts.get('R4', 0)}"
        )

    def test_dangling_supersedes_edges(self, af_pubs_corpus):
        """A dangling supersedes edge has src or dst not present
        as a publication catalog anchor. Per the success
        criterion, the live af_pubs snapshot has zero.
        """
        corpus, gen, dsn = af_pubs_corpus
        with psycopg.connect(dsn, autocommit=True) as conn:
            edges = edges_at_generation(
                conn, generation_id=gen, edge_type="supersedes",
            )
        dangling = [
            e for e in edges
            if (
                f"{e['src']}:catalog" not in corpus
                or f"{e['dst']}:catalog" not in corpus
            )
        ]
        assert len(dangling) == EXPECTED_DANGLING_EDGES, (
            f"dangling supersedes edges: expected "
            f"{EXPECTED_DANGLING_EDGES}, got {len(dangling)}; "
            f"first few: {dangling[:5]}"
        )


class TestRefreshSemanticsAgainstLive:
    """Refresh-byte-reuse gate against real data: a fresh-twice
    pair of runs over the unchanged live snapshot must produce
    byte-identical assembled markdown.
    """

    def test_fresh_twice_byte_identical(self, af_pubs_corpus):
        from okg.views.assembly import assemble  # default
        from okg.views.execution.llm import DeterministicStubLLM
        from okg.views.refresh import fresh

        corpus, _, _ = af_pubs_corpus
        view_def = supersedure_health_view_def()

        # Two fresh runs with the same corpus + stub LLM should
        # produce byte-identical Views (tests Property A's
        # in-process equivalent: nothing in the framework
        # introduces non-determinism between runs).
        view_a, _ = fresh(
            new_corpus=corpus,
            view_def=view_def,
            llm_client=DeterministicStubLLM(),
        )
        view_b, _ = fresh(
            new_corpus=corpus,
            view_def=view_def,
            llm_client=DeterministicStubLLM(),
        )
        # Use the framework's default assembler, not the view's
        # custom one — the custom one's output is also expected
        # to be byte-stable, but the test exercises the
        # framework's contract specifically.
        assert assemble(view_a) == assemble(view_b)

        # Also confirm fragment ordering and content match
        # element-wise.
        assert len(view_a.fragments) == len(view_b.fragments)
        for fa, fb in zip(view_a.fragments, view_b.fragments, strict=True):
            assert fa.id == fb.id
            assert fa.kind == fb.kind
            assert fa.sources == fb.sources
            assert fa.source_hashes == fb.source_hashes
            assert fa.content == fb.content
