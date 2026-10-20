"""Independent-targeter witnesses (task.bench.independent-targeter).

Pins the verification clause: the targeter is constructed without any DSN or
generation handle (the type exposes no graph-read capability), emits sites
for the regexable object facets and at least one co-occurrence
cross-document pair from corpus bytes alone, and its sites carry the
independent-mode marker so the scorer keeps their denominator separate.
"""

from __future__ import annotations

import inspect
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

import pytest

from okg.bench.injection import (
    ClaimSnapshot,
    CorpusSnapshot,
    ExpectedOutcome,
    Facets,
    InjectionError,
    KeyedPRF,
    MutationPlan,
    RunConfig,
    Site,
    Span,
    run_injection,
)
from okg.bench.injection.independent import (
    INDEPENDENT_FACET_KINDS,
    IndependentTargeter,
)

NODES_MD = """# Nodes

Primary is DB-01 with 512 MB reserved.
Failover drill on 2026-03-14.
Dashboard: https://ops.example/nodes
"""

RUNBOOK_MD = """# Runbook

Restart DB-01 after any failover.

retry_limit: 5
"""


@pytest.fixture()
def corpus(tmp_path: Path) -> CorpusSnapshot:
    root = tmp_path / "clean"
    root.mkdir()
    (root / "nodes.md").write_text(NODES_MD, encoding="utf-8")
    (root / "runbook.md").write_text(RUNBOOK_MD, encoding="utf-8")
    return CorpusSnapshot.load(root)


def test_type_exposes_no_graph_read_surface():
    parameters = set(inspect.signature(IndependentTargeter.__init__).parameters)
    assert parameters == {"self", "facet_kinds"}
    sites_parameters = list(inspect.signature(IndependentTargeter.sites).parameters)
    assert sites_parameters == ["self", "corpus"]
    forbidden = {"dsn", "conn", "generation_id", "claims", "session"}
    assert not forbidden & parameters


def test_emits_every_regexable_facet_kind(corpus):
    sites = IndependentTargeter().sites(corpus)
    kinds = {site.kind for site in sites}
    assert kinds == set(INDEPENDENT_FACET_KINDS)
    assert all(site.site.mode == "independent" for site in sites)
    for site in sites:
        text = corpus.read_text(site.site.doc)
        span = site.site.span
        assert span is not None
        assert text[span.start : span.end] == site.matched_text


def test_cooccurrence_pair_is_cross_document(corpus):
    sites = IndependentTargeter().sites(corpus)
    paired = [site for site in sites if site.counterparts]
    assert paired, "expected at least one co-occurrence pair"
    db01 = [site for site in paired if site.matched_text == "DB-01"]
    assert db01
    for site in db01:
        assert all(doc != site.site.doc for doc, _span in site.counterparts)


def test_enumeration_is_deterministic(corpus):
    first = IndependentTargeter().sites(corpus)
    second = IndependentTargeter().sites(corpus)
    assert first == second


def test_unknown_facet_kind_is_loud():
    with pytest.raises(InjectionError):
        IndependentTargeter(facet_kinds=("date", "nonsense"))


@dataclass(frozen=True)
class IndependentDateShift:
    """Test operator driving targeter sites through the injection engine."""

    operator_id: str = "test.independent_date_shift"
    version: str = "1"

    def sites(self, corpus: CorpusSnapshot, claims: ClaimSnapshot) -> Iterable[Site]:
        for independent_site in IndependentTargeter(facet_kinds=("date",)).sites(corpus):
            yield independent_site.site

    def plan(self, site: Site, doc_text: str, span: Span, prf: KeyedPRF) -> MutationPlan:
        pre_text = doc_text[span.start : span.end]
        year = int(pre_text[:4]) + prf.randint(f"{self.operator_id}:{site.site_id}", 1, 3)
        return MutationPlan(
            site=site,
            span=span,
            pre_text=pre_text,
            post_text=f"{year}{pre_text[4:]}",
            params={"kind": "year_shift"},
            facets=Facets("contradiction", "date", "intra_document", "time", "wrong_fact"),
            hardness_tier="t1_literal",
            expected=ExpectedOutcome(kind="evidence_overlap"),
        )


def test_independent_sites_flow_through_the_engine(corpus, tmp_path):
    claims = ClaimSnapshot.from_rows("g-clean-0003", [])
    result = run_injection(
        corpus=corpus,
        claims=claims,
        operators=[IndependentDateShift()],
        config=RunConfig(seed=11, out_dir=tmp_path / "out"),
    )
    assert result.mutations
    for record in result.mutations:
        assert record.mode == "independent"
        assert record.expected.kind == "evidence_overlap"
    assert result.manifest["targeting"]["modes"] == ["independent"]
    mutated = (result.mutated_corpus_dir / "nodes.md").read_text(encoding="utf-8")
    assert "2026-03-14" not in mutated
