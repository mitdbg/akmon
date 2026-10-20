"""The inverted-index blocking in `_candidate_link_edges` must be
output-preserving: it scores only artifact<->spine pairs sharing >=1 token
(the rest could never match `_candidate_match` anyway), so the emitted
candidate_link edges are exactly those a full cross-product would emit.

Unit tests over the orchestration — the chronos reads are monkeypatched, no
Postgres required.
"""
from __future__ import annotations

from okg.substrate.library.linkers import academic_researcher_collaboration as lk


SPINE = [
    {"node_id": "project:chronos", "subtype": "project",
     "attrs": {"slug": "chronos", "title": "Chronos bitemporal store"}},
    {"node_id": "project:marius", "subtype": "project",
     "attrs": {"slug": "marius", "title": "Marius graph embeddings"}},
    {"node_id": "person:researcher", "subtype": "person",
     "attrs": {"display_name": "Persephone Quantitative"}},
]

ARTIFACTS = [
    # shares distinctive "chronos" + "bitemporal" with project:chronos
    {"node_id": "doc:1", "subtype": "document",
     "attrs": {"title": "Chronos design notes", "text": "bitemporal chronos store"}},
    # shares nothing with any spine node -> no candidate
    {"node_id": "doc:2", "subtype": "document",
     "attrs": {"title": "grocery errands", "text": "milk eggs bread"}},
    # shares distinctive "marius" + "embeddings" with project:marius
    {"node_id": "doc:3", "subtype": "document",
     "attrs": {"title": "marius embeddings run", "text": "marius embeddings"}},
]


def _patch(monkeypatch, spine=SPINE, artifacts=ARTIFACTS):
    def fake_nodes(cur, subtypes):
        if set(subtypes) & set(lk._CANDIDATE_ARTIFACT_SUBTYPES):
            return list(artifacts)
        return list(spine)
    monkeypatch.setattr(lk._chronos, "nodes", fake_nodes)
    monkeypatch.setattr(lk, "_existing_canonical_targets", lambda cur: set())
    monkeypatch.setattr(lk, "_existing_candidate_targets", lambda cur: set())


def _pairs(monkeypatch, **kw):
    _patch(monkeypatch, **kw)
    out = lk._candidate_link_edges(
        None, generation_id=1, canonical_pairs={}, linker_name="t"
    )
    return {(c.src, c.dst) for c in out}, out


def test_blocking_emits_token_sharing_pairs(monkeypatch):
    pairs, _ = _pairs(monkeypatch)
    assert ("doc:1", "project:chronos") in pairs
    assert ("doc:3", "project:marius") in pairs


def test_zero_overlap_artifact_emits_nothing(monkeypatch):
    pairs, _ = _pairs(monkeypatch)
    assert not any(src == "doc:2" for src, _ in pairs)


def test_no_cross_links_to_unshared_spine(monkeypatch):
    # doc:1 must not link to project:marius (no shared distinctive token).
    pairs, _ = _pairs(monkeypatch)
    assert ("doc:1", "project:marius") not in pairs
    assert ("doc:3", "project:chronos") not in pairs


def test_deterministic_across_runs(monkeypatch):
    # The candidate set is a `set` internally; the (score, dst_id) sort must
    # make the emitted order stable run-to-run.
    a, _ = _pairs(monkeypatch)
    b, _ = _pairs(monkeypatch)
    assert a == b


def test_emits_well_formed_candidate_links(monkeypatch):
    _, out = _pairs(monkeypatch)
    assert out, "expected at least one candidate link"
    for c in out:
        assert c.edge_type == "candidate_link"
        assert c.attrs["candidate_state"] == "pending_review"
        assert c.attrs["schema"] == "okg.academic_pi.candidate_link.v1"
