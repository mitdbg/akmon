"""`okg ingest` CLI — topological ordering, cycle detection,
include/exclude.

Per `add-okg-ingest-cli` (2026-04-26).
"""
from __future__ import annotations

from types import SimpleNamespace

from okg.substrate.ingest import (
    resolve_predecessor_deps,
    topo_layers,
    topo_sort,
)


def test_topo_sort_simple_chain() -> None:
    deps = {"a": set(), "b": {"a"}, "c": {"b"}}
    assert topo_sort(deps) == ["a", "b", "c"]


def test_topo_sort_independent_sources() -> None:
    deps = {"a": set(), "b": set(), "c": set()}
    out = topo_sort(deps)
    # All three are independent; order is alphabetical via the
    # tiebreak in the implementation.
    assert sorted(out) == ["a", "b", "c"]
    assert len(out) == 3


def test_topo_sort_diamond() -> None:
    deps = {
        "a": set(),
        "b": {"a"},
        "c": {"a"},
        "d": {"b", "c"},
    }
    order = topo_sort(deps)
    # `a` first; `d` last; `b` and `c` between.
    assert order[0] == "a"
    assert order[-1] == "d"
    assert set(order[1:3]) == {"b", "c"}


def test_topo_layers_publishable_dependency_depths() -> None:
    deps = {
        "obsolete": set(),
        "catalog": {"obsolete"},
        "pdf_text": {"catalog"},
        "section_chunker": {"pdf_text"},
        "publication_family": {"obsolete", "catalog"},
    }

    assert topo_layers(deps) == [
        ["obsolete"],
        ["catalog"],
        ["pdf_text", "publication_family"],
        ["section_chunker"],
    ]


def test_topo_sort_cycle_returns_none() -> None:
    deps = {"a": {"b"}, "b": {"a"}}
    assert topo_sort(deps) is None


def test_topo_sort_self_loop_is_cycle() -> None:
    deps = {"a": {"a"}}
    assert topo_sort(deps) is None


def test_topo_layers_cycle_returns_none() -> None:
    deps = {"a": {"b"}, "b": {"a"}}
    assert topo_layers(deps) is None


def test_resolve_predecessor_deps_accepts_registry_keys_and_ownership_ids() -> None:
    runnable = {
        "openalex": SimpleNamespace(
            ownership_id="memex.openalex",
            raw={"ownership_id": "memex.openalex"},
        ),
        "arxiv_oai": SimpleNamespace(
            ownership_id="memex.arxiv_oai",
            raw={"ownership_id": "memex.arxiv_oai"},
        ),
        "semantic_scholar": SimpleNamespace(
            ownership_id="memex.semantic_scholar",
            raw={
                "ownership_id": "memex.semantic_scholar",
                "predecessor_source": ["openalex", "memex.arxiv_oai"],
            },
        ),
    }

    assert resolve_predecessor_deps(runnable) == {
        "openalex": set(),
        "arxiv_oai": set(),
        "semantic_scholar": {"openalex", "arxiv_oai"},
    }
