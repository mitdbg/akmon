from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest

from okg.substrate.library.sources.base import EdgeFact, NodeFact, ProgressMarker
from okg.substrate.library.sources.scholarly import inspire_hep, openreview


@dataclass(frozen=True)
class _Case:
    module: Any
    source_type: type
    source_name: str
    venue: dict[str, Any]
    payloads: tuple[dict[str, Any], dict[str, Any]]
    provider_record_ids: tuple[str, str]


CASES = (
    _Case(
        module=openreview,
        source_type=openreview.OpenReviewSource,
        source_name="openreview",
        venue={
            "slug": "test",
            "name": "TestConf",
            "year": 2026,
            "invitation": "Test/2026/-/Submission",
        },
        payloads=(
            {
                "notes": [
                    {
                        "id": "note-or-1",
                        "forum": "or-1",
                        "content": {"title": {"value": "OpenReview One"}},
                    }
                ],
                "count": 2,
            },
            {
                "notes": [
                    {
                        "id": "note-or-2",
                        "forum": "or-2",
                        "content": {"title": {"value": "OpenReview Two"}},
                    }
                ],
                "count": 2,
            },
        ),
        provider_record_ids=("test:2026:0", "test:2026:1"),
    ),
    _Case(
        module=inspire_hep,
        source_type=inspire_hep.InspireHepSource,
        source_name="inspire_hep",
        venue={
            "slug": "test",
            "name": "TestConf",
            "queries": ["conference TestConf"],
            "split_by_year": False,
        },
        payloads=(
            {
                "hits": {
                    "hits": [
                        {
                            "id": "inspire-1",
                            "metadata": {
                                "control_number": 1,
                                "titles": [{"title": "INSPIRE One"}],
                                "publication_info": [{"year": 2026}],
                            },
                        }
                    ],
                    "total": 2,
                }
            },
            {
                "hits": {
                    "hits": [
                        {
                            "id": "inspire-2",
                            "metadata": {
                                "control_number": 2,
                                "titles": [{"title": "INSPIRE Two"}],
                                "publication_info": [{"year": 2026}],
                            },
                        }
                    ],
                    "total": 2,
                }
            },
        ),
        provider_record_ids=("test:1", "test:2"),
    ),
)


def _source(case: _Case, cache_root: Path):
    kwargs: dict[str, Any] = {
        "cache_root": cache_root,
        "requests_per_second": 10.0,
        "page_size": 1,
        "venues": [case.venue],
    }
    return case.source_type(**kwargs)


@pytest.mark.parametrize("case", CASES, ids=lambda case: case.source_name)
def test_paginated_run_preserves_facts_provenance_cache_and_progress(
    case: _Case,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    payloads = list(case.payloads)
    fetch_queries: list[dict[str, Any]] = []
    limiter_waits: list[None] = []

    def _fetch(_endpoint: str, query: dict[str, Any], **_kwargs: Any):
        fetch_queries.append(query)
        return payloads.pop(0)

    monkeypatch.setattr(case.module, "_fetch_json", _fetch)
    monkeypatch.setattr(
        case.module.RateLimiter,
        "wait",
        lambda _self: limiter_waits.append(None),
    )
    source = _source(case, tmp_path)

    first = list(source.run("first").facts)
    nodes = [fact for fact in first if isinstance(fact, NodeFact)]
    edges = [fact for fact in first if isinstance(fact, EdgeFact)]
    progress = [fact for fact in first if isinstance(fact, ProgressMarker)]

    assert len(fetch_queries) == 2
    assert len(limiter_waits) == 2
    assert len(nodes) == 4
    assert len(edges) == 4
    assert [marker.record_id for marker in progress] == [
        f"page:{record_id}" for record_id in case.provider_record_ids
    ]
    assert {fact.attrs["source_provider"] for fact in [*nodes, *edges]} == {
        case.source_name
    }
    assert {fact.source_record_id["source"] for fact in [*nodes, *edges]} == {
        case.source_name
    }
    assert source.last_run_stats["pages_completed"] == 2
    assert source.last_run_stats["cache_misses"] == 2

    cache_rows = sorted(
        (
            json.loads(path.read_text(encoding="utf-8"))
            for path in (tmp_path / case.source_name).glob("*.json")
        ),
        key=lambda row: row["provider_record_id"],
    )
    assert {row["provider_record_id"] for row in cache_rows} == set(
        case.provider_record_ids
    )
    assert {json.dumps(row["query"], sort_keys=True) for row in cache_rows} == {
        json.dumps(query, sort_keys=True) for query in fetch_queries
    }

    second = list(source.run("second").facts)
    assert [(type(fact), fact) for fact in second] == [
        (type(fact), fact) for fact in first
    ]
    assert len(fetch_queries) == 2
    assert len(limiter_waits) == 2
    assert source.last_run_stats["cache_hits"] == 2
    assert source.last_run_stats["cache_misses"] == 0


@pytest.mark.parametrize("case", CASES, ids=lambda case: case.source_name)
def test_failed_later_page_emits_no_completion_marker_for_that_page(
    case: _Case,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    failure = RuntimeError("second page failed")
    actions: list[dict[str, Any] | BaseException] = [case.payloads[0], failure]

    def _fetch(_endpoint: str, _query: dict[str, Any], **_kwargs: Any):
        action = actions.pop(0)
        if isinstance(action, BaseException):
            raise action
        return action

    monkeypatch.setattr(case.module, "_fetch_json", _fetch)
    source = _source(case, tmp_path)
    facts = iter(source.run("failed", mode="scope_complete").facts)
    emitted: list[NodeFact | EdgeFact | ProgressMarker] = []

    def _consume_facts() -> None:
        for fact in facts:
            emitted.append(fact)

    with pytest.raises(RuntimeError, match="second page failed"):
        _consume_facts()

    assert [
        marker.record_id for marker in emitted if isinstance(marker, ProgressMarker)
    ] == [f"page:{case.provider_record_ids[0]}"]
    assert not any(
        marker.record_id == f"page:{case.provider_record_ids[1]}"
        for marker in emitted
        if isinstance(marker, ProgressMarker)
    )
    assert source.last_run_stats == {}
