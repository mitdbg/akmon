"""Phase 3 tests for A4 — TalksSource.

Verifies talks YAML reading, transcript fetcher injection, transcript
caching across runs, and the chunked-document emission shape.
"""
from __future__ import annotations

from pathlib import Path

import pytest

from okg.substrate.errors import SubstrateError
from okg.substrate.library.sources.scholarly.talks import (
    TalksSource,
    TranscriptFetchError,
)


FIXTURE = Path(__file__).resolve().parent / "fixtures" / "talks.yaml"


def _mock_fetcher_factory(call_log: list[str]):
    def _mock(youtube_id: str) -> list[dict]:
        call_log.append(youtube_id)
        # Return 3 simulated segments of ~30 chars each.
        return [
            {"text": f"segment one for {youtube_id}", "start": 0.0, "duration": 5.0},
            {"text": "segment two text here", "start": 5.0, "duration": 5.0},
            {"text": "segment three closes things", "start": 10.0, "duration": 5.0},
        ]
    return _mock


def _collect(source: TalksSource) -> tuple[dict, list, dict]:
    run = source.run("test-run", mode="scope_complete")
    nodes: dict[str, object] = {}
    edges: list[object] = []
    for f in run.facts:
        if hasattr(f, "node_id"):
            nodes[f.node_id] = f
        elif hasattr(f, "edge_type"):
            edges.append(f)
    return nodes, edges, source.last_run_stats


def test_talks_emits_paper_speaker_document_chunks(tmp_path: Path) -> None:
    log: list[str] = []
    src = TalksSource(
        talks_yaml_path=str(FIXTURE),
        transcript_cache_root=str(tmp_path / "transcripts"),
        transcript_fetcher=_mock_fetcher_factory(log),
    )
    nodes, edges, stats = _collect(src)

    # Two Paper nodes, both kind=talk
    paper_ids = [k for k in nodes if k.startswith("paper:talk:")]
    assert len(paper_ids) == 2
    for pid in paper_ids:
        assert nodes[pid].attrs["kind"] == "talk"
        assert nodes[pid].attrs["tier"] == 1

    # Speakers as Person nodes
    persons = [k for k in nodes if k.startswith("person:")]
    assert len(persons) == 2

    # Each talk emits one Document + chunks
    documents = [k for k in nodes if k.startswith("doc:paper:talk:")]
    assert len(documents) == 2
    chunks = [k for k in nodes if k.startswith("chunk:paper:talk:")]
    assert len(chunks) == 2  # each fixture transcript is short → 1 chunk per talk

    # Stats
    assert stats["talks"] == 2
    assert stats["transcripts_fetched"] == 2
    assert stats["transcripts_cached"] == 0


def test_talks_cache_hit_on_second_run(tmp_path: Path) -> None:
    log: list[str] = []
    cache = tmp_path / "transcripts"
    src = TalksSource(
        talks_yaml_path=str(FIXTURE),
        transcript_cache_root=str(cache),
        transcript_fetcher=_mock_fetcher_factory(log),
    )
    # First run populates the cache.
    list(src.run("run-1", mode="scope_complete").facts)
    assert len(log) == 2  # two transcripts fetched

    # Second run reads from cache.
    log.clear()
    src2 = TalksSource(
        talks_yaml_path=str(FIXTURE),
        transcript_cache_root=str(cache),
        transcript_fetcher=_mock_fetcher_factory(log),
    )
    list(src2.run("run-2", mode="scope_complete").facts)
    assert log == []  # fetcher NOT called; cache hits
    assert src2.last_run_stats["transcripts_cached"] == 2
    assert src2.last_run_stats["transcripts_fetched"] == 0


def test_talks_tier_propagation_from_annote(tmp_path: Path) -> None:
    src = TalksSource(
        talks_yaml_path=str(FIXTURE),
        transcript_cache_root=str(tmp_path),
        transcript_fetcher=_mock_fetcher_factory([]),
    )
    nodes, _, _ = _collect(src)
    # First fixture entry has `annote: tier: 1, auto_extract: true`
    first = nodes["paper:talk:youtube:ABCDEFGHIJK"]
    assert first.attrs["tier"] == 1
    assert first.attrs["auto_extract"] is True
    # Second has no annote → default Tier 1
    second = nodes["paper:talk:youtube:QRSTUVWXYZ1"]
    assert second.attrs["tier"] == 1


def test_talks_missing_yaml_path_raises() -> None:
    with pytest.raises(SubstrateError) as exc:
        TalksSource()
    assert "talks_yaml_path is required" in str(exc.value)


def test_talks_nonexistent_yaml_raises(tmp_path: Path) -> None:
    src = TalksSource(talks_yaml_path=str(tmp_path / "missing.yaml"))
    with pytest.raises(SubstrateError) as exc:
        list(src.run("run", mode="scope_complete").facts)
    assert "does not resolve to a file" in str(exc.value)


def test_talks_fetcher_failure_propagates(tmp_path: Path) -> None:
    def _failing_fetcher(yt: str) -> list[dict]:
        raise TranscriptFetchError(
            f"simulated fetch failure for {yt}",
            data={"youtube_id": yt},
        )

    src = TalksSource(
        talks_yaml_path=str(FIXTURE),
        transcript_cache_root=str(tmp_path),
        transcript_fetcher=_failing_fetcher,
    )
    with pytest.raises(TranscriptFetchError):
        list(src.run("run", mode="scope_complete").facts)


def test_talks_youtube_id_extraction() -> None:
    from okg.substrate.library.sources.scholarly.talks import (
        _extract_youtube_id,
    )
    assert _extract_youtube_id(
        "https://www.youtube.com/watch?v=ABCDEFGHIJK"
    ) == "ABCDEFGHIJK"
    assert _extract_youtube_id(
        "https://youtu.be/QRSTUVWXYZ1"
    ) == "QRSTUVWXYZ1"
    assert _extract_youtube_id("not a youtube url") is None
