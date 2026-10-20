"""Regression proofs for the standalone-PDF caption caller fan-out."""

from __future__ import annotations

import io
import threading
import time
from pathlib import Path

import psycopg
import pytest
import yaml
from pypdf import PdfWriter

from okg.substrate.db import concurrency_budget
from okg.substrate.exchange.harness import describe_generation
from okg.substrate.graph_store import ChronosGraphStore
from okg.substrate.ingest import SourceRunner
from okg.substrate.library.captioning.service import CaptionResult
from okg.substrate.library.sources.base import NodeFact
from okg.substrate.library.sources.doc_corpus import DocCorpusSource
from okg.substrate.library.sources.scholarly.latex_project_manifest import (
    LatexProjectManifestBuilder,
)
from okg.substrate.publisher import Publisher
from tests.substrate._db_helpers import seed_catalog_version
from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer  # noqa: E402


def _pdf(width: int, height: int) -> bytes:
    writer = PdfWriter()
    writer.add_blank_page(width=width, height=height)
    out = io.BytesIO()
    writer.write(out)
    return out.getvalue()


def _source(
    tmp_path: Path, service: object, **source_kwargs: object,
) -> DocCorpusSource:
    corpus = tmp_path / "corpus"
    corpus.mkdir(exist_ok=True)
    names = ("fig-a.pdf", "fig-b.pdf", "fig-c.pdf")
    sizes = ((72, 36), (144, 72), (216, 108))
    (corpus / "main.tex").write_text(
        "\n".join(rf"\includegraphics{{{name[:-4]}}}" for name in names)
    )
    for name, size in zip(names, sizes):
        (corpus / name).write_bytes(_pdf(*size))
    manifest = LatexProjectManifestBuilder(
        source_root=corpus,
        repository_authority="fanout-test",
        projects=[{"project_dir": ".", "entrypoints": ["main.tex"]}],
    ).build()
    extractors = tmp_path / "extractors"
    extractors.mkdir(exist_ok=True)
    (extractors / "extractors.yaml").write_text(
        yaml.safe_dump(
            {
                "parser": [],
                "chunker": [],
                "ner": [],
                "captioning": {
                    "enabled": True,
                    "model": "stub/vision",
                    "min_image_px": 0,
                },
            }
        )
    )
    return DocCorpusSource(
        repo="fanout-test",
        repo_root=corpus, non_git_walk=True,
        extractors_dir=extractors,
        caption_service=service,
        project_manifest=manifest,
        **source_kwargs,
    )


class _SizeCaptionService:
    def __init__(self, *, fail_width: int | None = None, delay: float = 0.0):
        self.calls: list[tuple[int, int]] = []
        self.active = 0
        self.max_active = 0
        self.fail_width = fail_width
        self.delay = delay
        self._lock = threading.Lock()

    def caption(self, image: object) -> list[CaptionResult]:
        size = image.size
        with self._lock:
            self.calls.append(size)
            self.active += 1
            self.max_active = max(self.max_active, self.active)
        try:
            if self.delay:
                time.sleep(self.delay)
            if self.fail_width == size[0]:
                raise RuntimeError("provider secret should not be persisted")
            return [
                CaptionResult(
                    text=f"caption-size-{size[0]}x{size[1]}",
                    caption_model="stub/vision",
                    caption_provider="stub",
                )
            ]
        finally:
            with self._lock:
                self.active -= 1


@pytest.fixture(autouse=True)
def _clean_budget():
    concurrency_budget.clear_overrides()
    yield
    concurrency_budget.clear_overrides()


def _facts(source: DocCorpusSource) -> list[NodeFact]:
    run = source.run("fanout-test-run", mode="scope_complete", cursor=None)
    return [fact for fact in run.facts if isinstance(fact, NodeFact)]


def test_standalone_pdf_results_stay_associated_and_overlap(tmp_path: Path) -> None:
    concurrency_budget.set_override("enrichment.model_calls", 3)
    service = _SizeCaptionService(delay=0.05)
    source = _source(tmp_path, service)
    facts = _facts(source)

    visuals = {
        fact.attrs["width"]: fact.attrs["visual_id"]
        for fact in facts
        if fact.subtype == "visual_artifact"
    }
    captions = {
        fact.attrs["text"].split("caption-size-", 1)[1].split("x", 1)[0]: fact.attrs[
            "visual_ref"
        ]
        for fact in facts
        if fact.subtype == "document_chunk"
        and fact.attrs.get("chunk_source") == "vision_caption"
    }
    assert set(visuals) == {100, 200, 300}
    assert len(captions) == 3
    assert all(
        captions[str(width)] == visual_id for width, visual_id in visuals.items()
    )
    assert service.max_active >= 2
    assert source.last_run_stats["caption_calls"] == 3
    assert source.last_run_stats["caption_incomplete"] is False


def test_provider_failure_holds_record_set_and_sanitizes_error(tmp_path: Path) -> None:
    concurrency_budget.set_override("enrichment.model_calls", 3)
    source = _source(tmp_path, _SizeCaptionService(fail_width=200))
    run = source.run("fanout-test-run", mode="scope_complete", cursor=None)
    facts = [fact for fact in run.facts if isinstance(fact, NodeFact)]

    assert run.completed_scope is False
    assert run.record_set is None
    assert source.last_run_stats["caption_incomplete"] is True
    assert source.last_run_stats["caption_failures_by_kind"] == {"provider_error": 1}
    serialized = repr([fact.attrs for fact in facts]) + repr(source.last_run_stats)
    assert "provider secret should not be persisted" not in serialized
    failed = [
        fact
        for fact in facts
        if fact.subtype == "visual_artifact"
        and fact.attrs.get("caption_failure_kind") == "provider_error"
    ]
    assert len(failed) == 1


def test_caption_incomplete_run_keeps_a_byte_limit_change_pending(
    tmp_path: Path,
) -> None:
    """Issue #2872: a run whose captions do not finish emits no record set,
    so it must not record a changed max_bytes_per_file as settled; the next
    complete run must re-parse instead of taking the no-change shortcut."""
    concurrency_budget.set_override("enrichment.model_calls", 3)
    first = _source(tmp_path, _SizeCaptionService()).run(
        "limit-default", mode="scope_complete", cursor=None,
    )
    assert first.completed_scope is True
    cursor = dict(first.next_cursor)
    cursor["record_set"] = first.record_set.to_cursor()

    # With no carried caption cache every figure calls the provider again,
    # and fig-b's call fails.
    no_caption_cache = {**cursor, "visual_captions": {}}
    changed_limit = 10**7
    failing_service = _SizeCaptionService(fail_width=200)
    failing = _source(tmp_path, failing_service, max_bytes_per_file=changed_limit)
    incomplete = failing.run(
        "limit-changed", mode="scope_complete", cursor=no_caption_cache,
    )
    assert failing_service.calls
    assert failing.last_run_stats["caption_incomplete"] is True
    assert incomplete.completed_scope is False
    assert incomplete.record_set is None
    # What SourceRunner stores after a run with no record set: the prior one.
    after_incomplete = dict(incomplete.next_cursor)
    after_incomplete["record_set"] = cursor["record_set"]

    complete = _source(
        tmp_path, _SizeCaptionService(), max_bytes_per_file=changed_limit,
    ).run("limit-settled", mode="scope_complete", cursor=after_incomplete)
    assert not complete.preserves_prior_record_set
    assert complete.completed_scope is True
    assert complete.record_set is not None
    assert complete.next_cursor["doc_corpus_max_bytes_per_file"] == changed_limit
    # The cause: the incomplete run recorded a value no real limit equals.
    assert incomplete.next_cursor["doc_corpus_max_bytes_per_file"] == -1


class _RateLimitError(RuntimeError):
    status_code = 429


class _TruncatedError(RuntimeError):
    pass


def test_caption_outcome_classifies_truncation_and_rate_limit_without_text() -> None:
    class _FailureService:
        def __init__(self, exc: Exception, *, rate_limit_hits: int = 0):
            self.exc = exc
            self.last_call_stats = {
                "extras": {"rate_limit_hits": rate_limit_hits},
            }

        def caption(self, image: object) -> list[CaptionResult]:
            raise self.exc

    planned = DocCorpusSource.plan_caption_call("image", context="", caption_kwargs={})
    truncated = DocCorpusSource.run_caption_call_outcome(
        _FailureService(_TruncatedError("provider body")),
        planned,
        image_label="figure.pdf",
    )
    rate_limited = DocCorpusSource.run_caption_call_outcome(
        _FailureService(_RateLimitError("provider key and body"), rate_limit_hits=2),
        planned,
        image_label="figure.pdf",
    )
    assert truncated.failure_kind == "truncated"
    assert rate_limited.failure_kind == "rate_limited"
    assert rate_limited.rate_limit_hits == 2
    assert not hasattr(truncated, "error")


@pytest.mark.postgres
def test_failed_rerun_keeps_prior_published_standalone_pdf_rows(
    clean_runtime: str,
    tmp_path: Path,
) -> None:
    """A real source/publish cycle keeps the prior record-set authority.

    The second run changes one PDF and fails its caption call. The source
    reports an incomplete scope, and the following publish must still expose
    the prior visual/caption rows rather than replacing them with a partial
    record set.
    """
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog_version(conn)
        ChronosGraphStore.from_connection(conn, ensure_schema=True)

    service = _SizeCaptionService()
    source = _source(tmp_path, service)
    runner = SourceRunner(clean_runtime, source_name="test.standalone-pdf-fanout")
    first_result = runner.run(source, mode="scope_complete")
    assert first_result.completed_scope is True
    first_publish = Publisher(clean_runtime, "default", catalog_identity=bind_test_catalog_writer(clean_runtime, "default")).cycle()
    assert first_publish["status"] == "published", first_publish
    first_generation = str(first_publish["generation_id"])
    print(f"ISSUE1190_PUBLISHED_GENERATION={first_generation}")
    assert describe_generation(clean_runtime, first_generation)["status"] == (
        "published"
    )

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
        first_rows = store.query(
            "SELECT node_id, attrs->>'text' AS text "
            "FROM okg.graph_nodes "
            "WHERE subtype = 'document_chunk' "
            "AND attrs->>'chunk_source' = 'vision_caption'"
        )
    assert len(first_rows) == 3

    # Change one source PDF, then fail exactly that visual's provider call.
    (tmp_path / "corpus" / "fig-b.pdf").write_bytes(_pdf(144, 80))
    failed_service = _SizeCaptionService(fail_width=200)
    failed_source = _source(tmp_path, failed_service)
    failed_result = runner.run(
        failed_source,
        mode="scope_complete",
        reset_cursor=True,
    )
    assert failed_service.calls, failed_source.last_run_stats
    assert failed_result.completed_scope is False
    assert failed_result.next_cursor is not None
    failed_publish = Publisher(clean_runtime, "default", catalog_identity=bind_test_catalog_writer(clean_runtime, "default")).cycle()
    assert failed_publish["status"] == "published", failed_publish
    second_generation = str(failed_publish["generation_id"])
    assert describe_generation(clean_runtime, second_generation)["status"] == (
        "published"
    )

    store = ChronosGraphStore.connect(clean_runtime, ensure_schema=False)
    try:
        second_checkout = store.checkout_generation(second_generation)
        second_rows = second_checkout.query(
            "SELECT node_id, attrs->>'text' AS text "
            "FROM okg.graph_nodes "
            "WHERE subtype = 'document_chunk' "
            "AND attrs->>'chunk_source' = 'vision_caption'"
        )
    finally:
        store.close()
    assert {(row["node_id"], row["text"]) for row in second_rows} == {
        (row["node_id"], row["text"]) for row in first_rows
    }
