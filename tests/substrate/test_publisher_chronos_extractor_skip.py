from __future__ import annotations

from types import SimpleNamespace

import pytest

from okg.substrate.catalog.linker_config import LinkerRoute
from okg.substrate.projection import RegexEntityExtractor
from okg.substrate.publisher import Publisher
from okg.substrate.publisher.extractor_stage import ChronosExtractorStage


def _extractor_config() -> dict[str, object]:
    return {
        "extractor": RegexEntityExtractor([("ticket", r"(TICKET-\d+)")]),
        "extractor_source_subtypes": ["page"],
        "routes": [
            LinkerRoute(
                entity_type="ticket",
                src_subtype=None,
                edge_type="mentions",
                dst_template="ticket:{value}",
            ),
        ],
    }


def _stage() -> ChronosExtractorStage:
    return ChronosExtractorStage.from_config(
        "postgres://unused",
        deployment_name="test",
        **_extractor_config(),
    )


def _publisher() -> Publisher:
    return Publisher(
        "postgres://unused",
        "test",
        **_extractor_config(),
    )


def test_chronos_extractor_stage_signature_includes_config() -> None:
    stage = _stage()

    signature = stage.input_signature()

    assert signature is not None
    assert signature["version"] == 1
    assert signature["source_subtypes"] == ["page"]
    assert len(signature["extractor_fingerprint"]) == 64


def test_publisher_chronos_extractor_signature_uses_stage() -> None:
    publisher = _publisher()
    stage = _stage()

    assert publisher._chronos_extractor_input_signature() == stage.input_signature()


def test_chronos_extractor_stage_skip_uses_signature_and_dirty_probe() -> None:
    stage = _stage()
    signature = stage.input_signature()

    assert signature is not None
    calls = []
    previous = SimpleNamespace(metadata={"extractor_input_signature": signature})

    should_run = stage.should_run(
        previous=previous,
        input_signature=signature,
        dirty_probe=lambda: calls.append("dirty") or False,
    )
    assert should_run is False
    assert calls == ["dirty"]


def test_chronos_extractor_skip_uses_signature_and_dirty_probe(
    monkeypatch,
) -> None:
    publisher = _publisher()
    signature = publisher._chronos_extractor_input_signature()
    assert signature is not None
    calls = []

    def fake_dirty_probe(conn, *, branch_name):
        calls.append((conn, branch_name))
        return False

    monkeypatch.setattr(
        publisher,
        "_chronos_current_segment_has_extractor_input_changes",
        fake_dirty_probe,
    )

    conn = object()
    previous = SimpleNamespace(metadata={"extractor_input_signature": signature})

    assert publisher._should_run_chronos_extractor(
        conn,
        previous=previous,
        input_signature=signature,
        branch_name="default",
    ) is False
    assert calls == [(conn, "default")]


def test_chronos_extractor_runs_when_signature_changes(monkeypatch) -> None:
    publisher = _publisher()
    signature = publisher._chronos_extractor_input_signature()
    assert signature is not None

    def fail_if_called(conn, *, branch_name):  # pragma: no cover
        raise AssertionError("dirty probe should not run on signature mismatch")

    monkeypatch.setattr(
        publisher,
        "_chronos_current_segment_has_extractor_input_changes",
        fail_if_called,
    )
    previous = SimpleNamespace(
        metadata={"extractor_input_signature": {**signature, "version": 0}},
    )

    assert publisher._should_run_chronos_extractor(
        object(),
        previous=previous,
        input_signature=signature,
        branch_name="default",
    ) is True


def test_chronos_extractor_probe_fails_closed() -> None:
    publisher = _publisher()

    with pytest.raises(AttributeError, match="cursor"):
        publisher._chronos_current_segment_has_extractor_input_changes(
            object(),
            branch_name="default",
        )


def test_clean_enricher_context_reuses_cycle_inputs(monkeypatch) -> None:
    class _Cursor:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def execute(self, *args, **kwargs):
            return None

    class _Conn:
        def cursor(self):
            return _Cursor()

    class _CleanEnricher:
        publisher_skip_when_clean = True
        reads_subtypes = ("source_file",)
        reads_edge_types = ()
        requires_narrowings = ()

        def __init__(self, name: str) -> None:
            self.name = name

        def can_skip_clean(self, conn, ctx) -> bool:
            return True

        def enrich(self, conn, *, generation_id: int, incremental=None):
            raise AssertionError("clean enricher should have been skipped")

    publisher = _publisher()
    publisher.enrichers = [_CleanEnricher("a"), _CleanEnricher("b")]
    monkeypatch.setattr(
        "okg.substrate.publisher.publisher.persist_transform_program",
        lambda conn, program: None,
    )
    verdicts = []
    monkeypatch.setattr(
        publisher,
        "_emit_derived_verdict_event",
        lambda payload: verdicts.append(payload),
    )
    calls = {
        "watermarks": 0,
        "catalog": 0,
        "dirty": 0,
        "progress": 0,
        "profiles": 0,
    }
    advances = []

    def fake_progress(conn, enricher_names):
        calls["progress"] += 1
        return {
            name: (5, 6, "success", None, 7)
            for name in enricher_names
        }

    def fake_profiles(conn, producer_ids):
        calls["profiles"] += 1
        return {producer_id: f"fp:{producer_id}" for producer_id in producer_ids}

    def fake_watermarks(conn):
        calls["watermarks"] += 1
        return 5, 6

    def fake_catalog(conn):
        calls["catalog"] += 1
        return 7

    def fake_dirty(
        conn, *, branch_name, previous=None, timing_sink=None, timing_prefix=None,
    ):
        calls["dirty"] += 1
        assert previous is None
        return {"source_file": []}, {}, True

    def fake_advance(conn, ename, **kwargs):
        advances.append((ename, kwargs))

    monkeypatch.setattr(
        publisher, "_load_enricher_progress_rows", fake_progress,
    )
    monkeypatch.setattr(
        publisher, "_load_enricher_profile_fingerprints", fake_profiles,
    )
    monkeypatch.setattr(publisher, "_current_fact_watermarks", fake_watermarks)
    monkeypatch.setattr(publisher, "_current_catalog_version_id", fake_catalog)
    monkeypatch.setattr(
        publisher, "_query_dirty_from_chronos_current_segment_by_type",
        fake_dirty,
    )
    monkeypatch.setattr(
        publisher, "_advance_enricher_progress", fake_advance,
    )

    runs = publisher._run_enrichers(
        _Conn(),
        generation_id=123,
        catalog_version_id=7,
    )

    assert calls == {
        "watermarks": 1,
        "catalog": 1,
        "dirty": 1,
        "progress": 1,
        "profiles": 1,
    }
    assert [run["status"] for run in runs[1:]] == ["skipped", "skipped"]
    assert [run["profile_fingerprint"] for run in runs[1:]] == [
        "fp:_enricher:a",
        "fp:_enricher:b",
    ]
    assert len(verdicts) == 2
    assert [
        (
            name,
            kwargs["current_fact_watermark"],
            kwargs["current_edge_watermark"],
        )
        for name, kwargs in advances
    ] == [("a", 5, 6), ("b", 5, 6)]


def test_admit_enricher_outputs_can_reuse_materialized_profile(
    monkeypatch,
) -> None:
    class _Enricher:
        name = "already_profiled"

    publisher = _publisher()
    materialize_calls = []
    admit_calls = []
    monkeypatch.setattr(
        publisher,
        "_materialize_enricher_profile",
        lambda *args, **kwargs: materialize_calls.append(kwargs),
    )
    monkeypatch.setattr(
        publisher,
        "_admit_derived_edge_outputs",
        lambda *args, **kwargs: admit_calls.append(kwargs),
    )

    publisher._admit_enricher_outputs(
        object(),
        enricher=_Enricher(),
        generation_id=123,
        catalog_version_id=7,
        prev_edge_fact_watermark=6,
        profile_already_materialized=True,
    )

    assert materialize_calls == []
    assert admit_calls and admit_calls[0]["source_prefix"] == (
        "_enricher:already_profiled"
    )


def test_dirty_enricher_reuses_matching_loaded_profile(monkeypatch) -> None:
    class _Cursor:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def execute(self, *args, **kwargs):
            return None

    class _Conn:
        def cursor(self):
            return _Cursor()

    class _DirtyEnricher:
        name = "dirty"
        publisher_skip_when_clean = True
        reads_subtypes = ("pact_change",)
        reads_edge_types = ()
        requires_narrowings = ()

        def can_skip_clean(self, conn, ctx) -> bool:
            return False

        def enrich(self, conn, *, generation_id: int, incremental=None):
            return SimpleNamespace(
                enricher_name=self.name,
                n_edges_emitted=0,
                n_edges_skipped=0,
            )

    publisher = _publisher()
    publisher.enrichers = [_DirtyEnricher()]
    monkeypatch.setattr(
        "okg.substrate.publisher.publisher.persist_transform_program",
        lambda conn, program: None,
    )
    monkeypatch.setattr(
        publisher,
        "_load_enricher_progress_rows",
        lambda conn, names: {"dirty": (5, 6, "success", None, 7)},
    )
    monkeypatch.setattr(
        publisher,
        "_load_enricher_profile_fingerprints",
        lambda conn, producer_ids: {"_enricher:dirty": "fp:dirty"},
    )
    monkeypatch.setattr(
        publisher,
        "_current_fact_watermarks",
        lambda conn: (5, 6),
    )
    monkeypatch.setattr(
        publisher,
        "_current_catalog_version_id",
        lambda conn: 7,
    )
    monkeypatch.setattr(
        publisher,
        "_query_dirty_from_chronos_current_segment_by_type",
        lambda conn, *, branch_name, previous=None, timing_sink=None, timing_prefix=None: (
            {"pact_change": ["pact-change:x"]},
            {},
            True,
            {"pact_change": {"pact-change:x": ["state_digest"]}},
        ),
    )
    monkeypatch.setattr(
        publisher,
        "_expected_enricher_profile_fingerprint",
        lambda conn, *, enricher, producer_id, catalog_version_id: "fp:dirty",
    )
    materialize_calls = []
    admit_calls = []
    monkeypatch.setattr(
        publisher,
        "_materialize_enricher_profile",
        lambda *args, **kwargs: materialize_calls.append(kwargs) or "new",
    )
    monkeypatch.setattr(
        publisher,
        "_admit_enricher_outputs",
        lambda *args, **kwargs: admit_calls.append(kwargs),
    )
    monkeypatch.setattr(
        publisher,
        "_advance_enricher_progress",
        lambda *args, **kwargs: None,
    )

    runs = publisher._run_enrichers(
        _Conn(),
        generation_id=123,
        catalog_version_id=7,
    )

    assert materialize_calls == []
    assert admit_calls
    assert runs[1]["status"] == "ok"
    assert runs[1]["profile_fingerprint"] == "fp:dirty"
    assert runs[1]["timings_ms"]["profile_reuse"] == 0.0
