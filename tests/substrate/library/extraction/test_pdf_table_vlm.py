from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, replace
from typing import Any, Sequence

import pytest

from okg.substrate.library.extraction.latex_table_grid import parse_latex_table_grid
from okg.substrate.library.extraction.pdf_table_vlm import (
    MAX_CROP_BYTES_HARD,
    MAX_CROP_PIXELS_HARD,
    MAX_DPI_HARD,
    MAX_OUTPUT_TOKENS_HARD,
    MAX_PAGES_HARD,
    MAX_REGIONS_PER_PAGE_HARD,
    MAX_TABLES_PER_RUN_HARD,
    MAX_TIMEOUT_SECONDS_HARD,
    MemoryTableResponseCache,
    PdfTableConfigError,
    PdfTableResponseError,
    PdfTableTranscriptionConfig,
    PdfTableTranscriptionService,
    TableCacheSignatureComponents,
    TableRegion,
    TableRegionCandidate,
    build_table_transcription_provider,
    parse_table_provider_response,
    prepare_table_regions,
)


def _grid_mapping() -> dict[str, Any]:
    return {
        "schema_version": 1,
        "column_count": 2,
        "col_headers": ["Method", "Score"],
        "row_label_column_index": 0,
        "rows": [
            {
                "row_index": 0,
                "row_label": "Alpha",
                "cells": [
                    {
                        "column_index": 0,
                        "column_header": "Method",
                        "text": "Alpha",
                        "row_span": 1,
                        "column_span": 1,
                    },
                    {
                        "column_index": 1,
                        "column_header": "Score",
                        "text": "10",
                        "row_span": 1,
                        "column_span": 1,
                    },
                ],
            }
        ],
    }


def _provider_response(*, fenced: bool = False) -> str:
    raw = json.dumps({"table_grid": _grid_mapping()}, ensure_ascii=False)
    return f"```json\n{raw}\n```" if fenced else raw


def _config(**changes: Any) -> PdfTableTranscriptionConfig:
    values: dict[str, Any] = {
        "enabled": True,
        "provider": "injected",
        "model": "recorded/table-v1",
        "prompt_version": "prompt-v1",
        "operator_version": "operator-v1",
        "dpi": 72,
        "max_pages": 10,
        "max_regions_per_page": 5,
        "max_tables_per_run": 10,
        "max_crop_pixels": 100_000,
        "max_crop_bytes": 10_000,
        "timeout_seconds": 30.0,
        "max_output_tokens": 512,
        "retry_count": 0,
        "temperature": 0.0,
        "tracking_mode": "disabled",
    }
    values.update(changes)
    return PdfTableTranscriptionConfig(**values)


def _candidate(
    *,
    page: int = 0,
    bbox: tuple[Any, Any, Any, Any] = (0, 0, 10, 20),
    detector: str = "fixture",
    detector_version: str = "v1",
) -> TableRegionCandidate:
    return TableRegionCandidate(
        page_index=page,
        bbox=bbox,
        page_width=100,
        page_height=200,
        detector_name=detector,
        detector_version=detector_version,
    )


@dataclass
class _Locator:
    candidates: Sequence[TableRegionCandidate]
    raises: Exception | None = None
    name: str = "fixture-locator"
    version: str = "v1"
    calls: int = 0

    def locate(
        self, _pdf_bytes: bytes, *, max_pages: int
    ) -> Sequence[TableRegionCandidate]:
        self.calls += 1
        assert max_pages > 0
        if self.raises is not None:
            raise self.raises
        return self.candidates


@dataclass
class _Renderer:
    crop: bytes = b"png-crop"
    raises: Exception | None = None
    name: str = "fixture-renderer"
    version: str = "v1"
    calls: int = 0

    def render_crop(self, _pdf: bytes, region: TableRegion, *, dpi: int) -> bytes:
        self.calls += 1
        assert region.source_locator.startswith("pdf:")
        assert dpi > 0
        if self.raises is not None:
            raise self.raises
        return self.crop


@dataclass
class _Provider:
    response: str = ""
    raises: Exception | None = None
    model: str = "recorded/table-v1"
    external_network: bool = False
    calls: list[dict[str, Any]] | None = None

    def __post_init__(self) -> None:
        if self.calls is None:
            self.calls = []

    def transcribe(
        self,
        crop_bytes: bytes,
        prompt: str,
        *,
        fragment_id: str,
        outcome_key_digest: str,
    ) -> str:
        assert self.calls is not None
        self.calls.append(
            {
                "crop": crop_bytes,
                "prompt": prompt,
                "fragment_id": fragment_id,
                "outcome_key_digest": outcome_key_digest,
            }
        )
        if self.raises is not None:
            raise self.raises
        return self.response


def _service(
    *,
    config: PdfTableTranscriptionConfig | None = None,
    locator: _Locator | None = None,
    renderer: _Renderer | None = None,
    provider: _Provider | None = None,
    cache: MemoryTableResponseCache | None = None,
    deployment: str | None = None,
    settlement_dsn: str | None = None,
) -> tuple[
    PdfTableTranscriptionService,
    _Locator,
    _Renderer,
    _Provider,
    MemoryTableResponseCache,
]:
    locator = locator or _Locator([_candidate()])
    renderer = renderer or _Renderer()
    provider = provider or _Provider(response=_provider_response())
    cache = cache or MemoryTableResponseCache()
    service = PdfTableTranscriptionService(
        config=config or _config(),
        locator=locator,
        renderer=renderer,
        provider=provider,
        cache=cache,
        deployment=deployment,
        settlement_dsn=settlement_dsn,
    )
    return service, locator, renderer, provider, cache


def test_enabled_configuration_must_name_all_bounds_and_never_accepts_secrets() -> None:
    authored = _config().public_mapping()
    loaded = PdfTableTranscriptionConfig.from_mapping(authored)
    assert loaded == _config()
    assert not any(
        token in key.lower()
        for key in loaded.public_mapping()
        for token in ("secret", "password", "credential", "api_key")
    )

    incomplete = dict(authored)
    incomplete.pop("max_crop_bytes")
    with pytest.raises(PdfTableConfigError, match="must name every bounded field"):
        PdfTableTranscriptionConfig.from_mapping(incomplete)

    secret = dict(authored)
    secret["api_key"] = "must-not-persist"
    with pytest.raises(PdfTableConfigError, match="must not contain secret"):
        PdfTableTranscriptionConfig.from_mapping(secret)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("max_pages", MAX_PAGES_HARD + 1),
        ("max_regions_per_page", MAX_REGIONS_PER_PAGE_HARD + 1),
        ("max_tables_per_run", MAX_TABLES_PER_RUN_HARD + 1),
        ("dpi", MAX_DPI_HARD + 1),
        ("max_crop_pixels", MAX_CROP_PIXELS_HARD + 1),
        ("max_crop_bytes", MAX_CROP_BYTES_HARD + 1),
        ("timeout_seconds", MAX_TIMEOUT_SECONDS_HARD + 0.1),
        ("max_output_tokens", MAX_OUTPUT_TOKENS_HARD + 1),
        ("retry_count", 1),
        ("temperature", 0.1),
    ],
)
def test_configuration_hard_ceilings_fail_before_emission(
    field: str, value: Any
) -> None:
    with pytest.raises(PdfTableConfigError):
        _config(**{field: value})


def test_disabled_extraction_performs_no_location_render_or_provider_call() -> None:
    locator = _Locator([_candidate()])
    renderer = _Renderer()
    provider = _Provider(response=_provider_response())
    service = PdfTableTranscriptionService(
        config=PdfTableTranscriptionConfig(),
        locator=locator,
        renderer=renderer,
        provider=provider,
    )

    result = service.extract_document(b"pdf")
    assert result.occurrences == ()
    assert (locator.calls, renderer.calls, len(provider.calls or [])) == (0, 0, 0)


def test_regions_clip_quantize_deduplicate_sort_then_apply_caps() -> None:
    candidates = [
        _candidate(page=1, bbox=(20, 20, 30, 40), detector="z"),
        _candidate(page=0, bbox=(-2, -1, 10.0004, 20.0004), detector="z"),
        _candidate(page=0, bbox=(0, 0, 10.00049, 20.00049), detector="a"),
        _candidate(page=0, bbox=(40, 40, 40, 50)),
        _candidate(page=20, bbox=(0, 0, 10, 10)),
    ]
    plan = prepare_table_regions(candidates, config=_config(), remaining_tables=2)

    assert [region.source_locator for region in plan.regions] == [
        "pdf:0:0.000,0.000,10.000,20.000",
        "pdf:1:20.000,20.000,30.000,40.000",
    ]
    assert plan.regions[0].detector_name == "a"
    assert set(plan.warnings) == {
        "table_region_invalid",
        "table_region_budget_exhausted",
    }


def test_region_caps_follow_visual_y_x_order_not_bbox_storage_order() -> None:
    candidates = [
        _candidate(page=0, bbox=(0, 20, 5, 25)),
        _candidate(page=0, bbox=(10, 0, 15, 5)),
    ]
    plan = prepare_table_regions(candidates, config=_config(), remaining_tables=1)

    assert [region.source_locator for region in plan.regions] == [
        "pdf:0:10.000,0.000,15.000,5.000"
    ]
    assert plan.warnings == ("table_region_budget_exhausted",)


def test_extreme_decimal_candidate_is_rejected_without_aborting_document() -> None:
    candidate = TableRegionCandidate(
        page_index=0,
        bbox=(0, 0, "1e999999", "1e999999"),
        page_width="1e999999",
        page_height="1e999999",
        detector_name="fixture",
        detector_version="v1",
    )
    plan = prepare_table_regions(
        [candidate],
        config=_config(),
        remaining_tables=1,
    )

    assert plan.regions == ()
    assert plan.warnings == ("table_region_invalid",)


def test_tolerant_envelopes_feed_one_strict_grid_validator() -> None:
    direct = json.dumps(_grid_mapping())
    wrapped = _provider_response(fenced=True)
    nested = json.dumps({"result": {"table_grid": _grid_mapping()}})
    grids = [
        parse_table_provider_response(value) for value in (direct, wrapped, nested)
    ]

    assert (
        grids[0].canonical_bytes()
        == grids[1].canonical_bytes()
        == grids[2].canonical_bytes()
    )
    with pytest.raises(PdfTableResponseError) as exc_info:
        parse_table_provider_response("not json")
    assert exc_info.value.code == "invalid_provider_response"
    with pytest.raises(PdfTableResponseError) as exc_info:
        parse_table_provider_response('{"table_grid":{"schema_version":1}}')
    assert exc_info.value.code == "invalid_table_grid"


def test_injected_provider_yields_same_grid_as_deterministic_lane_and_is_keyed() -> (
    None
):
    service, _locator, _renderer, provider, _cache = _service()
    result = service.extract_document(b"pdf-fixture")
    occurrence = result.occurrences[0]
    latex = parse_latex_table_grid(
        r"\begin{tabular}{lc}Method & Score \\ Alpha & 10 \\ \end{tabular}"
    )

    assert occurrence.result.status == "parsed"
    assert occurrence.result.grid is not None
    assert occurrence.result.grid.canonical_bytes() == latex.canonical_bytes()
    assert result.provider_invocations == 1
    assert result.external_network_attempts == 0
    assert result.cache_misses == 1
    assert len(provider.calls or []) == 1
    assert len((provider.calls or [])[0]["outcome_key_digest"]) == 64
    attrs = occurrence.to_chunk_extras()
    assert attrs["source_bbox"] == ["0.000", "0.000", "10.000", "20.000"]
    assert not any("secret" in key or "token" in key for key in attrs)


def test_exact_cache_hit_causes_zero_provider_and_network_attempts() -> None:
    service, _locator, renderer, provider, cache = _service()
    first = service.extract_document(b"pdf-fixture")
    second = service.extract_document(b"pdf-fixture")

    assert first.cache_misses == 1 and first.provider_invocations == 1
    assert second.cache_hits == 1
    assert second.provider_invocations == 0
    assert second.external_network_attempts == 0
    assert second.occurrences[0].cache_hit is True
    assert len(provider.calls or []) == 1
    assert renderer.calls == 2
    assert len(cache.values) == 1


def test_uncached_external_provider_has_exactly_one_network_attempt() -> None:
    provider = _Provider(raises=RuntimeError("provider failure"), external_network=True)
    service, *_ = _service(provider=provider)
    result = service.extract_document(b"pdf")

    assert result.provider_invocations == 1
    assert result.external_network_attempts == 1
    assert len(provider.calls or []) == 1
    assert result.occurrences[0].result.error_code == "provider_exception"


def test_cache_signature_invalidates_on_every_contract_component() -> None:
    digest = hashlib.sha256(b"x").hexdigest()
    base = TableCacheSignatureComponents(
        document_content_sha256=digest,
        crop_sha256=digest,
        source_locator="pdf:0:0.000,0.000,10.000,20.000",
        detector_name="detector",
        detector_version="v1",
        renderer_name="renderer",
        renderer_version="v1",
        dpi=72,
        provider="injected",
        model="recorded/v1",
        prompt_digest=digest,
        prompt_version="p1",
        operator_version="o1",
        max_output_tokens=512,
        timeout_seconds=30.0,
        retry_count=0,
        temperature=0.0,
    )
    changes = {
        "document_content_sha256": hashlib.sha256(b"doc2").hexdigest(),
        "crop_sha256": hashlib.sha256(b"crop2").hexdigest(),
        "source_locator": "pdf:0:0.000,0.000,11.000,20.000",
        "detector_name": "detector2",
        "detector_version": "v2",
        "renderer_name": "renderer2",
        "renderer_version": "v2",
        "dpi": 73,
        "provider": "litellm",
        "model": "recorded/v2",
        "prompt_digest": hashlib.sha256(b"prompt2").hexdigest(),
        "prompt_version": "p2",
        "operator_version": "o2",
        "max_output_tokens": 513,
        "timeout_seconds": 31.0,
        "retry_count": 1,
        "temperature": 0.1,
        "response_parser_version": "parser-v2",
        "table_grid_schema_version": 2,
        "normalization_version": "normalization-v2",
    }
    for field, value in changes.items():
        assert replace(base, **{field: value}).signature() != base.signature(), field


@pytest.mark.parametrize(
    ("config", "renderer", "provider", "status", "code"),
    [
        (
            _config(),
            _Renderer(raises=RuntimeError("render")),
            _Provider(response=_provider_response()),
            "parse_failed",
            "render_failed",
        ),
        (
            _config(max_crop_pixels=100),
            _Renderer(),
            _Provider(response=_provider_response()),
            "budget_exhausted",
            "crop_budget_exceeded",
        ),
        (
            _config(max_crop_bytes=2),
            _Renderer(crop=b"too large"),
            _Provider(response=_provider_response()),
            "budget_exhausted",
            "crop_budget_exceeded",
        ),
        (
            _config(),
            _Renderer(),
            _Provider(raises=TimeoutError("timeout")),
            "provider_failed",
            "provider_timeout",
        ),
        (
            _config(),
            _Renderer(),
            _Provider(raises=RuntimeError("provider")),
            "provider_failed",
            "provider_exception",
        ),
        (
            _config(),
            _Renderer(),
            _Provider(response="not json"),
            "parse_failed",
            "invalid_provider_response",
        ),
        (
            _config(),
            _Renderer(),
            _Provider(response='{"table_grid":{"schema_version":1}}'),
            "parse_failed",
            "invalid_table_grid",
        ),
    ],
)
def test_content_local_failures_emit_one_typed_occurrence_and_continue(
    config: PdfTableTranscriptionConfig,
    renderer: _Renderer,
    provider: _Provider,
    status: str,
    code: str,
) -> None:
    service, *_ = _service(config=config, renderer=renderer, provider=provider)
    result = service.extract_document(b"pdf")
    occurrence = result.occurrences[0]

    assert occurrence.result.status == status
    assert occurrence.result.error_code == code
    assert occurrence.result.grid is None
    assert "table_grid" not in occurrence.to_chunk_extras()


def test_locator_failure_missing_regions_and_caps_are_document_warnings() -> None:
    failed, *_ = _service(locator=_Locator([], raises=RuntimeError("locator")))
    result = failed.extract_document(b"pdf")
    assert result.occurrences == ()
    assert result.warnings == ("table_region_locator_failed",)

    missing, *_ = _service(locator=_Locator([]))
    result = missing.extract_document(b"pdf")
    assert result.occurrences == ()
    assert result.warnings == ("table_region_not_found",)

    capped, *_ = _service(
        config=_config(max_regions_per_page=1),
        locator=_Locator(
            [_candidate(bbox=(0, 0, 10, 10)), _candidate(bbox=(20, 0, 30, 10))]
        ),
    )
    result = capped.extract_document(b"pdf")
    assert len(result.occurrences) == 1
    assert result.warnings == ("table_region_budget_exhausted",)

    run_capped, *_ = _service(config=_config(max_tables_per_run=1))
    assert len(run_capped.extract_document(b"first").occurrences) == 1
    second = run_capped.extract_document(b"second")
    assert second.occurrences == ()
    assert second.warnings == ("table_region_budget_exhausted",)


def test_invalid_cached_response_never_synthesizes_a_grid_or_calls_provider() -> None:
    service, _locator, _renderer, provider, cache = _service()
    first = service.extract_document(b"pdf")
    signature = first.occurrences[0].result.cache_signature
    assert signature is not None
    cache.values[signature] = "not json"
    second = service.extract_document(b"pdf")

    assert second.occurrences[0].result.status == "parse_failed"
    assert second.occurrences[0].result.error_code == "invalid_provider_response"
    assert second.provider_invocations == 0
    assert len(provider.calls or []) == 1


def test_litellm_adapter_constructs_one_attempt_with_tracking_and_output_cap(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, Any] = {}

    class FakeVLM:
        def __init__(self, **kwargs: Any) -> None:
            captured.update(kwargs)

        def complete_image(self, *_args: Any, **kwargs: Any) -> str:
            captured["call"] = kwargs
            return _provider_response()

    monkeypatch.setattr("okg.views.execution.llm.VLMClient", FakeVLM)
    config = _config(
        provider="litellm",
        model="openai/gpt-fixture",
        tracking_mode="required",
    )
    provider = build_table_transcription_provider(
        config,
        tracking_dsn="postgres://tracking",
        deployment="fixture",
    )
    assert provider is not None and provider.external_network is True
    assert captured["max_retries"] == 1
    assert captured["max_output_tokens"] == config.max_output_tokens
    provider.transcribe(
        b"crop",
        "prompt",
        fragment_id="pdf:0:0.000,0.000,1.000,1.000",
        outcome_key_digest="digest",
    )
    assert captured["call"]["caller"] == "doc_corpus:table_transcription"
    assert captured["call"]["outcome_key_digest"] == "digest"


def test_litellm_adapter_inherits_runtime_dsn_when_tracking_is_optional(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, Any] = {}

    class FakeVLM:
        def __init__(self, **kwargs: Any) -> None:
            captured.update(kwargs)

    monkeypatch.setattr("okg.views.execution.llm.VLMClient", FakeVLM)
    provider = build_table_transcription_provider(
        _config(provider="litellm", tracking_mode="optional"),
        tracking_dsn="postgresql://user:p%40ss@db.example/one",
    )
    assert provider is not None
    assert captured["tracking_dsn"] == "postgresql://user:p%40ss@db.example/one"


def _four_region_locator() -> _Locator:
    return _Locator(
        [_candidate(bbox=(0, y, 10, y + 10)) for y in (0, 20, 40, 60)]
    )


def test_spend_config_is_both_or_neither_and_positive() -> None:
    config = _config(spend_cap_usd=1.0, spend_estimate_usd=0.25)
    assert (config.spend_cap_usd, config.spend_estimate_usd) == (1.0, 0.25)

    with pytest.raises(PdfTableConfigError, match="declared together"):
        _config(spend_cap_usd=1.0)
    with pytest.raises(PdfTableConfigError, match="declared together"):
        _config(spend_estimate_usd=0.25)
    # Half a spend contract is malformed even on a disabled lane: the author
    # believes a cap exists, and it would not.
    with pytest.raises(PdfTableConfigError, match="declared together"):
        PdfTableTranscriptionConfig(enabled=False, spend_cap_usd=1.0)
    for bad in (0.0, -1.0, float("nan"), float("inf"), True, "1.0"):
        with pytest.raises(PdfTableConfigError, match="positive finite"):
            _config(spend_cap_usd=bad, spend_estimate_usd=0.25)
        with pytest.raises(PdfTableConfigError, match="positive finite"):
            _config(spend_cap_usd=1.0, spend_estimate_usd=bad)


def test_enabled_mapping_keeps_spend_optional_but_never_half_declared() -> None:
    authored = {
        key: value
        for key, value in _config().public_mapping().items()
        if key not in {"spend_cap_usd", "spend_estimate_usd"}
    }
    # Undeclared spend is a valid enabled config — the lane is honestly
    # unbounded, not misauthored.
    loaded = PdfTableTranscriptionConfig.from_mapping(authored)
    assert loaded.spend_cap_usd is None and loaded.spend_estimate_usd is None

    declared = dict(authored)
    declared["spend_cap_usd"] = 2.0
    declared["spend_estimate_usd"] = 0.5
    loaded = PdfTableTranscriptionConfig.from_mapping(declared)
    assert (loaded.spend_cap_usd, loaded.spend_estimate_usd) == (2.0, 0.5)

    with pytest.raises(PdfTableConfigError, match="declared together"):
        PdfTableTranscriptionConfig.from_mapping(
            {**authored, "spend_cap_usd": 2.0}
        )


def test_spend_cap_requires_a_deployment_identity() -> None:
    with pytest.raises(PdfTableConfigError, match="deployment identity"):
        _service(config=_config(spend_cap_usd=1.0, spend_estimate_usd=0.25))


def test_spend_cap_refusal_skips_provider_and_records_the_region() -> None:
    service, _locator, _renderer, provider, _cache = _service(
        config=_config(spend_cap_usd=0.5, spend_estimate_usd=0.25),
        locator=_four_region_locator(),
        deployment="fixture-deploy",
    )
    result = service.extract_document(b"pdf-fixture")

    # Every planned region is accounted for: the two the cap admitted parsed,
    # the two it refused became typed occurrences, none were dropped.
    assert len(result.occurrences) == 4
    assert [item.result.status for item in result.occurrences] == [
        "parsed",
        "parsed",
        "budget_exhausted",
        "budget_exhausted",
    ]
    assert [
        item.result.error_code
        for item in result.occurrences
        if item.result.status == "budget_exhausted"
    ] == ["spend_cap_exhausted", "spend_cap_exhausted"]
    assert result.provider_invocations == 2
    assert len(provider.calls or []) == 2


def test_undeclared_spend_leaves_lane_unbounded_and_transcribes_all() -> None:
    service, _locator, _renderer, provider, _cache = _service(
        locator=_four_region_locator(),
    )
    result = service.extract_document(b"pdf-fixture")

    assert [item.result.status for item in result.occurrences] == ["parsed"] * 4
    assert result.provider_invocations == 4
    assert len(provider.calls or []) == 4


def test_settlement_charges_dispatched_calls_only_and_skips_loudly_sans_dsn(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    class _FakeConn:
        def __enter__(self) -> "_FakeConn":
            return self

        def __exit__(self, *_exc: Any) -> bool:
            return False

    connects: list[str] = []
    # Settlement goes through the sanctioned wrapper (it stamps an
    # application_name so the connection is attributable), so the fake
    # intercepts there — patching raw psycopg.connect would miss it.
    monkeypatch.setattr(
        "okg.substrate.db.connection.connect",
        lambda dsn, *, autocommit=False, application_name=None,
        connect_timeout=None: (connects.append(dsn), _FakeConn())[1],
    )
    service, _locator, _renderer, provider, _cache = _service(
        config=_config(spend_cap_usd=10.0, spend_estimate_usd=0.25),
        locator=_Locator(
            [_candidate(bbox=(0, 0, 10, 10)), _candidate(bbox=(0, 20, 10, 30))]
        ),
        deployment="fixture-deploy",
        settlement_dsn="postgres://settlement",
    )
    first = service.extract_document(b"pdf-fixture")
    assert first.provider_invocations == 2
    # Settlement happened on the calling thread, over one connection, and
    # charged exactly the two dispatched calls (the fake connection makes the
    # ledger write fail loudly, but the settled total is still counted).
    assert connects == ["postgres://settlement"]
    assert service._spend.settled_usd == pytest.approx(0.5)

    second = service.extract_document(b"pdf-fixture")
    assert second.cache_hits == 2 and second.provider_invocations == 0
    # Cache hits never RESERVE (reservation happens at dispatch time, after
    # the cache check), so a cache-heavy re-run charges no headroom and no
    # ledger row — up-front reservation used to starve the uncached tail.
    assert connects == ["postgres://settlement"]
    assert service._spend.settled_usd == pytest.approx(0.5)
    assert service._spend.reserved_usd == pytest.approx(0.5)

    no_dsn, *_ = _service(
        config=_config(spend_cap_usd=10.0, spend_estimate_usd=0.25),
        deployment="fixture-deploy",
    )
    with caplog.at_level("ERROR"):
        no_dsn.extract_document(b"pdf-fixture")
    assert "settlement SKIPPED" in caplog.text


def test_provider_construction_fails_loudly_before_source_emission() -> None:
    with pytest.raises(PdfTableConfigError, match="requires an injected provider"):
        build_table_transcription_provider(_config(), injected_provider=None)
    with pytest.raises(
        PdfTableConfigError, match="runtime database authority is unresolved"
    ) as error:
        build_table_transcription_provider(
            _config(provider="litellm", tracking_mode="required"),
            tracking_dsn="${WORKER_DSN}",
        )
    assert error.value.code == "pdf_table_config"
    assert error.value.data == {
        "underlying_code": "llm_tracking_configuration",
        "component": "PDF table transcription",
        "reason": "unresolved_runtime_dsn",
    }
