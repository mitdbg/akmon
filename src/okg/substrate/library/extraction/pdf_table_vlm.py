"""Bounded rendered-PDF table location and transcription contract.

Region location, crop rendering, provider invocation, response parsing, and
cache identity are separate injectable boundaries.  The module imports no PDF
or model-provider package; production adapters are constructed explicitly.
"""

from __future__ import annotations

import hashlib
import json
import logging
import math
import re
from dataclasses import asdict, dataclass, field
from decimal import Decimal, InvalidOperation
from typing import (
    Any,
    Callable,
    Final,
    Mapping,
    Optional,
    Protocol,
    Sequence,
    runtime_checkable,
)

from okg.substrate.library.extraction.table_grid import (
    TABLE_GRID_SCHEMA_VERSION,
    TableExtractionResult,
    TableGridError,
    TableGridV1,
    canonical_pdf_bbox,
    pdf_table_locator,
)
from okg.substrate.llm.outcomes import OutcomeKey, prompt_content_hash
from okg.substrate.llm.spend import ModelCallSpend, SpendAdmission
from okg.substrate.llm.tracking import (
    TrackingConfigurationError,
    resolve_tracking_dsn,
)


log = logging.getLogger(__name__)

MAX_PAGES_HARD: Final = 200
MAX_REGIONS_PER_PAGE_HARD: Final = 20
MAX_TABLES_PER_RUN_HARD: Final = 100
MAX_DPI_HARD: Final = 300
MAX_CROP_PIXELS_HARD: Final = 20_000_000
MAX_CROP_BYTES_HARD: Final = 20 << 20
MAX_TIMEOUT_SECONDS_HARD: Final = 120.0
MAX_OUTPUT_TOKENS_HARD: Final = 8_192
TABLE_RESPONSE_PARSER_VERSION: Final = "table-response-v1"
TABLE_NORMALIZATION_VERSION: Final = "table-grid-nfc-whitespace-v1"
DEFAULT_TABLE_PROMPT: Final = (
    "Transcribe only the visible table in this crop. Return JSON containing "
    "one `table_grid` object with schema_version, column_count, col_headers, "
    "row_label_column_index, and rows. Preserve visible text; do not infer "
    "missing cells or add prose."
)

_FENCE_RE = re.compile(r"^```(?:json)?\s*(.*?)\s*```$", re.DOTALL | re.IGNORECASE)
_SHA256_RE = re.compile(r"[0-9a-f]{64}\Z")
_SENSITIVE_KEY_RE = re.compile(
    r"(?:api[_-]?key|access[_-]?token|auth[_-]?token|bearer[_-]?token|"
    r"secret|password|credential)",
    re.IGNORECASE,
)
_CONFIG_KEYS = frozenset(
    {
        "enabled",
        "provider",
        "model",
        "prompt_version",
        "operator_version",
        "dpi",
        "max_pages",
        "max_regions_per_page",
        "max_tables_per_run",
        "max_crop_pixels",
        "max_crop_bytes",
        "timeout_seconds",
        "max_output_tokens",
        "retry_count",
        "temperature",
        "tracking_mode",
        "spend_cap_usd",
        "spend_estimate_usd",
    }
)
# Spend keys stay OUTSIDE the "enabled must name every bounded field" set: a
# lane that declares neither is honestly unbounded by design, and `ModelCallSpend`
# reports exactly that. Forcing the pair into the required set would turn "no
# cap declared" into an authoring error, which invites operators to invent a
# number nobody chose — the silent-fallback failure mode spend admission
# exists to avoid. Declaring exactly one of them IS an authoring error.
_SPEND_CONFIG_KEYS = frozenset({"spend_cap_usd", "spend_estimate_usd"})
# The cost_budget scope the VLM table lane charges to. It matches the caller
# stamp `_LiteLLMTableProvider` writes on tracked llm_calls rows, so a ledger
# row and its call telemetry name the same lane.
TABLE_SPEND_SCOPE: Final = "vlm-table:doc_corpus:table_transcription"


class PdfTableConfigError(ValueError):
    """Enabled table transcription configuration is malformed."""

    code = "pdf_table_config"

    def __init__(
        self,
        message: str,
        *,
        underlying_code: str | None = None,
        data: Mapping[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.data = {
            key: value
            for key, value in {
                "underlying_code": underlying_code,
                **(dict(data) if data is not None else {}),
            }.items()
            if value is not None
        }


class PdfTableResponseError(ValueError):
    """A provider response could not become a validated table grid."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code


@dataclass(frozen=True, slots=True)
class PdfTableTranscriptionConfig:
    enabled: bool = False
    provider: str = ""
    model: str = ""
    prompt_version: str = "v1"
    operator_version: str = "v1"
    dpi: int = 200
    max_pages: int = 200
    max_regions_per_page: int = 20
    max_tables_per_run: int = 100
    max_crop_pixels: int = 20_000_000
    max_crop_bytes: int = 20 << 20
    timeout_seconds: float = 120.0
    max_output_tokens: int = 8_192
    retry_count: int = 0
    temperature: float = 0.0
    tracking_mode: str = "optional"
    # Optional spend admission (both-or-neither; see _SPEND_CONFIG_KEYS).
    # `None`/`None` means the lane is economically UNBOUNDED, and the service
    # reports it as such rather than defaulting to a number nobody chose.
    spend_cap_usd: float | None = None
    spend_estimate_usd: float | None = None

    @classmethod
    def from_mapping(
        cls, value: Mapping[str, Any] | None
    ) -> PdfTableTranscriptionConfig:
        if value is None:
            return cls()
        if not isinstance(value, Mapping) or not all(
            isinstance(key, str) for key in value
        ):
            raise PdfTableConfigError("table_transcription must be an object")
        sensitive = sorted(key for key in value if _SENSITIVE_KEY_RE.search(key))
        if sensitive:
            raise PdfTableConfigError(
                f"table_transcription must not contain secret fields: {sensitive!r}"
            )
        extra = sorted(set(value) - _CONFIG_KEYS)
        if extra:
            raise PdfTableConfigError(
                f"unknown table_transcription configuration fields: {extra!r}"
            )
        if value.get("enabled") is True:
            # Spend keys are exempt from the every-bounded-field rule; see the
            # comment on _SPEND_CONFIG_KEYS.
            missing = sorted(_CONFIG_KEYS - _SPEND_CONFIG_KEYS - set(value))
            if missing:
                raise PdfTableConfigError(
                    "enabled table_transcription must name every bounded field; "
                    f"missing {missing!r}"
                )
        try:
            return cls(**dict(value))
        except TypeError as exc:
            raise PdfTableConfigError(str(exc)) from exc

    def __post_init__(self) -> None:
        if type(self.enabled) is not bool:
            raise PdfTableConfigError("enabled must be a boolean")
        # Spend declarations are validated even when the lane is disabled: half
        # a spend contract is malformed authoring — an operator who wrote one
        # key believes a cap exists, and it would not.
        if (self.spend_cap_usd is None) != (self.spend_estimate_usd is None):
            raise PdfTableConfigError(
                "spend_cap_usd and spend_estimate_usd must be declared "
                "together or not at all"
            )
        if self.spend_cap_usd is not None:
            for field_name in ("spend_cap_usd", "spend_estimate_usd"):
                value = getattr(self, field_name)
                if (
                    isinstance(value, bool)
                    or not isinstance(value, (int, float))
                    or not math.isfinite(float(value))
                    or float(value) <= 0
                ):
                    raise PdfTableConfigError(
                        f"{field_name} must be a positive finite number"
                    )
        if not self.enabled:
            return
        if self.provider not in {"injected", "litellm"}:
            raise PdfTableConfigError("provider must be 'injected' or 'litellm'")
        for field_name in ("model", "prompt_version", "operator_version"):
            value = getattr(self, field_name)
            if not isinstance(value, str) or not value.strip():
                raise PdfTableConfigError(f"{field_name} must be non-empty")
        self._bounded_int("dpi", self.dpi, MAX_DPI_HARD)
        self._bounded_int("max_pages", self.max_pages, MAX_PAGES_HARD)
        self._bounded_int(
            "max_regions_per_page",
            self.max_regions_per_page,
            MAX_REGIONS_PER_PAGE_HARD,
        )
        self._bounded_int(
            "max_tables_per_run", self.max_tables_per_run, MAX_TABLES_PER_RUN_HARD
        )
        self._bounded_int("max_crop_pixels", self.max_crop_pixels, MAX_CROP_PIXELS_HARD)
        self._bounded_int("max_crop_bytes", self.max_crop_bytes, MAX_CROP_BYTES_HARD)
        self._bounded_int(
            "max_output_tokens", self.max_output_tokens, MAX_OUTPUT_TOKENS_HARD
        )
        if isinstance(self.timeout_seconds, bool) or not isinstance(
            self.timeout_seconds, (int, float)
        ):
            raise PdfTableConfigError("timeout_seconds must be numeric")
        if not 0 < float(self.timeout_seconds) <= MAX_TIMEOUT_SECONDS_HARD:
            raise PdfTableConfigError(
                f"timeout_seconds must be in (0, {MAX_TIMEOUT_SECONDS_HARD}]"
            )
        if type(self.retry_count) is not int or self.retry_count != 0:
            raise PdfTableConfigError("V1 retry_count must be exactly zero")
        if isinstance(self.temperature, bool) or float(self.temperature) != 0.0:
            raise PdfTableConfigError("V1 temperature must be exactly zero")
        if self.tracking_mode not in {"disabled", "optional", "required"}:
            raise PdfTableConfigError(
                "tracking_mode must be disabled, optional, or required"
            )

    @staticmethod
    def _bounded_int(name: str, value: Any, ceiling: int) -> None:
        if type(value) is not int or not 0 < value <= ceiling:
            raise PdfTableConfigError(f"{name} must be in [1, {ceiling}]")

    def public_mapping(self) -> dict[str, Any]:
        """Return the complete authored config; it contains no secret surface."""

        return asdict(self)


@dataclass(frozen=True, slots=True)
class TableRegionCandidate:
    page_index: int
    bbox: tuple[Any, Any, Any, Any]
    page_width: Any
    page_height: Any
    detector_name: str
    detector_version: str


@dataclass(frozen=True, slots=True)
class TableRegion:
    page_index: int
    bbox: tuple[Decimal, Decimal, Decimal, Decimal]
    page_width: Decimal
    page_height: Decimal
    detector_name: str
    detector_version: str
    source_locator: str


@dataclass(frozen=True, slots=True)
class TableRegionPlan:
    regions: tuple[TableRegion, ...]
    warnings: tuple[str, ...]


@runtime_checkable
class TableRegionLocator(Protocol):
    name: str
    version: str

    def locate(
        self, pdf_bytes: bytes, *, max_pages: int
    ) -> Sequence[TableRegionCandidate]: ...


@runtime_checkable
class TableCropRenderer(Protocol):
    name: str
    version: str

    def render_crop(
        self, pdf_bytes: bytes, region: TableRegion, *, dpi: int
    ) -> bytes: ...


@runtime_checkable
class TableTranscriptionProvider(Protocol):
    model: str
    external_network: bool

    def transcribe(
        self,
        crop_bytes: bytes,
        prompt: str,
        *,
        fragment_id: str,
        outcome_key_digest: str,
    ) -> str: ...


@runtime_checkable
class TableResponseCache(Protocol):
    def get(self, signature: str) -> str | None: ...

    def put(self, signature: str, response: str) -> None: ...


@dataclass(slots=True)
class MemoryTableResponseCache:
    values: dict[str, str] = field(default_factory=dict)

    def get(self, signature: str) -> str | None:
        return self.values.get(signature)

    def put(self, signature: str, response: str) -> None:
        self.values[signature] = response


def _decimal(value: Any, *, field_name: str) -> Decimal:
    if isinstance(value, bool) or not isinstance(value, (Decimal, int, float, str)):
        raise ValueError(f"{field_name} must be numeric")
    if isinstance(value, float) and not math.isfinite(value):
        raise ValueError(f"{field_name} must be finite")
    try:
        result = Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise ValueError(f"{field_name} must be numeric") from exc
    if not result.is_finite():
        raise ValueError(f"{field_name} must be finite")
    return result


def _canonical_region(candidate: TableRegionCandidate) -> TableRegion:
    if type(candidate.page_index) is not int or candidate.page_index < 0:
        raise ValueError("page_index must be a nonnegative integer")
    if not isinstance(candidate.detector_name, str) or not candidate.detector_name:
        raise ValueError("detector_name must be non-empty")
    if (
        not isinstance(candidate.detector_version, str)
        or not candidate.detector_version
    ):
        raise ValueError("detector_version must be non-empty")
    if not isinstance(candidate.bbox, (tuple, list)) or len(candidate.bbox) != 4:
        raise ValueError("bbox must contain four coordinates")
    width = _decimal(candidate.page_width, field_name="page_width")
    height = _decimal(candidate.page_height, field_name="page_height")
    if width <= 0 or height <= 0:
        raise ValueError("page dimensions must be positive")
    x0, y0, x1, y1 = (
        _decimal(value, field_name=f"bbox[{index}]")
        for index, value in enumerate(candidate.bbox)
    )
    clipped = canonical_pdf_bbox(
        (
            min(max(x0, Decimal(0)), width),
            min(max(y0, Decimal(0)), height),
            min(max(x1, Decimal(0)), width),
            min(max(y1, Decimal(0)), height),
        )
    )
    locator = pdf_table_locator(candidate.page_index, clipped)
    return TableRegion(
        page_index=candidate.page_index,
        bbox=clipped,
        page_width=width,
        page_height=height,
        detector_name=candidate.detector_name,
        detector_version=candidate.detector_version,
        source_locator=locator,
    )


def prepare_table_regions(
    candidates: Sequence[TableRegionCandidate],
    *,
    config: PdfTableTranscriptionConfig,
    remaining_tables: int,
) -> TableRegionPlan:
    canonical: list[TableRegion] = []
    warnings: list[str] = []
    for candidate in candidates:
        try:
            region = _canonical_region(candidate)
        except (TableGridError, TypeError, ValueError):
            warnings.append("table_region_invalid")
            continue
        if region.page_index >= config.max_pages:
            warnings.append("table_region_budget_exhausted")
            continue
        canonical.append(region)
    canonical.sort(
        key=lambda item: (
            item.page_index,
            item.bbox[1],
            item.bbox[0],
            item.bbox[3],
            item.bbox[2],
            item.detector_name,
            item.detector_version,
        )
    )
    deduplicated: list[TableRegion] = []
    seen: set[tuple[int, Decimal, Decimal, Decimal, Decimal]] = set()
    for region in canonical:
        key = (region.page_index, *region.bbox)
        if key in seen:
            continue
        seen.add(key)
        deduplicated.append(region)

    accepted: list[TableRegion] = []
    per_page: dict[int, int] = {}
    for region in deduplicated:
        page_count = per_page.get(region.page_index, 0)
        if (
            page_count >= config.max_regions_per_page
            or len(accepted) >= remaining_tables
        ):
            warnings.append("table_region_budget_exhausted")
            continue
        per_page[region.page_index] = page_count + 1
        accepted.append(region)
    if not candidates:
        warnings.append("table_region_not_found")
    return TableRegionPlan(
        regions=tuple(accepted), warnings=tuple(dict.fromkeys(warnings))
    )


def parse_table_provider_response(response: str | bytes) -> TableGridV1:
    if isinstance(response, bytes):
        try:
            response = response.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise PdfTableResponseError(
                "invalid_provider_response", "provider response must be UTF-8"
            ) from exc
    if not isinstance(response, str) or not response.strip():
        raise PdfTableResponseError(
            "invalid_provider_response", "provider response must be non-empty JSON"
        )
    clean = response.strip()
    fenced = _FENCE_RE.fullmatch(clean)
    if fenced is not None:
        clean = fenced.group(1).strip()
    try:
        parsed = json.loads(clean)
    except ValueError as exc:
        raise PdfTableResponseError(
            "invalid_provider_response", "provider response is not valid JSON"
        ) from exc
    grid_value: Any = parsed
    if isinstance(parsed, Mapping) and "table_grid" in parsed:
        grid_value = parsed["table_grid"]
    elif (
        isinstance(parsed, Mapping)
        and isinstance(parsed.get("result"), Mapping)
        and "table_grid" in parsed["result"]
    ):
        grid_value = parsed["result"]["table_grid"]
    try:
        return TableGridV1.from_mapping(grid_value)
    except TableGridError as exc:
        raise PdfTableResponseError("invalid_table_grid", str(exc)) from exc


@dataclass(frozen=True, slots=True)
class TableCacheSignatureComponents:
    document_content_sha256: str
    crop_sha256: str
    source_locator: str
    detector_name: str
    detector_version: str
    renderer_name: str
    renderer_version: str
    dpi: int
    provider: str
    model: str
    prompt_digest: str
    prompt_version: str
    operator_version: str
    max_output_tokens: int
    timeout_seconds: float
    retry_count: int
    temperature: float
    response_parser_version: str = TABLE_RESPONSE_PARSER_VERSION
    table_grid_schema_version: int = TABLE_GRID_SCHEMA_VERSION
    normalization_version: str = TABLE_NORMALIZATION_VERSION

    def __post_init__(self) -> None:
        for name in ("document_content_sha256", "crop_sha256", "prompt_digest"):
            value = getattr(self, name)
            if not isinstance(value, str) or _SHA256_RE.fullmatch(value) is None:
                raise ValueError(f"{name} must be a lowercase SHA-256 digest")

    def signature(self) -> str:
        encoded = json.dumps(
            asdict(self), sort_keys=True, ensure_ascii=False, separators=(",", ":")
        ).encode("utf-8")
        return "sha256:" + hashlib.sha256(encoded).hexdigest()


@dataclass(frozen=True, slots=True)
class PdfTableOccurrence:
    result: TableExtractionResult
    page_index: int
    bbox: tuple[Decimal, Decimal, Decimal, Decimal]
    detector_name: str
    detector_version: str
    renderer_name: str
    renderer_version: str
    dpi: int
    cache_hit: bool

    def to_chunk_extras(self) -> dict[str, Any]:
        attrs = self.result.to_chunk_extras()
        attrs.update(
            {
                "source_page_index": self.page_index,
                "source_bbox": [format(value, ".3f") for value in self.bbox],
                "table_detector": self.detector_name,
                "table_detector_version": self.detector_version,
                "table_renderer": self.renderer_name,
                "table_renderer_version": self.renderer_version,
                "table_render_dpi": self.dpi,
                "table_cache_hit": self.cache_hit,
            }
        )
        return attrs


@dataclass(frozen=True, slots=True)
class PdfTableDocumentResult:
    occurrences: tuple[PdfTableOccurrence, ...]
    warnings: tuple[str, ...]
    provider_invocations: int
    external_network_attempts: int
    cache_hits: int
    cache_misses: int


class _LiteLLMTableProvider:
    external_network = True

    def __init__(self, client: Any, *, model: str) -> None:
        self._client = client
        self.model = model

    def transcribe(
        self,
        crop_bytes: bytes,
        prompt: str,
        *,
        fragment_id: str,
        outcome_key_digest: str,
    ) -> str:
        return self._client.complete_image(
            crop_bytes,
            prompt,
            caller="doc_corpus:table_transcription",
            fragment_id=fragment_id,
            outcome_key_digest=outcome_key_digest,
        )


def build_table_transcription_provider(
    config: PdfTableTranscriptionConfig,
    *,
    injected_provider: TableTranscriptionProvider | None = None,
    tracking_dsn: str | None = None,
    deployment: str | None = None,
) -> TableTranscriptionProvider | None:
    if not config.enabled:
        return None
    try:
        resolved_tracking_dsn = resolve_tracking_dsn(
            tracking_mode=config.tracking_mode,
            explicit_tracking_dsn=None,
            runtime_dsn=tracking_dsn,
            component="PDF table transcription",
        )
    except TrackingConfigurationError as exc:
        raise PdfTableConfigError(
            str(exc), underlying_code=exc.code, data=exc.data
        ) from exc
    if config.provider == "injected":
        if injected_provider is None:
            raise PdfTableConfigError("provider=injected requires an injected provider")
        if not isinstance(injected_provider, TableTranscriptionProvider):
            raise PdfTableConfigError("injected provider does not satisfy the protocol")
        return injected_provider
    if config.provider == "litellm":
        try:
            from okg.views.execution.llm import VLMClient

            client = VLMClient(
                model=config.model,
                max_retries=config.retry_count + 1,
                temperature=config.temperature,
                timeout_s=config.timeout_seconds,
                max_output_tokens=config.max_output_tokens,
                tracking_dsn=resolved_tracking_dsn,
                tracking_mode=(
                    "optional" if resolved_tracking_dsn else "disabled"
                ),
                deployment=deployment,
            )
        except Exception as exc:
            raise PdfTableConfigError(
                f"could not construct litellm table provider: {exc}"
            ) from exc
        return _LiteLLMTableProvider(client, model=config.model)
    raise PdfTableConfigError("unsupported provider")


def _is_timeout_error(exc: BaseException) -> bool:
    current: BaseException | None = exc
    while current is not None:
        if (
            isinstance(current, TimeoutError)
            or "timeout" in type(current).__name__.lower()
        ):
            return True
        current = current.__cause__
    return False


class PdfTableTranscriptionService:
    """Apply one bounded, cache-aware transcription policy across documents."""

    def __init__(
        self,
        *,
        config: PdfTableTranscriptionConfig,
        locator: TableRegionLocator | None,
        renderer: TableCropRenderer | None,
        provider: TableTranscriptionProvider | None,
        cache: TableResponseCache | None = None,
        prompt: str = DEFAULT_TABLE_PROMPT,
        deployment: str | None = None,
        settlement_dsn: str | None = None,
    ) -> None:
        self.config = config
        self.locator = locator
        self.renderer = renderer
        self.provider = provider
        self.cache = cache or MemoryTableResponseCache()
        self.prompt = prompt
        self._tables_processed = 0
        self._provider_invocations = 0
        self._external_network_attempts = 0
        self._cache_hits = 0
        self._cache_misses = 0
        # The four counters above `_tables_processed` are incremented from
        # fan-out worker threads; unlocked read-modify-writes there can lose
        # updates and under-report (including external-network attempts,
        # which is an audit number, not a nicety).
        import threading

        self._telemetry_lock = threading.Lock()
        # Spend admission for the transcription lane, over the same ledger the
        # rest of the substrate charges (`okg.deployment_cost_ledger`). A cap
        # without a deployment identity would settle ledger rows against an
        # invented name, so it refuses up front instead of mis-attributing.
        if config.enabled and config.spend_cap_usd is not None and not deployment:
            raise PdfTableConfigError(
                "spend_cap_usd requires a deployment identity for the cost "
                "ledger; construct the service with deployment=<name>"
            )
        self._spend = ModelCallSpend(
            deployment=deployment or "",
            scope=TABLE_SPEND_SCOPE,
            cap_usd=config.spend_cap_usd,
            estimated_usd_per_call=config.spend_estimate_usd,
        )
        self._settlement_dsn = settlement_dsn
        if config.enabled:
            if locator is None or renderer is None or provider is None:
                raise PdfTableConfigError(
                    "enabled table transcription requires locator, renderer, and provider"
                )
            if not isinstance(locator, TableRegionLocator):
                raise PdfTableConfigError("locator does not satisfy TableRegionLocator")
            if not isinstance(renderer, TableCropRenderer):
                raise PdfTableConfigError("renderer does not satisfy TableCropRenderer")
            if not isinstance(provider, TableTranscriptionProvider):
                raise PdfTableConfigError(
                    "provider does not satisfy TableTranscriptionProvider"
                )

    def extract_document(self, pdf_bytes: bytes) -> PdfTableDocumentResult:
        before = self._counter_snapshot()
        if not self.config.enabled:
            return self._document_result((), (), before)
        if not isinstance(pdf_bytes, bytes):
            raise TypeError("pdf_bytes must be bytes")
        assert self.locator is not None
        assert self.renderer is not None
        assert self.provider is not None
        document_hash = hashlib.sha256(pdf_bytes).hexdigest()
        try:
            candidates = self.locator.locate(pdf_bytes, max_pages=self.config.max_pages)
        except Exception:
            return self._document_result((), ("table_region_locator_failed",), before)
        plan = prepare_table_regions(
            candidates,
            config=self.config,
            remaining_tables=max(
                0, self.config.max_tables_per_run - self._tables_processed
            ),
        )
        # Regions are independent VLM round-trips over crops of the same PDF
        # bytes, so they overlap under the shared concurrency term. The
        # per-run table counter is incremented BEFORE dispatch, on this
        # thread: incrementing inside the workers would race and could let the
        # run exceed `max_tables_per_run`, which is a budget, not a hint.
        from concurrent.futures import ThreadPoolExecutor

        from okg.substrate.db.concurrency_budget import width

        regions = list(plan.regions)
        self._tables_processed += len(regions)
        # Spend admission reserves at DISPATCH time, inside the worker, AFTER
        # the cache check — `reserve` is in-memory and lock-guarded, built to
        # be called from fan-out worker threads. Reserving up-front for every
        # planned region charged the cap for cache hits: on a cache-heavy
        # re-run the first cap/estimate regions (cached, in stable sorted
        # order) consumed the whole cap and the same uncached tail regions
        # were refused every run, deterministically, forever. A refused
        # region is not dropped — it becomes a `budget_exhausted` /
        # `spend_cap_exhausted` occurrence, so the document result still
        # accounts for every planned region. Settlement touches a database
        # connection and therefore stays off the worker threads: one slot
        # per region, written by at most one worker; the pool join orders
        # every write before the settlement read below.
        admissions: list[SpendAdmission | None] = [None] * len(regions)
        provider_called = [False] * len(regions)

        def _run(index: int, region: TableRegion) -> PdfTableOccurrence:
            return self._extract_region(
                pdf_bytes,
                document_hash,
                region,
                on_admission=lambda a: admissions.__setitem__(index, a),
                on_provider_call=lambda: provider_called.__setitem__(index, True),
            )

        fanout = min(width("enrichment.model_calls"), max(1, len(regions)))
        if fanout <= 1 or len(regions) <= 1:
            occurrences = [
                _run(index, region) for index, region in enumerate(regions)
            ]
        else:
            with ThreadPoolExecutor(
                max_workers=fanout, thread_name_prefix="okg-vlm-table",
            ) as pool:
                # `map` preserves input order, so occurrences stay aligned
                # with regions — a reordered list would attach one table's
                # cells to another table's bounding box.
                occurrences = list(pool.map(_run, range(len(regions)), regions))
        refused_n = sum(
            1 for item in occurrences
            if item.result.error_code == "spend_cap_exhausted"
        )
        if refused_n:
            log.warning(
                "vlm table spend cap refused %d of %d region(s) for scope %s",
                refused_n, len(regions), self._spend.scope,
            )
        self._settle_admissions(admissions, provider_called, occurrences)
        return self._document_result(tuple(occurrences), plan.warnings, before)

    def _settle_admissions(
        self,
        admissions: Sequence[SpendAdmission | None],
        provider_called: Sequence[bool],
        occurrences: Sequence[PdfTableOccurrence],
    ) -> None:
        """Charge dispatched provider calls to the shared cost ledger.

        Only regions that RESERVED settle (reservation happens at dispatch
        time, after the cache check, so cache hits neither reserve nor
        settle). A dispatched call that raised settles at ZERO — no usage is
        knowable here, and charging the estimate would over-report failures
        which are retried and charged again. Successful calls settle the
        reservation (this provider surface reports no per-call cost).

        A settlement failure never loses extraction output — the calls were
        already made and paid for — but it is loud, because a lost settlement
        under-reports spend and the cap drifts (see `ModelCallSpend.settle`).
        """
        if not self._spend.enabled:
            return
        to_settle: list[tuple[SpendAdmission, Optional[float]]] = []
        for admission, called, occurrence in zip(
            admissions, provider_called, occurrences,
        ):
            if admission is None or not admission.admitted:
                continue
            if not called:
                # Reserved but never dispatched (nothing between reserve and
                # dispatch should fail, so this is defensive): return the
                # headroom, record nothing.
                self._spend.release(admission)
                continue
            to_settle.append((
                admission,
                0.0 if occurrence.result.status == "provider_failed" else None,
            ))
        if not to_settle:
            return
        if self._settlement_dsn is None:
            log.error(
                "vlm table spend settlement SKIPPED for %d call(s): no "
                "settlement DSN was provided, so okg.deployment_cost_ledger "
                "will not record this run's spend and reported spend "
                "UNDER-states actual. The in-process cap still holds.",
                len(to_settle),
            )
            return
        try:
            # Through the sanctioned wrapper so the connection carries an
            # application_name — a raw connect here re-opened the unnamed-
            # backend hole the attribution inventory just closed.
            from okg.substrate.db.connection import connect as db_connect

            with db_connect(
                self._settlement_dsn, autocommit=True,
                application_name="okg-vlm-table-settlement",
            ) as conn:
                for admission, actual in to_settle:
                    self._spend.settle(conn, admission, actual)
        except Exception as exc:  # noqa: BLE001 — see docstring
            log.error(
                "vlm table spend settlement FAILED for %d call(s) (scope %s): "
                "the calls were made and paid for, but the ledger does not "
                "record them, so reported spend UNDER-states actual. "
                "Underlying: %s",
                len(to_settle), self._spend.scope, exc,
            )

    def _extract_region(
        self,
        pdf_bytes: bytes,
        document_hash: str,
        region: TableRegion,
        *,
        on_admission: Callable[[SpendAdmission], None] | None = None,
        on_provider_call: Callable[[], None] | None = None,
    ) -> PdfTableOccurrence:
        assert self.renderer is not None
        assert self.provider is not None
        scale = Decimal(self.config.dpi) / Decimal(72)
        width_px = math.ceil(float((region.bbox[2] - region.bbox[0]) * scale))
        height_px = math.ceil(float((region.bbox[3] - region.bbox[1]) * scale))
        if width_px * height_px > self.config.max_crop_pixels:
            return self._failed_occurrence(
                region,
                source_hash=document_hash,
                status="budget_exhausted",
                error_code="crop_budget_exceeded",
            )
        try:
            crop = self.renderer.render_crop(pdf_bytes, region, dpi=self.config.dpi)
        except Exception:
            return self._failed_occurrence(
                region,
                source_hash=document_hash,
                status="parse_failed",
                error_code="render_failed",
            )
        if not isinstance(crop, bytes):
            return self._failed_occurrence(
                region,
                source_hash=document_hash,
                status="parse_failed",
                error_code="render_failed",
            )
        crop_hash = hashlib.sha256(crop).hexdigest()
        if len(crop) > self.config.max_crop_bytes:
            return self._failed_occurrence(
                region,
                source_hash=crop_hash,
                status="budget_exhausted",
                error_code="crop_budget_exceeded",
            )
        signature_components = TableCacheSignatureComponents(
            document_content_sha256=document_hash,
            crop_sha256=crop_hash,
            source_locator=region.source_locator,
            detector_name=region.detector_name,
            detector_version=region.detector_version,
            renderer_name=self.renderer.name,
            renderer_version=self.renderer.version,
            dpi=self.config.dpi,
            provider=self.config.provider,
            model=self.config.model,
            prompt_digest=prompt_content_hash(self.prompt),
            prompt_version=self.config.prompt_version,
            operator_version=self.config.operator_version,
            max_output_tokens=self.config.max_output_tokens,
            timeout_seconds=float(self.config.timeout_seconds),
            retry_count=self.config.retry_count,
            temperature=float(self.config.temperature),
        )
        signature = signature_components.signature()
        cached = self.cache.get(signature)
        if cached is not None:
            with self._telemetry_lock:
                self._cache_hits += 1
            try:
                grid = parse_table_provider_response(cached)
            except PdfTableResponseError as exc:
                return self._failed_occurrence(
                    region,
                    source_hash=crop_hash,
                    status="parse_failed",
                    error_code=exc.code,
                    cache_signature=signature,
                    cache_hit=True,
                )
            return self._parsed_occurrence(
                region,
                source_hash=crop_hash,
                grid=grid,
                cache_signature=signature,
                cache_hit=True,
            )

        with self._telemetry_lock:
            self._cache_misses += 1
        # Reserve at DISPATCH time, after the cache check: a cache hit costs
        # nothing and must not consume cap headroom (up-front reservation
        # starved the uncached tail of a cache-heavy corpus deterministically,
        # every run). `reserve` is lock-guarded and safe on worker threads.
        admission = self._spend.reserve(None)
        if not admission.admitted:
            return self._failed_occurrence(
                region,
                source_hash=crop_hash,
                status="budget_exhausted",
                error_code="spend_cap_exhausted",
                cache_signature=signature,
            )
        if on_admission is not None:
            on_admission(admission)
        outcome_key = OutcomeKey(
            step_id="doc_corpus:table_transcription",
            operator_version=self.config.operator_version,
            model_id=self.config.model,
            prompt_hash=prompt_content_hash(self.prompt),
            input_hash=hashlib.sha256(signature.encode("utf-8")).hexdigest(),
        )
        with self._telemetry_lock:
            self._provider_invocations += 1
            if self.provider.external_network:
                self._external_network_attempts += 1
        # Marked BEFORE the call so a raising provider still settles: the
        # attempt was dispatched and possibly paid for.
        if on_provider_call is not None:
            on_provider_call()
        try:
            response = self.provider.transcribe(
                crop,
                self.prompt,
                fragment_id=region.source_locator,
                outcome_key_digest=outcome_key.digest,
            )
        except Exception as exc:
            return self._failed_occurrence(
                region,
                source_hash=crop_hash,
                status="provider_failed",
                error_code=(
                    "provider_timeout"
                    if _is_timeout_error(exc)
                    else "provider_exception"
                ),
                cache_signature=signature,
            )
        try:
            grid = parse_table_provider_response(response)
        except PdfTableResponseError as exc:
            return self._failed_occurrence(
                region,
                source_hash=crop_hash,
                status="parse_failed",
                error_code=exc.code,
                cache_signature=signature,
            )
        self.cache.put(signature, response)
        return self._parsed_occurrence(
            region,
            source_hash=crop_hash,
            grid=grid,
            cache_signature=signature,
            cache_hit=False,
        )

    def _parsed_occurrence(
        self,
        region: TableRegion,
        *,
        source_hash: str,
        grid: TableGridV1,
        cache_signature: str,
        cache_hit: bool,
    ) -> PdfTableOccurrence:
        return self._occurrence(
            region,
            result=TableExtractionResult(
                lane="pdf_vlm",
                status="parsed",
                source_locator=region.source_locator,
                source_content_sha256=source_hash,
                grid=grid,
                cache_signature=cache_signature,
            ),
            cache_hit=cache_hit,
        )

    def _failed_occurrence(
        self,
        region: TableRegion,
        *,
        source_hash: str,
        status: str,
        error_code: str,
        cache_signature: str | None = None,
        cache_hit: bool = False,
    ) -> PdfTableOccurrence:
        return self._occurrence(
            region,
            result=TableExtractionResult(
                lane="pdf_vlm",
                status=status,
                error_code=error_code,
                source_locator=region.source_locator,
                source_content_sha256=source_hash,
                cache_signature=cache_signature,
            ),
            cache_hit=cache_hit,
        )

    def _occurrence(
        self,
        region: TableRegion,
        *,
        result: TableExtractionResult,
        cache_hit: bool,
    ) -> PdfTableOccurrence:
        assert self.renderer is not None
        return PdfTableOccurrence(
            result=result,
            page_index=region.page_index,
            bbox=region.bbox,
            detector_name=region.detector_name,
            detector_version=region.detector_version,
            renderer_name=self.renderer.name,
            renderer_version=self.renderer.version,
            dpi=self.config.dpi,
            cache_hit=cache_hit,
        )

    def _counter_snapshot(self) -> tuple[int, int, int, int]:
        return (
            self._provider_invocations,
            self._external_network_attempts,
            self._cache_hits,
            self._cache_misses,
        )

    def _document_result(
        self,
        occurrences: tuple[PdfTableOccurrence, ...],
        warnings: tuple[str, ...],
        before: tuple[int, int, int, int],
    ) -> PdfTableDocumentResult:
        after = self._counter_snapshot()
        return PdfTableDocumentResult(
            occurrences=occurrences,
            warnings=warnings,
            provider_invocations=after[0] - before[0],
            external_network_attempts=after[1] - before[1],
            cache_hits=after[2] - before[2],
            cache_misses=after[3] - before[3],
        )


__all__ = [
    "DEFAULT_TABLE_PROMPT",
    "MemoryTableResponseCache",
    "PdfTableConfigError",
    "PdfTableDocumentResult",
    "PdfTableOccurrence",
    "PdfTableResponseError",
    "PdfTableTranscriptionConfig",
    "PdfTableTranscriptionService",
    "TABLE_SPEND_SCOPE",
    "TableCacheSignatureComponents",
    "TableCropRenderer",
    "TableRegion",
    "TableRegionCandidate",
    "TableRegionLocator",
    "TableRegionPlan",
    "TableResponseCache",
    "TableTranscriptionProvider",
    "build_table_transcription_provider",
    "parse_table_provider_response",
    "prepare_table_regions",
]
