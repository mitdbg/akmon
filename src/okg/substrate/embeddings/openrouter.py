"""OpenRouter embedding backend (PACT openrouter-embeddings-hnsw).

``openrouter:<model>@<dims>`` calls OpenRouter's OpenAI-compatible
``POST /api/v1/embeddings`` with ``dimensions=<dims>`` and zero-pads each
returned vector to the substrate width (:data:`EMBEDDING_DIMENSION`). Padding
with zeros changes neither a dot product nor a norm, so the cosine distance
between two padded vectors equals the distance between the model's own
``<dims>``-wide vectors. The column contract, the startup width probe and the
readiness checks stay as they are. A vector set's backend
(``pad_to_column=False``, PACT vector-set-pgvector-widths) returns the
``<dims>``-wide vectors unpadded, at any width the set's pgvector limits allow.

What this module guarantees, and what it refuses:

* **Batched.** :meth:`OpenRouterEmbedder.embed_batch` sends up to
  ``batch_size`` texts and ``max_batch_chars`` characters per request and keeps
  ``concurrency`` requests in flight. Output order is input order.
* **Validated.** Every response must return one vector per input, with each
  ``index`` once, each vector ``<dims>`` finite numbers, from the requested
  model. Anything else raises ``embedder_output_invalid``; nothing is skipped.
* **Retried, then loud.** 408/409/425/429/5xx, timeouts and connection errors
  are retried with exponential backoff and jitter, honoring ``Retry-After``.
  Any other error status fails at once with the provider's message. The API
  key is sent only in the ``Authorization`` header and never appears in an
  error, a log line or a call record.
* **Truncated, explicitly.** A text longer than :data:`MAX_INPUT_CHARS` is cut
  to its first :data:`MAX_INPUT_CHARS` characters before it is sent. The
  constant is part of what the identity means: changing it must change the
  identity. An empty text is refused rather than sent.
* **Costed.** Each request reports ``usage`` (tokens and ``cost`` in USD, as
  OpenRouter returns them) to an optional call observer, which the backfill
  uses for its spend cap and for ``okg.llm_calls`` rows.

The key comes from the ``OPENROUTER_API_KEY`` environment variable, never from
deployment configuration.
"""
from __future__ import annotations

import math
import os
import random
import threading
import time
from collections.abc import Callable, Sequence
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from typing import Any, Optional

import httpx

from okg.substrate.embeddings.identity import (
    EMBEDDING_DIMENSION,
    OPENROUTER_PREFIX,
)
from okg.substrate.errors import SubstrateError
from okg.substrate.library.sources._url_safety import (
    UnsafeFetchURL,
    assert_token_request_url,
)


__all__ = [
    "API_KEY_ENV",
    "DEFAULT_BASE_URL",
    "MAX_INPUT_CHARS",
    "EmbeddingCall",
    "OpenRouterEmbedder",
]


#: Environment variable holding the OpenRouter API key.
API_KEY_ENV = "OPENROUTER_API_KEY"
DEFAULT_BASE_URL = "https://openrouter.ai/api/v1"
#: The provider origin the API key may be sent to. Every request passes the
#: central egress guard (``_url_safety.assert_token_request_url``) at call
#: time: a public http(s) address AND this origin, unless the operator's
#: ``OKG_EGRESS_ALLOW_HOSTS`` exception names the host.
OPENROUTER_HOSTS = ("openrouter.ai",)

#: Characters kept from one text. A probe on 2026-10-09 sent 41,200
#: characters (6,801 tokens) to google/gemini-embedding-2 without error; this
#: bound keeps inputs well inside that. 821 of 629,867 comp-ops texts exceed
#: 8,000 characters, and 139 exceed 30,000.
#: The identity ``openrouter:<model>@<dims>`` stands for vectors of texts cut
#: at this bound; vectors cut elsewhere are not comparable. Changing it must
#: change the identity grammar, and tests/substrate/test_openrouter_embedder.py
#: pins the value so the change cannot be made silently.
MAX_INPUT_CHARS = 24_000

DEFAULT_BATCH_SIZE = 64
DEFAULT_MAX_BATCH_CHARS = 120_000
DEFAULT_CONCURRENCY = 4
DEFAULT_MAX_ATTEMPTS = 6
DEFAULT_TIMEOUT_SECONDS = 120.0
#: The search path's client (a query's own embedding): a 5 s timeout and two
#: attempts, then a refusal, so a hung or rate-limited request cannot stall a
#: search for minutes. The backfill keeps the defaults above.
QUERY_TIMEOUT_SECONDS = 5.0
QUERY_MAX_ATTEMPTS = 2
_BACKOFF_BASE_SECONDS = 1.0
_BACKOFF_CAP_SECONDS = 60.0
_RETRY_STATUSES = frozenset({408, 409, 425, 429, 500, 502, 503, 504})
_MESSAGE_CHARS = 400


@dataclass(frozen=True)
class EmbeddingCall:
    """One HTTP request's outcome, as the call observer receives it."""

    model: str
    provider: Optional[str]
    inputs: int
    input_chars: int
    prompt_tokens: Optional[int]
    total_tokens: Optional[int]
    cost_usd: Optional[float]
    latency_ms: int
    attempts: int
    success: bool
    error_kind: Optional[str] = None
    #: OpenRouter answered (and may have charged), whether or not the
    #: response then passed validation.
    answered: bool = False


def _error(code: str, message: str, /, **data: Any) -> SubstrateError:
    err = SubstrateError(message, data=data)
    err.code = code
    return err


def _int_or_none(value: Any) -> Optional[int]:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    return int(value)


def _float_or_none(value: Any) -> Optional[float]:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    return float(value)


class OpenRouterEmbedder:
    """``openrouter:<model>@<native_dimension>``, padded to the column width
    unless ``pad_to_column`` is False (a vector set's backend)."""

    def __init__(
        self,
        *,
        model: str,
        native_dimension: int,
        api_key: Optional[str] = None,
        base_url: str = DEFAULT_BASE_URL,
        batch_size: int = DEFAULT_BATCH_SIZE,
        max_batch_chars: int = DEFAULT_MAX_BATCH_CHARS,
        concurrency: int = DEFAULT_CONCURRENCY,
        max_attempts: int = DEFAULT_MAX_ATTEMPTS,
        timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS,
        transport: Optional[httpx.BaseTransport] = None,
        sleep: Callable[[float], None] = time.sleep,
        call_observer: Optional[Callable[[EmbeddingCall], None]] = None,
        provider: Optional[str] = None,
        pad_to_column: bool = True,
    ) -> None:
        # pad_to_column=False is a vector set's backend (PACT
        # vector-set-pgvector-widths): vectors keep the model's width and the
        # set's configuration bounds it by pgvector's limits.
        if pad_to_column and not 1 <= int(native_dimension) <= EMBEDDING_DIMENSION:
            raise ValueError(
                f"native_dimension must be in 1..{EMBEDDING_DIMENSION}; "
                f"got {native_dimension}"
            )
        if int(native_dimension) < 1:
            raise ValueError(
                f"native_dimension must be at least 1; got {native_dimension}"
            )
        self._pad_to_column = bool(pad_to_column)
        for name, value in (
            ("batch_size", batch_size), ("max_batch_chars", max_batch_chars),
            ("concurrency", concurrency), ("max_attempts", max_attempts),
        ):
            if int(value) < 1:
                raise ValueError(f"{name} must be at least 1; got {value}")
        key = api_key if api_key is not None else os.environ.get(API_KEY_ENV)
        if not key or not key.strip():
            raise _error(
                "embedder_api_key_missing",
                f"the openrouter embedding backend needs an API key in "
                f"${API_KEY_ENV}, and it is unset or empty",
                env_var=API_KEY_ENV,
            )
        self._model = model
        self._provider = provider
        self._timeout_seconds = float(timeout_seconds)
        self._native = int(native_dimension)
        self._batch_size = int(batch_size)
        self._max_batch_chars = int(max_batch_chars)
        self._concurrency = int(concurrency)
        self._max_attempts = int(max_attempts)
        self._sleep = sleep
        self.call_observer = call_observer
        self._endpoint = base_url.rstrip("/") + "/embeddings"
        self._client = httpx.Client(
            base_url=base_url.rstrip("/"),
            # A 3xx is never followed: it fails the request (not retried), so
            # a redirect cannot carry the key past the egress guard.
            follow_redirects=False,
            headers={"Authorization": f"Bearer {key.strip()}"},
            timeout=timeout_seconds,
            transport=transport,
        )
        self._lock = threading.Lock()
        self._spent_usd = 0.0
        self._requests = 0
        self._prompt_tokens = 0

    def __repr__(self) -> str:
        # Never the client or its headers: the key lives there.
        return f"OpenRouterEmbedder(model_id={self.model_id!r})"

    # ── the Embedder contract ─────────────────────────────────────────

    @property
    def dimension(self) -> int:
        return EMBEDDING_DIMENSION if self._pad_to_column else self._native

    @property
    def native_dimension(self) -> int:
        return self._native

    @property
    def client_policy(self) -> dict[str, Any]:
        """The request timeout and attempt budget this client uses."""
        return {
            "timeout_seconds": self._timeout_seconds,
            "max_attempts": self._max_attempts,
            "provider": self._provider,
        }

    @property
    def model_id(self) -> str:
        return f"{OPENROUTER_PREFIX}{self._model}@{self._native}"

    def embed(self, text: str) -> list[float]:
        return self.embed_batch([text])[0]

    def embed_batch(self, texts: Sequence[str]) -> list[list[float]]:
        prepared = [self._prepare(text) for text in texts]
        if not prepared:
            return []
        chunks = self._chunks(prepared)
        if len(chunks) == 1 or self._concurrency == 1:
            results = [self._request(chunk) for chunk in chunks]
        else:
            with ThreadPoolExecutor(
                max_workers=min(self._concurrency, len(chunks)),
                thread_name_prefix="okg-openrouter-embed",
            ) as pool:
                results = list(pool.map(self._request, chunks))
        out: list[list[float]] = []
        for vectors in results:
            out.extend(vectors)
        return out

    # ── accounting ────────────────────────────────────────────────────

    @property
    def spent_usd(self) -> float:
        """USD OpenRouter reported for every answered request so far,
        including one whose response then failed validation."""
        with self._lock:
            return self._spent_usd

    @property
    def requests(self) -> int:
        with self._lock:
            return self._requests

    @property
    def prompt_tokens(self) -> int:
        with self._lock:
            return self._prompt_tokens

    def close(self) -> None:
        self._client.close()

    # ── internals ─────────────────────────────────────────────────────

    def _prepare(self, text: str) -> str:
        if not isinstance(text, str):
            raise ValueError("OpenRouterEmbedder: text must be a string")
        if not text.strip():
            raise ValueError(
                "OpenRouterEmbedder: refusing to embed an empty text"
            )
        return text[:MAX_INPUT_CHARS]

    def _chunks(self, texts: list[str]) -> list[list[str]]:
        chunks: list[list[str]] = []
        current: list[str] = []
        chars = 0
        for text in texts:
            if current and (
                len(current) >= self._batch_size
                or chars + len(text) > self._max_batch_chars
            ):
                chunks.append(current)
                current, chars = [], 0
            current.append(text)
            chars += len(text)
        if current:
            chunks.append(current)
        return chunks

    def _backoff(self, attempt: int, retry_after: Optional[str]) -> float:
        delay = min(
            _BACKOFF_CAP_SECONDS, _BACKOFF_BASE_SECONDS * (2 ** (attempt - 1)),
        )
        delay += random.uniform(0.0, delay / 2)  # noqa: S311 - jitter, not security
        if retry_after:
            try:
                delay = max(delay, min(_BACKOFF_CAP_SECONDS, float(retry_after)))
            except ValueError:
                pass  # an HTTP-date Retry-After: keep the computed backoff
        return delay

    def _observe(self, call: EmbeddingCall) -> None:
        observer = self.call_observer
        if observer is not None:
            observer(call)

    def _assert_egress_allowed(self) -> None:
        """Refuse the call unless the egress policy admits the endpoint."""
        try:
            assert_token_request_url(self._endpoint, OPENROUTER_HOSTS)
        except UnsafeFetchURL as exc:
            raise _error(
                "embedder_egress_refused",
                f"the egress policy refuses the embedding endpoint: {exc}",
                model_id=self.model_id,
                endpoint=self._endpoint,
            ) from exc

    def _request(self, texts: list[str]) -> list[list[float]]:
        # Checked on every request, before the key is sent anywhere.
        self._assert_egress_allowed()
        payload = {
            "model": self._model,
            "input": texts,
            "dimensions": self._native,
            "encoding_format": "float",
        }
        if self._provider is not None:
            # A pinned provider is part of the vector set's identity: no
            # fallback to another provider's model build.
            payload["provider"] = {"order": [self._provider], "allow_fallbacks": False}
        chars = sum(len(t) for t in texts)
        started = time.monotonic()
        last: dict[str, Any] = {}
        attempt = 0
        for attempt in range(1, self._max_attempts + 1):
            retry_after: Optional[str] = None
            try:
                response = self._client.post("/embeddings", json=payload)
            except (httpx.TimeoutException, httpx.TransportError) as exc:
                last = {
                    "error_kind": type(exc).__name__,
                    "message": str(exc)[:_MESSAGE_CHARS],
                }
            else:
                try:
                    body: Any = response.json()
                except ValueError:
                    body = None
                error = body.get("error") if isinstance(body, dict) else None
                if (
                    response.status_code == 200
                    and isinstance(body, dict)
                    and not error
                ):
                    # Settle (cost, tokens, observer) BEFORE validating: a
                    # response that fails validation was still charged, and
                    # the spend cap must count it.
                    try:
                        vectors = self._validate(body, len(texts))
                    except SubstrateError as exc:
                        self._settle(
                            body, texts, chars, started, attempt,
                            error_kind=str(getattr(exc, "code", "invalid")),
                        )
                        raise
                    self._settle(body, texts, chars, started, attempt)
                    return vectors
                status = response.status_code
                if status == 200 and isinstance(error, dict):
                    # OpenRouter reports some upstream failures as a 200
                    # whose body is an error object.
                    status = _int_or_none(error.get("code")) or 502
                message = (
                    str(error.get("message")) if isinstance(error, dict)
                    else response.text
                )[:_MESSAGE_CHARS]
                last = {
                    "error_kind": f"http_{status}",
                    "status": status,
                    "message": message,
                }
                retry_after = response.headers.get("retry-after")
                if status not in _RETRY_STATUSES:
                    break
            if attempt < self._max_attempts:
                self._sleep(self._backoff(attempt, retry_after))
        self._observe(EmbeddingCall(
            model=self.model_id, provider=None, inputs=len(texts),
            input_chars=chars, prompt_tokens=None, total_tokens=None,
            cost_usd=None, latency_ms=int((time.monotonic() - started) * 1000),
            attempts=attempt, success=False, error_kind=last.get("error_kind"),
        ))
        raise _error(
            "embedder_request_failed",
            f"openrouter embedding request for {self.model_id!r} failed after "
            f"{attempt} attempt(s): {last.get('error_kind')}: "
            f"{last.get('message')}",
            model_id=self.model_id,
            attempts=attempt,
            inputs=len(texts),
            **{
                k: v for k, v in last.items()
                if k in ("status", "error_kind", "message")
            },
        )

    def _validate(self, body: dict[str, Any], expected: int) -> list[list[float]]:
        returned = body.get("model")
        if isinstance(returned, str) and returned not in (
            self._model, self._model.rsplit("/", 1)[-1],
        ):
            raise _error(
                "embedder_output_invalid",
                f"openrouter answered with model {returned!r}, not the "
                f"requested {self._model!r}",
                model_id=self.model_id, returned_model=returned,
            )
        data = body.get("data")
        if not isinstance(data, list) or len(data) != expected:
            got = len(data) if isinstance(data, list) else "no"
            raise _error(
                "embedder_output_invalid",
                f"openrouter returned {got} vectors for {expected} inputs",
                model_id=self.model_id, expected=expected,
            )
        by_index: dict[int, list[float]] = {}
        for position, item in enumerate(data):
            if not isinstance(item, dict):
                raise _error(
                    "embedder_output_invalid",
                    "openrouter returned a non-object item",
                    model_id=self.model_id,
                )
            index = item.get("index", position)
            vector = item.get("embedding")
            if (
                isinstance(index, bool) or not isinstance(index, int)
                or not 0 <= index < expected or index in by_index
            ):
                raise _error(
                    "embedder_output_invalid",
                    f"openrouter returned an invalid or repeated index {index!r}",
                    model_id=self.model_id,
                )
            if not isinstance(vector, list) or len(vector) != self._native:
                width = len(vector) if isinstance(vector, list) else None
                raise _error(
                    "embedder_output_invalid",
                    f"openrouter returned a vector of width {width}; the spec "
                    f"asks for {self._native}",
                    model_id=self.model_id, observed_dimension=width,
                )
            values: list[float] = []
            for value in vector:
                if (
                    isinstance(value, bool)
                    or not isinstance(value, (int, float))
                    or not math.isfinite(float(value))
                ):
                    raise _error(
                        "embedder_output_invalid",
                        "openrouter returned a non-finite or non-numeric value",
                        model_id=self.model_id,
                    )
                values.append(float(value))
            norm = math.sqrt(sum(v * v for v in values))
            if norm <= 0.0:
                raise _error(
                    "embedder_output_invalid",
                    "openrouter returned an all-zero vector",
                    model_id=self.model_id,
                )
            # Unit norm is the embed() contract. The model already returns
            # unit vectors at this width (probe, 2026-10-09); dividing by the
            # norm only enforces the contract and leaves cosine unchanged.
            padded = [v / norm for v in values]
            if self._pad_to_column:
                padded.extend([0.0] * (EMBEDDING_DIMENSION - self._native))
            by_index[index] = padded
        return [by_index[i] for i in range(expected)]

    def _settle(
        self, body: dict[str, Any], texts: list[str], chars: int,
        started: float, attempts: int, *, error_kind: Optional[str] = None,
    ) -> None:
        raw_usage = body.get("usage")
        usage: dict[str, Any] = raw_usage if isinstance(raw_usage, dict) else {}
        prompt_tokens = _int_or_none(usage.get("prompt_tokens"))
        cost = _float_or_none(usage.get("cost"))
        with self._lock:
            self._requests += 1
            self._spent_usd += cost or 0.0
            self._prompt_tokens += prompt_tokens or 0
        provider = body.get("provider")
        self._observe(EmbeddingCall(
            model=self.model_id,
            provider=provider if isinstance(provider, str) else None,
            inputs=len(texts),
            input_chars=chars,
            prompt_tokens=prompt_tokens,
            total_tokens=_int_or_none(usage.get("total_tokens")),
            cost_usd=cost,
            latency_ms=int((time.monotonic() - started) * 1000),
            attempts=attempts,
            success=error_kind is None,
            error_kind=error_kind,
            answered=True,
        ))
