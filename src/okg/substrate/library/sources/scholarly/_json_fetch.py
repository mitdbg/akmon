"""Private JSON request and retry policy shared by scholarly adapters."""

from __future__ import annotations

import json
import time
import urllib.parse
import urllib.request
from typing import Any
from urllib.error import HTTPError, URLError

from okg.substrate.library.sources._url_safety import (
    assert_public_http_url,
    open_validated_url,
)
from okg.substrate.library.sources.scholarly._common import okg_user_agent


def fetch_json(
    endpoint: str,
    query: dict[str, Any],
    *,
    max_retries: int = 3,
    retry_backoff_seconds: float = 2.0,
    unreachable_error: str = "unreachable scholarly JSON retry state",
) -> dict[str, Any]:
    """Fetch one JSON object with the OpenReview/INSPIRE retry contract."""
    url = f"{endpoint}?{urllib.parse.urlencode(query)}"
    request = urllib.request.Request(url, headers={"User-Agent": okg_user_agent()})
    assert_public_http_url(request.full_url)

    for attempt in range(max_retries + 1):
        try:
            with open_validated_url(request, timeout=60) as response:
                return json.loads(response.read().decode("utf-8"))
        except HTTPError as error:
            if not retryable_http_status(error.code) or attempt >= max_retries:
                raise
            retry_after = retry_after_seconds(error)
            sleep_for = (
                retry_after
                if retry_after is not None
                else retry_backoff_seconds * (2**attempt)
            )
        except URLError:
            if attempt >= max_retries:
                raise
            sleep_for = retry_backoff_seconds * (2**attempt)
        time.sleep(sleep_for)

    raise RuntimeError(unreachable_error)


def retryable_http_status(status: int) -> bool:
    return status == 429 or status >= 500


def retry_after_seconds(error: HTTPError) -> float | None:
    raw = error.headers.get("Retry-After") if error.headers else None
    if not raw:
        return None
    try:
        return max(0.0, float(raw))
    except ValueError:
        return None
