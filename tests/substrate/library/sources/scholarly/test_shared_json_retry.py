from __future__ import annotations

import importlib.metadata
import json
from email.message import Message
from typing import Any
from urllib.error import HTTPError, URLError
from unittest import mock

import pytest

import okg
from okg.substrate.library.sources._url_safety import UnsafeFetchURL
from okg.substrate.library.sources.scholarly import (
    _json_fetch,
    inspire_hep,
    openreview,
)


MODULES = (openreview, inspire_hep)


class _Response:
    def __init__(self, payload: bytes) -> None:
        self.payload = payload

    def __enter__(self) -> "_Response":
        return self

    def __exit__(self, *_args: object) -> bool:
        return False

    def read(self) -> bytes:
        return self.payload


@pytest.fixture(autouse=True)
def _allow_test_endpoint(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(_json_fetch, "assert_public_http_url", lambda _url: None)


def _http_error(status: int, retry_after: str | None = None) -> HTTPError:
    headers = Message()
    if retry_after is not None:
        headers["Retry-After"] = retry_after
    return HTTPError("https://example.test/api", status, "test", headers, None)


def _script_requests(
    monkeypatch: pytest.MonkeyPatch,
    actions: list[bytes | BaseException],
) -> list[tuple[Any, float]]:
    calls: list[tuple[Any, float]] = []

    def _open(request: Any, *, timeout: float):
        calls.append((request, timeout))
        action = actions.pop(0)
        if isinstance(action, BaseException):
            raise action
        return _Response(action)

    monkeypatch.setattr(_json_fetch, "open_validated_url", _open)
    return calls


@pytest.mark.parametrize("module", MODULES)
def test_entrypoint_preserves_request_shape_and_json_decoding(
    module: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = _script_requests(
        monkeypatch,
        ['{"title":"café"}'.encode("utf-8")],
    )

    assert module._fetch_json(
        "https://example.test/api",
        {"query": "a b", "limit": 2},
    ) == {"title": "café"}

    assert len(calls) == 1
    request, timeout = calls[0]
    assert request.full_url == "https://example.test/api?query=a+b&limit=2"
    assert request.get_header("User-agent") == f"okg/{okg.__version__}"
    assert timeout == 60


@pytest.mark.parametrize("module", MODULES)
def test_entrypoint_recovers_with_exponential_backoff(
    module: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = _script_requests(
        monkeypatch,
        [_http_error(503), URLError("temporary"), b'{"ok":true}'],
    )
    waits: list[float] = []
    monkeypatch.setattr(_json_fetch.time, "sleep", waits.append)

    assert module._fetch_json(
        "https://example.test/api",
        {},
        max_retries=2,
        retry_backoff_seconds=0.25,
    ) == {"ok": True}
    assert len(calls) == 3
    assert waits == [0.25, 0.5]


@pytest.mark.parametrize("module", MODULES)
@pytest.mark.parametrize(
    ("retry_after", "expected_wait"),
    [
        (None, 3.0),
        ("7.5", 7.5),
        ("malformed", 3.0),
        ("-4", 0.0),
    ],
)
def test_retry_after_policy(
    module: Any,
    retry_after: str | None,
    expected_wait: float,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = _script_requests(
        monkeypatch,
        [_http_error(429, retry_after), b'{"ok":true}'],
    )
    waits: list[float] = []
    monkeypatch.setattr(_json_fetch.time, "sleep", waits.append)

    assert module._fetch_json(
        "https://example.test/api",
        {},
        max_retries=1,
        retry_backoff_seconds=3.0,
    ) == {"ok": True}
    assert len(calls) == 2
    assert waits == [expected_wait]


@pytest.mark.parametrize("module", MODULES)
@pytest.mark.parametrize("error", [_http_error(500), URLError("offline")])
def test_retry_exhaustion_reraises_original_exception(
    module: Any,
    error: BaseException,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = _script_requests(monkeypatch, [error, error])
    waits: list[float] = []
    monkeypatch.setattr(_json_fetch.time, "sleep", waits.append)

    with pytest.raises(type(error)) as raised:
        module._fetch_json(
            "https://example.test/api",
            {},
            max_retries=1,
            retry_backoff_seconds=2.0,
        )

    assert raised.value is error
    assert len(calls) == 2
    assert waits == [2.0]


@pytest.mark.parametrize("module", MODULES)
def test_zero_retries_makes_one_attempt_and_does_not_sleep(
    module: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    error = URLError("offline")
    calls = _script_requests(monkeypatch, [error])
    sleep = mock.Mock()
    monkeypatch.setattr(_json_fetch.time, "sleep", sleep)

    with pytest.raises(URLError) as raised:
        module._fetch_json("https://example.test/api", {}, max_retries=0)

    assert raised.value is error
    assert len(calls) == 1
    sleep.assert_not_called()


@pytest.mark.parametrize("module", MODULES)
def test_nonretryable_http_error_is_terminal(
    module: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    error = _http_error(404)
    calls = _script_requests(monkeypatch, [error])
    sleep = mock.Mock()
    monkeypatch.setattr(_json_fetch.time, "sleep", sleep)

    with pytest.raises(HTTPError) as raised:
        module._fetch_json("https://example.test/api", {}, max_retries=3)

    assert raised.value is error
    assert len(calls) == 1
    sleep.assert_not_called()


@pytest.mark.parametrize("module", MODULES)
def test_malformed_json_is_terminal(
    module: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = _script_requests(monkeypatch, [b"{"])
    sleep = mock.Mock()
    monkeypatch.setattr(_json_fetch.time, "sleep", sleep)

    with pytest.raises(json.JSONDecodeError):
        module._fetch_json("https://example.test/api", {}, max_retries=3)

    assert len(calls) == 1
    sleep.assert_not_called()


@pytest.mark.parametrize("module", MODULES)
def test_unsafe_initial_url_is_rejected_before_open(
    module: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.undo()
    opener = mock.Mock()
    monkeypatch.setattr(_json_fetch, "open_validated_url", opener)

    with pytest.raises(UnsafeFetchURL):
        module._fetch_json("http://169.254.169.254/latest/meta-data", {})

    opener.assert_not_called()


@pytest.mark.parametrize("module", MODULES)
def test_redirect_target_is_revalidated_before_follow(
    module: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.undo()
    headers = Message()
    headers["Location"] = "http://169.254.169.254/latest/meta-data"
    redirect = HTTPError(
        "https://93.184.216.34/api",
        302,
        "redirect",
        headers,
        None,
    )
    opener = mock.MagicMock()
    opener.open.side_effect = redirect
    with mock.patch(
        "okg.substrate.library.sources._url_safety.urllib.request.build_opener",
        return_value=opener,
    ):
        with pytest.raises(UnsafeFetchURL):
            module._fetch_json("https://93.184.216.34/api", {})

    assert opener.open.call_count == 1


@pytest.mark.parametrize(
    ("module", "source_type", "payload"),
    [
        (openreview, openreview.OpenReviewSource, {"notes": [], "count": 0}),
        (
            inspire_hep,
            inspire_hep.InspireHepSource,
            {"hits": {"hits": [], "total": 0}},
        ),
    ],
)
def test_helper_and_adapter_backoff_defaults_are_preserved(
    module: Any,
    source_type: type,
    payload: dict[str, Any],
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    shared_calls: list[dict[str, Any]] = []

    def _shared(_endpoint: str, _query: dict[str, Any], **kwargs: Any):
        shared_calls.append(kwargs)
        return payload

    monkeypatch.setattr(module, "_shared_fetch_json", _shared)
    module._fetch_json("https://example.test/api", {})
    assert shared_calls[-1]["retry_backoff_seconds"] == 2.0

    adapter_calls: list[dict[str, Any]] = []

    def _entrypoint(_endpoint: str, _query: dict[str, Any], **kwargs: Any):
        adapter_calls.append(kwargs)
        return payload

    monkeypatch.setattr(module, "_fetch_json", _entrypoint)
    source_kwargs: dict[str, Any] = {
        "cache_root": tmp_path,
        "requests_per_second": None,
        "venues": [
            {
                "slug": "test",
                "name": "Test",
                "invitation": "Test/-/Submission",
                "queries": ["test"],
                "split_by_year": False,
            }
        ],
    }
    source = source_type(**source_kwargs)
    list(source.run("run").facts)

    assert adapter_calls[-1] == {
        "max_retries": 3,
        "retry_backoff_seconds": 10.0,
    }


def _pretend_okg_version(monkeypatch: pytest.MonkeyPatch, version: str) -> None:
    """Make the installed okg distribution report `version`."""
    real_version = importlib.metadata.version

    def _version(name: str) -> str:
        return version if name == "okg" else real_version(name)

    monkeypatch.setattr(importlib.metadata, "version", _version)


def test_user_agent_follows_the_installed_okg_version(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A version bump moves the User-Agent with no edit to the readers."""
    _pretend_okg_version(monkeypatch, "9.8.7")
    calls = _script_requests(monkeypatch, [b"{}"])

    assert _json_fetch.fetch_json("https://example.test/api", {}) == {}

    request, _timeout = calls[0]
    assert request.get_header("User-agent") == "okg/9.8.7"


def test_no_scholarly_reader_hard_codes_an_okg_user_agent() -> None:
    """Every scholarly reader builds its User-Agent from the package version."""
    import re
    from pathlib import Path

    package = Path(_json_fetch.__file__).parent
    pinned = re.compile(r"""["']okg/\d""")
    offenders = [
        f"{path.name}:{number}"
        for path in sorted(package.glob("*.py"))
        for number, line in enumerate(path.read_text().splitlines(), start=1)
        if pinned.search(line)
    ]
    assert offenders == []
